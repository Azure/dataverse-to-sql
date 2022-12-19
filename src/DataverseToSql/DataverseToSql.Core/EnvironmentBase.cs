// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Core;
using Azure.Storage.Blobs;
using DataverseToSql.Core.CdmModel;
using DataverseToSql.Core.Model;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using System.Collections.Concurrent;

namespace DataverseToSql.Core
{
    /// <summary>
    /// Provides:
    /// - configuration information
    /// - access to the CDM repository
    /// - access to the managed entities
    /// - access to the underlying database
    /// </summary>
    public abstract class EnvironmentBase
    {
        internal const string CONFIG_FILE = "DataverseToSql.json";
        internal const string CUSTOM_SQL_OBJECTS_FOLDER = "CustomSqlObjects";
        internal const string CUSTOM_DATATYPE_MAP = "CustomDatatypeMap.tsv";

        protected readonly ILogger log;
        internal readonly TokenCredential Credential;
        internal readonly EnvironmentConfiguration Config;

        internal readonly Database database;

        private IDictionary<string, CdmEntity>? _cdmEntityDict = null;
        private IDictionary<string, ManagedEntity>? _managedEntityDict = null;
        private IList<(string name, string script)>? _customScripts = null;
        private Dictionary<string, ManagedCustomScript>? _managedCustomScriptsDict = null;
        private Dictionary<string, HashSet<string>>? _optionSets;
        private Dictionary<string, Dictionary<string, string>>? _customDatatypeMap = null;

        public EnvironmentBase(
            ILogger log,
            TokenCredential credential,
            EnvironmentConfiguration config)
        {
            this.log = log;
            Credential = credential;
            Config = config;

            database = new Database(
                this.log,
                Config.Database,
                Credential);
        }

        // Return an interable over the blob names under Microsoft.Athena.TrickleFeedService
        // in the storage container containing Dataverse data.
        // Return all blobs whose name ends with "-model.json".
        // The *-model.json blobs contain the manifest of individual entities.
        internal async Task<IList<string>> GetCdmEntityModelBlobNamesAsync(CancellationToken cancellationToken)
        {
            List<string> result = new();

            var containerClient = new BlobContainerClient(
                Config.DataverseStorage.ContainerUri(),
                Credential);

            await foreach (var item in containerClient.GetBlobsByHierarchyAsync(
                delimiter: "/",
                prefix: "/Microsoft.Athena.TrickleFeedService/",
                cancellationToken: cancellationToken))
            {
                if (item.IsBlob && item.Blob.Name.EndsWith("-model.json"))
                {
                    result.Add(item.Blob.Name);
                }
            }

            return result;
        }

        // Dictionary of available CDM entities indexed by name (lowercase).
        internal async Task<IDictionary<string, CdmEntity>> GetCdmEntityDictAsync(CancellationToken cancellationToken)
            => (_cdmEntityDict ??= await InitCdmEntitiesDictAsync(cancellationToken));

        // Enumerable of available CDM entities.
        internal async Task<IEnumerable<CdmEntity>> GetCdmEntitiesAsync(CancellationToken cancellationToken)
            => (await GetCdmEntityDictAsync(cancellationToken)).Values;

        // Initialize the dictionary of CDM entities.
        // The dictionary contains only entities with
        // "Completed" InitialSyncState.
        internal async Task<IDictionary<string, CdmEntity>> InitCdmEntitiesDictAsync(CancellationToken cancellationToken)
        {
            log.LogInformation("Loading CDM entities.");

            var dict = new ConcurrentDictionary<string, CdmEntity>();

            // Iterate over the *-model.json blobs under Microsoft.Athena.TrickleFeedService
            // in the container with Dataverse data.

            if (!int.TryParse(Environment.GetEnvironmentVariable("ASYNC_PARALLELISM"), out int parallelism))
                parallelism = 16;

            var parallelOpts = new ParallelOptions()
            {
                MaxDegreeOfParallelism = parallelism,
                CancellationToken = cancellationToken
            };

            // force loading optionset data
            await GetOptionSetAsync(cancellationToken);

            await LoadCustomDatatypeMap(cancellationToken);

            await Parallel.ForEachAsync(await GetCdmEntityModelBlobNamesAsync(cancellationToken),
                parallelOpts,
                async (blobName, ct) =>
                {
                    var uriBuilder = new BlobUriBuilder(Config.DataverseStorage.ContainerUri())
                    {
                        BlobName = blobName
                    };

                    var blobUri = uriBuilder.ToUri();

                    var blobClient = new BlobClient(
                        blobUri,
                        Credential);

                    // Deserialize the manifest
                    try
                    {
                        var reader = new StreamReader(await blobClient.OpenReadAsync(default, ct));
                        var cdmManifest = new CdmManifest(JObject.Parse(await reader.ReadToEndAsync()));

                        foreach (var cdmEntity in cdmManifest.Entities)
                        {
                            // Add only entities whose InitialSyncState is "Completed"
                            if (cdmEntity.InitialSyncState == "Completed")
                            {
                                await OverrideDatatypes(cdmEntity, cancellationToken);

                                dict[cdmEntity.Name.ToLower()] = cdmEntity;
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        log.LogWarning(
                            "Failed to deserialized manifest {manifestUri} with error {message} " +
                            "The ingestion of the entity may be still in progress; " +
                            "the manifest may require up to one hour to be updated.",
                            blobUri,
                            ex.Message);
                    }
                });

            return dict;
        }

        private async Task LoadCustomDatatypeMap(CancellationToken cancellationToken)
        {
            var reader = await GetCustomDatatypeMapReader(cancellationToken);

            if (reader is null)
            {
                log.LogInformation("Custom data type map file is not present.");
                return;
            }

            try
            {
                _customDatatypeMap = new();

                string? line;
                int lineCount = 0;
                while ((line = await reader.ReadLineAsync()) != null)
                {
                    lineCount++;
                    var vals = line.Split('\t').Select(v => v.Trim().ToLower()).ToList();

                    if (vals.Count != 3)
                        throw new Exception($"Unexpected number of fields in the custom data type map file on line {lineCount}");

                    for (int i = 0; i < 3; i++)
                    {
                        if (vals[i] == "")
                            throw new Exception($"Empty field {i + 1} on line {lineCount} in the custom data type map file");
                    }

                    var table = vals[0];
                    var column = vals[1];
                    var datatype = vals[2];

                    try
                    {
                        SqlBuiltinDataTypeValidator.Validate(datatype);
                    }
                    catch (Exception ex)
                    {
                        throw new Exception(
                            $"Invalid data type on line {lineCount} in the custom data type map file: {ex.Message}",
                            ex);
                    }

                    if (!_customDatatypeMap.ContainsKey(table))
                        _customDatatypeMap[table] = new();

                    if (_customDatatypeMap[table].ContainsKey(column))
                        throw new Exception($"Duplicate column {column} in table {table} in the custom data type map file");

                    _customDatatypeMap[table][column] = datatype;
                }
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                reader.Close();
            }
        }

        internal abstract Task<TextReader?> GetCustomDatatypeMapReader(CancellationToken cancellationToken);

        // Override the datatype of an entity attributes if necessary
        // e.g. when wanting to use int instead of bigint for optionsets.
        private async Task OverrideDatatypes(CdmEntity cdmEntity, CancellationToken cancellationToken)
        {

            // if OptionSetInt32 is true, override the SQL data type
            // of int64 optionset fields to int, instead of bigint
            if (Config.SchemaHandling.OptionSetInt32 || _customDatatypeMap is not null)
            {
                var entityName = cdmEntity.Name.ToLower();
                var optionSets = await GetOptionSetAsync(cancellationToken);

                foreach (var attribute in cdmEntity.Attributes)
                {
                    var attributeName = attribute.Name.ToLower();

                    // override data type if OptionSetInt32 option is set
                    if (Config.SchemaHandling.OptionSetInt32
                        && (attributeName == "statecode"
                        || attributeName == "statuscode"
                        || (optionSets.ContainsKey(entityName) && optionSets[entityName].Contains(attributeName)))
                        && attribute.DataType.ToLower() == "int64")
                    {
                        attribute.CustomSqlDatatype = "int";
                    }

                    // override data type if there is a match in the custom data type map
                    if (_customDatatypeMap is not null
                        && _customDatatypeMap.ContainsKey(entityName)
                        && _customDatatypeMap[entityName].ContainsKey(attributeName))
                    {
                        attribute.CustomSqlDatatype = _customDatatypeMap[entityName][attributeName];
                    }
                }
            }
        }

        private async Task<Dictionary<string, HashSet<string>>> GetOptionSetAsync(CancellationToken cancellationToken)
        {
            if (_optionSets is null)
            {
                _optionSets = new();

                var uriBuilder = new BlobUriBuilder(Config.DataverseStorage.ContainerUri())
                {
                    BlobName = "OptionsetMetadata/GlobalOptionsetMetadata.csv"
                };

                var blobUri = uriBuilder.ToUri();

                var blobClient = new BlobClient(
                    blobUri,
                    Credential);

                using var globalOptionSetMetadatareader = new StreamReader(await blobClient.OpenReadAsync(default, cancellationToken));
                while (true)
                {
                    var line = await globalOptionSetMetadatareader.ReadLineAsync();
                    if (line is null) break;

                    var globalOptionSetFields = line.Split(',');
                    var table = globalOptionSetFields[6].ToLower();
                    var field = globalOptionSetFields[0].ToLower();

                    if (!_optionSets.ContainsKey(table))
                    {
                        _optionSets[table] = new();
                    }

                    _optionSets[table].Add(field);
                }

                uriBuilder = new BlobUriBuilder(Config.DataverseStorage.ContainerUri())
                {
                    BlobName = "OptionsetMetadata/OptionsetMetadata.csv"
                };

                blobUri = uriBuilder.ToUri();

                blobClient = new BlobClient(
                    blobUri,
                    Credential);

                using var optionSetMetadatareader = new StreamReader(await blobClient.OpenReadAsync(default, cancellationToken));
                while (true)
                {
                    var line = await optionSetMetadatareader.ReadLineAsync();
                    if (line is null) break;

                    var globalOptionSetFields = line.Split(',');
                    var table = globalOptionSetFields[0].ToLower();
                    var field = globalOptionSetFields[1].ToLower();

                    if (!_optionSets.ContainsKey(table))
                    {
                        _optionSets[table] = new();
                    }

                    _optionSets[table].Add(field);
                }
            }

            return _optionSets;
        }

        // Dictionary of managed entities indexed by name (lowercase).
        internal async Task<IDictionary<string, ManagedEntity>> GetManagedEntityDictAsync(CancellationToken cancellationToken)
        {
            return _managedEntityDict ??=
                (await database.GetManagedEntitiesAsync(cancellationToken))
                .ToDictionary(e => e.Name.ToLower(), e => e);
        }

        // Enumerable of managed entities.
        internal async Task<ICollection<ManagedEntity>> GetManagedEntitiesAsync(CancellationToken cancellationToken)
            => (await GetManagedEntityDictAsync(cancellationToken)).Values;

        // Try to retrieve a CDM entity based on its name
        internal async Task<(bool found, CdmEntity? cdmEntity)> TryGetCdmEntityAsync(string name, CancellationToken cancellationToken)
        {
            if ((await GetCdmEntityDictAsync(cancellationToken)).TryGetValue(name.ToLower(), out var cdmEntity))
            {
                return (true, cdmEntity);
            }
            return (false, null);
        }

        // Try to retrieve a CDM entity based on the name of a given managed entity
        internal async Task<(bool found, CdmEntity? cdmEntity)> TryGetCdmEntityAsync(ManagedEntity managedEntity, CancellationToken cancellationToken)
            => await TryGetCdmEntityAsync(managedEntity.Name, cancellationToken);

        // Determines if the given entity (by name) is managed or not.
        internal async Task<bool> IsManagedEntityAsync(string name, CancellationToken cancellationToken)
            => (await GetManagedEntityDictAsync(cancellationToken)).ContainsKey(name.ToLower());

        // Determines if the given entity (based on the name of a given CDM entity) is managed or not.
        internal async Task<bool> IsManagedEntityAsync(CdmEntity cdmEntity, CancellationToken cancellationToken)
            => await IsManagedEntityAsync(cdmEntity.Name, cancellationToken);

        // Return all custom scripts contained in the environment.
        internal async Task<IList<(string name, string script)>> GetCustomScriptsAsync(CancellationToken cancellationToken)
        {
            return _customScripts ??= (await InitCustomScriptsAsync(cancellationToken));
        }

        // Initialize list of custom scripts from storage.
        internal abstract Task<IList<(string name, string script)>> InitCustomScriptsAsync(CancellationToken cancellationToken);

        // Return a managed custom script by name. Return null if not found.
        internal async Task<ManagedCustomScript?> GetManagedCustomScript(string name, CancellationToken cancellationToken)
        {
            (await GetManagedCustomScriptDict(cancellationToken)).TryGetValue(name.ToLower(), out ManagedCustomScript? result);
            return result;
        }

        // Return the dictionary of managed custom script, indexed by name.
        internal async Task<IDictionary<string, ManagedCustomScript>> GetManagedCustomScriptDict(CancellationToken cancellationToken)
        {
            if (_managedCustomScriptsDict is null)
            {
                var managedCustomScripts = await database.GetManagedCustomScriptsAsync(cancellationToken);
                _managedCustomScriptsDict = managedCustomScripts.ToDictionary(k => k.Name.ToLower(), v => v);
            }

            return _managedCustomScriptsDict;
        }
    }
}
