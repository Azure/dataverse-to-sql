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
    public class EnvironmentBase
    {
        internal const string CONFIG_FILE = "DataverseToSql.json";

        protected readonly ILogger log;
        internal readonly TokenCredential Credential;
        internal readonly EnvironmentConfiguration Config;

        internal readonly Database database;

        private IDictionary<string, CdmEntity>? _cdmEntityDict = null;
        private IDictionary<string, ManagedEntity>? _managedEntityDict = null;

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
        private async Task<IDictionary<string, CdmEntity>> InitCdmEntitiesDictAsync(CancellationToken cancellationToken)
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
                    var reader = new StreamReader(await blobClient.OpenReadAsync(default, ct));
                    var cdmManifest = new CdmManifest(JObject.Parse(await reader.ReadToEndAsync()));

                    foreach (var cdmEntity in cdmManifest.Entities)
                    {
                        // Add only entities whose InitialSyncState is "Completed"
                        if (cdmEntity.InitialSyncState == "Completed")
                        {
                            dict[cdmEntity.Name.ToLower()] = cdmEntity;
                        }
                    }
                });

            return dict;
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
    }
}
