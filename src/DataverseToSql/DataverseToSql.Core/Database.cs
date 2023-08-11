// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Core;
using DataverseToSql.Core.Auth;
using DataverseToSql.Core.Model;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.SqlServer.Dac;
using Microsoft.SqlServer.Dac.Model;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;

namespace DataverseToSql.Core
{
    /// <summary>
    /// Provides these funcionalities:
    /// - Deploying a schema to the database using DacFx
    /// - Interacting with some metadata tables
    /// </summary>
    internal class Database
    {
        private readonly ILogger log;
        private readonly DatabaseConfiguration Config;
        private readonly TokenCredential Credential;

        private TSqlModel? _model;

        internal Database(
            ILogger log,
            DatabaseConfiguration config,
            TokenCredential credential)
        {
            this.log = log;
            Config = config;
            Credential = credential;
        }

        internal TSqlModel Model
        {
            get
            {
                if (_model is null)
                {
                    _model = new TSqlModel(
                        SqlServerVersion.SqlAzure,
                        null
                        );

                    // Add DataverseToSql metadata objects
                    _model.AddObjects(SqlObjects.DataverseToSql_Schema);
                    _model.AddObjects(SqlObjects.DataverseToSql_Types);

                    _model.AddObjects(SqlObjects.DataverseToSql_ManagedEntities_Table);
                    _model.AddObjects(SqlObjects.DataverseToSql_ManagedEntities_Upsert_Proc);

                    _model.AddObjects(SqlObjects.DataverseToSql_ManagedBlobs_Table);
                    _model.AddObjects(SqlObjects.DataverseToSql_ManagedBlobs_Upsert_Proc);

                    _model.AddObjects(SqlObjects.DataverseToSql_BlobsToIngest_Table);
                    _model.AddObjects(SqlObjects.DataverseToSql_BlobsToIngest_Insert_Proc);

                    _model.AddObjects(SqlObjects.DataverseToSql_IngestionJobs_Get_Proc);
                    _model.AddObjects(SqlObjects.DataverseToSql_IngestionJobs_Complete_Proc);

                    _model.AddObjects(SqlObjects.DataverseToSql_ManagedCustomScripts_Table);
                    _model.AddObjects(SqlObjects.DataverseToSql_ManagedCustomScripts_Upsert_Proc);

                    _model.AddObjects(SqlObjects.Optionsets_AttributeMetadata.Replace(
                        "$$SCHEMA$$", Config.Schema));

                    _model.AddObjects(SqlObjects.Optionsets_OptionsetMetadata.Replace(
                        "$$SCHEMA$$", Config.Schema));

                    _model.AddObjects(SqlObjects.Optionsets_GlobalOptionsetMetadata.Replace(
                        "$$SCHEMA$$", Config.Schema));

                    _model.AddObjects(SqlObjects.Optionsets_StateMetadata.Replace(
                        "$$SCHEMA$$", Config.Schema));

                    _model.AddObjects(SqlObjects.Optionsets_StatusMetadata.Replace(
                        "$$SCHEMA$$", Config.Schema));

                    _model.AddObjects(SqlObjects.Optionsets_TargetMetadata.Replace(
                        "$$SCHEMA$$", Config.Schema));

                    // Add default schema, unless dbo
                    if (!Config.Schema.Equals("dbo", StringComparison.OrdinalIgnoreCase))
                    {
                        _model.AddObjects($"CREATE SCHEMA [{Config.Schema}];");
                    }
                }
                return _model;
            }
        }

        internal void AddObjects(string script)
        {
            Model.AddObjects(script);
        }

        /// <summary>
        /// Tries to add the specified scripts to the DAC model.
        /// Scripts that fail (e.g. due to syntax errors or missing dependencies) are skipped
        /// and a warning message is produced.
        /// </summary>
        /// <param name="scripts">List of tuples with name and content of the scripts to add.
        /// The name should correspond to the name of the script at the source (e.g. the original 
        /// file name).</param>
        internal void TryAddObjects(IEnumerable<(string name, string script)> scripts)
        {
            List<(string name, TSqlScript script)> InputScripts = new();
            List<(string name, TSqlScript script)> FailedScripts = new();
            Dictionary<string, IEnumerable<DacModelError>> SourceErrors = new();

            // Parse all the scripts and skip those who generate a syntax error.
            var parser = new TSql160Parser(true, SqlEngineType.SqlAzure);

            foreach (var (name, script) in scripts)
            {
                var stringReader = new StringReader(script);
                var tree = parser.Parse(stringReader, out var parseErrors);

                // Check if there is any syntax error
                if (parseErrors.Count > 0)
                {
                    foreach (var error in parseErrors)
                    {
                        log.LogWarning(
                            "Error in source file {source}: Code {errorcode}, Line {line}, Column {column}, {message}",
                            name,
                            error.Number,
                            error.Line,
                            error.Column,
                            error.Message);
                    }
                }
                // If there is no error and the parser produced a valid TSqlScript object
                // store the script in CurrentScripts for further processing
                else if (tree is TSqlScript tsqlScript)
                {
                    InputScripts.Add((name, tsqlScript));
                }
                // If the parser did not produce a TSqlScript, skip the script
                else
                {
                    log.LogWarning(
                        "Error in source file {source}: the file is not a valid SQL script",
                        name);
                }
            }

            // Iterate over the input scripts and add them to the model.
            // Validate the model after adding each script.
            // If a script fails, remove it from the model.
            // The order of iteration does not take into account dependencies between
            // scripts, so scripts executed out of order may fail and be
            // unnecessarily removed from the model.
            // This is dealt with by trying the failed scripts again until all
            // dependencies are resolved and FailedScripts list is left only with
            // scripts that fail due to other reasons.
            var counter = 0;
            while (true)
            {
                foreach (var source in InputScripts)
                {
                    // counter is used as a label to add/remove scripts to/from the model
                    counter++;
                    Model.AddOrUpdateObjects(source.script, counter.ToString(), default);

                    // Check if the model contains any error.
                    // The check is performed against Azure SQL Databse version both to validate
                    // the code is compatible with it and to get reacher error details.
                    var result = Model.CheckVersionCompatibility(SqlServerVersion.SqlAzure, default);
                    if (result.Any())
                    {
                        FailedScripts.Add(source);                  // Track the failed script
                        Model.DeleteObjects(counter.ToString());    // Remove the script from the model
                        SourceErrors[source.name] = result;         // Track the errors
                    }
                }

                // If the number of failed scripts is equal to the number of input scripts
                // it means all dependencies were resolved and we are left with scripts
                // that fail for other reasons, or with no failing script at all.
                if (FailedScripts.Count == InputScripts.Count)
                {
                    break;
                }

                // Repeat the loop over any remaining failed script
                InputScripts = FailedScripts;
                FailedScripts = new();
            }

            // Produce a warning for each error of the failing scripts
            foreach (var (name, _) in FailedScripts)
            {
                if (SourceErrors.ContainsKey(name))
                {
                    foreach (var error in SourceErrors[name])
                    {
                        log.LogWarning(
                            "Error in source file {source}: Code {errorcode}, Line {line}, Column {column}, {message}",
                            name,
                            error.ErrorCode,
                            error.Line,
                            error.Column,
                            error.Message);
                    }
                }
                else
                {
                    log.LogWarning("Error in source file {source}.", name);
                }
            }
        }

        internal void DeployModel()
        {
            // Generate in-memory package and deploy it
            log.LogInformation("Generating package.");

            MemoryStream packageStream = new();

            DacPackageExtensions.BuildPackage(
                packageStream,
                Model,
                new() { Name = "DataverseToSql" }
                );

            var package = DacPackage.Load(packageStream);

            log.LogInformation("Instantiating DAC services.");

            DacServices dacServices = new(
                ConnectionString,
                new DacAuthProvider(Credential)
                );

            log.LogInformation("Deploying package.");

            dacServices.Deploy(
                package,
                targetDatabaseName: Config.Database,
                upgradeExisting: true,
                options: new()
                {
                    DropObjectsNotInSource = false,
                    BlockOnPossibleDataLoss = false
                });
        }

        internal string ConnectionString
        {
            get
            {
                var scsb = new SqlConnectionStringBuilder
                {
                    DataSource = Config.Server,
                    InitialCatalog = Config.Database,
                    Pooling = true
                };
                return scsb.ToString();
            }
        }

        internal async Task<SqlConnection> GetSqlConnectionAsync(CancellationToken cancellationToken)
        {
            //var token = (await Credential.GetTokenAsync(
            //    new TokenRequestContext(new[] { "https://database.windows.net/.default" }),
            //    default //cancellationToken
            //    )).Token;

            var token = (Credential.GetToken(
                new[] { "https://database.windows.net/.default" }
                ));

            var conn = new SqlConnection(ConnectionString)
            {
                AccessToken = token
            };
            await conn.OpenAsync(cancellationToken);
            return conn;
        }

        internal async Task UpsertAsync(
            ManagedEntity managedEntity,
            CancellationToken cancellationToken)
        {
            var conn = await GetSqlConnectionAsync(cancellationToken);
            var cmd = conn.CreateCommand();
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandText = "[DataverseToSql].[ManagedEntities_Upsert]";
            cmd.Parameters.Add(new() { ParameterName = "@EntityName" });
            cmd.Parameters.Add(new() { ParameterName = "@TargetSchema" });
            cmd.Parameters.Add(new() { ParameterName = "@TargetTable" });
            cmd.Parameters.Add(new() { ParameterName = "@State" });
            cmd.Parameters.Add(new() { ParameterName = "@SchemaHash" });
            cmd.Parameters.Add(new() { ParameterName = "@FullLoadInnerQuery" });
            cmd.Parameters.Add(new() { ParameterName = "@IncrementalInnerQuery" });

            cmd.Parameters["@EntityName"].Value = managedEntity.Name;
            cmd.Parameters["@TargetSchema"].Value = managedEntity.TargetSchema;
            cmd.Parameters["@TargetTable"].Value = managedEntity.TargetTable;
            cmd.Parameters["@State"].Value = managedEntity.State;
            cmd.Parameters["@SchemaHash"].Value = managedEntity.SchemaHash;
            cmd.Parameters["@FullLoadInnerQuery"].Value = managedEntity.FullLoadInnerQuery;
            cmd.Parameters["@IncrementalInnerQuery"].Value = managedEntity.IncrementalInnerQuery;

            await cmd.ExecuteNonQueryAsync(cancellationToken);
            conn.Close();
        }

        internal async Task UpsertAsync(
            ManagedBlob managedBlob,
            CancellationToken cancellationToken)
        {
            var conn = await GetSqlConnectionAsync(cancellationToken);
            var cmd = conn.CreateCommand();
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandText = "[DataverseToSql].[ManagedBlobs_Upsert]";
            cmd.Parameters.Add(new() { ParameterName = "@EntityName" });
            cmd.Parameters.Add(new() { ParameterName = "@BlobName" });
            cmd.Parameters.Add(new() { ParameterName = "@FileOffset" });

            cmd.Parameters["@EntityName"].Value = managedBlob.Entity.Name;
            cmd.Parameters["@BlobName"].Value = managedBlob.Name;
            cmd.Parameters["@FileOffset"].Value = managedBlob.Offset;

            await cmd.ExecuteNonQueryAsync(cancellationToken);
            conn.Close();
        }

        internal async Task UpsertAsync(
            ManagedCustomScript managedCustomScript,
            CancellationToken cancellationToken)
        {
            var conn = await GetSqlConnectionAsync(cancellationToken);
            var cmd = conn.CreateCommand();
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandText = "[DataverseToSql].[ManagedCustomScripts_Upsert]";
            cmd.Parameters.Add(new() { ParameterName = "@ScriptName" });
            cmd.Parameters.Add(new() { ParameterName = "@Hash" });

            cmd.Parameters["@ScriptName"].Value = managedCustomScript.Name;
            cmd.Parameters["@Hash"].Value = managedCustomScript.Hash;

            await cmd.ExecuteNonQueryAsync(cancellationToken);
            conn.Close();
        }

        internal async Task<bool> IsTableEmptyAsync(
            string name,
            string? schema = null,
            CancellationToken cancellationToken = default)
        {
            schema ??= Config.Schema;

            var conn = await GetSqlConnectionAsync(cancellationToken);
            var cmd = conn.CreateCommand();

            cmd.CommandText = string.Format(
                "SELECT TOP 1 1 FROM [{0}].[{1}];",
                schema.Replace("]", "]]"),
                name.Replace("]", "]]")
                );

            var result = await cmd.ExecuteScalarAsync(cancellationToken);
            conn.Close();

            return !(result is int value && value == 1);
        }

        internal async Task<IList<ManagedEntity>> GetManagedEntitiesAsync(
            CancellationToken cancellationToken)
        {
            var conn = await GetSqlConnectionAsync(cancellationToken);
            var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT [EntityName], [State], [SchemaHash] FROM [DataverseToSql].[ManagedEntities];";
            var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            List<ManagedEntity> result = new();

            while (await reader.ReadAsync(cancellationToken))
            {
                var managedEntity = new ManagedEntity()
                {
                    Name = reader.GetString(0),
                    State = (ManagedEntityState)reader.GetInt32(1),
                    SchemaHash = reader.IsDBNull(2) ? null : reader.GetString(2)
                };

                result.Add(managedEntity);
            }
            reader.Close();
            conn.Close();

            return result;
        }

        internal async Task<IList<ManagedBlob>> GetManagedBlobsAsync(ManagedEntity managedEntity,
            CancellationToken cancellationToken)
        {
            var conn = await GetSqlConnectionAsync(cancellationToken);
            var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT [BlobName],[FileOffset] FROM [DataverseToSql].[ManagedBlobs] WHERE [EntityName] = @EntityName;";
            cmd.Parameters.Add(new() { ParameterName = "@EntityName" });
            cmd.Parameters["@EntityName"].Value = managedEntity.Name;

            List<ManagedBlob> result = new();

            var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                result.Add(new ManagedBlob(
                    managedEntity,
                    reader.GetString(0),
                    reader.GetInt64(1)
                    ));
            }
            reader.Close();
            conn.Close();

            return result;
        }

        internal async Task<IList<(long Id, string BlobName)>> GetCompletedBlobsToIngest(CancellationToken cancellationToken)
        {
            var conn = await GetSqlConnectionAsync(cancellationToken);
            var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT [Id], [BlobName] FROM [DataverseToSql].[BlobsToIngest] WHERE [Complete] = 1;";

            List<(long Id, string BlobName)> completedBlobs = new();

            var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                completedBlobs.Add(
                    new(reader.GetInt64(0), reader.GetString(1)));
            }
            reader.Close();
            conn.Close();

            return completedBlobs;
        }

        internal async Task<bool> AreBlobsPendingIngestion(CancellationToken cancellationToken)
        {
            var conn = await GetSqlConnectionAsync(cancellationToken);
            var cmd = conn.CreateCommand();

            cmd.CommandText = string.Format(
                "SELECT TOP 1 1 FROM [DataverseToSql].[BlobsToIngest] WHERE [Complete] = 0;"
                );

            var result = await cmd.ExecuteScalarAsync(cancellationToken);
            conn.Close();

            return result is int value && value == 1;
        }

        internal async Task DeleteBlobToIngest(long id, CancellationToken cancellationToken)
        {
            var conn = await GetSqlConnectionAsync(cancellationToken);
            var cmd = conn.CreateCommand();
            cmd.CommandText = "DELETE [DataverseToSql].[BlobsToIngest] WHERE [Id] = @Id;";
            cmd.Parameters.Add(new() { ParameterName = "@Id" });
            cmd.Parameters["@Id"].Value = id;

            await cmd.ExecuteNonQueryAsync(cancellationToken);

            conn.Close();
        }

        internal async Task InsertAsync(IEnumerable<BlobToIngest> blobs, CancellationToken cancellationToken)
        {
            var conn = await GetSqlConnectionAsync(cancellationToken);

            var cmd = conn.CreateCommand();
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandText = "[DataverseToSql].[BlobsToIngest_Insert]";
            cmd.Parameters.Add(new() { ParameterName = "@EntityName" });
            cmd.Parameters.Add(new() { ParameterName = "@BlobName" });
            cmd.Parameters.Add(new() { ParameterName = "@BasePath" });
            cmd.Parameters.Add(new() { ParameterName = "@Timestamp" });
            cmd.Parameters.Add(new() { ParameterName = "@Partition" });
            cmd.Parameters.Add(new() { ParameterName = "@LoadType" });

            SqlTransaction? transaction = null;

            try
            {
                transaction = await conn.BeginTransactionAsync(cancellationToken) as SqlTransaction;

                cmd.Transaction = transaction;

                foreach (var blob in blobs)
                {
                    cmd.Parameters["@EntityName"].Value = blob.Entity.Name;
                    cmd.Parameters["@BlobName"].Value = blob.Name;
                    cmd.Parameters["@BasePath"].Value = blob.BasePath;
                    cmd.Parameters["@Timestamp"].Value = blob.Timestamp;
                    cmd.Parameters["@Partition"].Value = blob.Partition;
                    cmd.Parameters["@LoadType"].Value = (int)blob.LoadType;
                    await cmd.ExecuteNonQueryAsync(cancellationToken);
                }

                if (transaction is not null)
                    await transaction.CommitAsync(cancellationToken);
            }
            catch (Exception)
            {
                try
                {
                    if (transaction is not null)
                        await transaction.RollbackAsync(cancellationToken);
                }
                catch (Exception)
                { }

                throw;
            }

            conn.Close();
        }

        internal async Task<List<ManagedCustomScript>> GetManagedCustomScriptsAsync(CancellationToken cancellationToken)
        {
            var conn = await GetSqlConnectionAsync(cancellationToken);
            var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT [ScriptName],[Hash] FROM [DataverseToSql].[ManagedCustomScripts];";

            List<ManagedCustomScript> result = new();

            var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                result.Add(new ManagedCustomScript(
                    reader.GetString(0),
                    reader.GetString(1)));
            }
            reader.Close();
            conn.Close();

            return result;
        }

        internal async Task<List<string>> GetTableTypeColumnsAsync(
            string tableName,
            CancellationToken cancellationToken)
        {
            var conn = await GetSqlConnectionAsync(cancellationToken);

            var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT c.[name] FROM sys.columns c " +
                "INNER JOIN sys.table_types tt ON c.object_id = tt.type_table_object_id " +
                "WHERE tt.name = @TableTypeName AND SCHEMA_NAME(tt.[schema_id]) = N'DataverseToSql' " +
                "ORDER BY column_id";

            cmd.Parameters.Add("@TableTypeName", SqlDbType.NVarChar).Value = $"{tableName}_TableType";

            var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var result = new List<string>();

            while (await reader.ReadAsync(cancellationToken))
            {
                result.Add(reader.GetString(0));
            }

            reader.Close();
            conn.Close();

            return result;
        }
    }
}
