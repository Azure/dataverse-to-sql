using Azure.Core;
using DataverseToSql.Core.Auth;
using DataverseToSql.Core.Model;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.SqlServer.Dac;
using Microsoft.SqlServer.Dac.Model;
using System.Data;

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
                    _model.AddObjects(SqlObjects.DataverseToSql_BlobsToIngest_Get_Proc);
                    _model.AddObjects(SqlObjects.DataverseToSql_BlobsToIngest_Complete_Proc);

                    _model.AddObjects(SqlObjects.DataverseToSql_FullLoad_Complete_Proc);

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
                    DropObjectsNotInSource = false
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
            cmd.Parameters.Add(new() { ParameterName = "@State" });
            cmd.Parameters.Add(new() { ParameterName = "@SchemaHash" });

            cmd.Parameters["@EntityName"].Value = managedEntity.Name;
            cmd.Parameters["@State"].Value = managedEntity.State;
            cmd.Parameters["@SchemaHash"].Value = managedEntity.SchemaHash;

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

        internal async Task InsertAsync(BlobToIngest blob, CancellationToken cancellationToken)
        {
            var conn = await GetSqlConnectionAsync(cancellationToken);
            var cmd = conn.CreateCommand();
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandText = "[DataverseToSql].[BlobsToIngest_Insert]";
            cmd.Parameters.Add(new() { ParameterName = "@EntityName" });
            cmd.Parameters.Add(new() { ParameterName = "@BlobName" });
            cmd.Parameters.Add(new() { ParameterName = "@TargetSchema" });
            cmd.Parameters.Add(new() { ParameterName = "@TargetTable" });
            cmd.Parameters.Add(new() { ParameterName = "@ServerlessQuery" });
            cmd.Parameters.Add(new() { ParameterName = "@LoadType" });

            cmd.Parameters["@EntityName"].Value = blob.Entity.Name;
            cmd.Parameters["@BlobName"].Value = blob.Name;
            cmd.Parameters["@TargetSchema"].Value = blob.TargetSchema;
            cmd.Parameters["@TargetTable"].Value = blob.TargetTable;
            cmd.Parameters["@ServerlessQuery"].Value = blob.ServerlessQuery;
            cmd.Parameters["@LoadType"].Value = (int)blob.LoadType;

            await cmd.ExecuteNonQueryAsync(cancellationToken);
            conn.Close();
        }
    }
}
