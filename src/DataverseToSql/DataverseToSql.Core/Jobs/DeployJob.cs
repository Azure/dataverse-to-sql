// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Analytics.Synapse.Artifacts;
using Azure.Analytics.Synapse.Artifacts.Models;
using Azure.Storage.Blobs;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using System.Data;
using Expression = Azure.Analytics.Synapse.Artifacts.Models.Expression;
using ExpressionType = Azure.Analytics.Synapse.Artifacts.Models.ExpressionType;
using System.Text.Json;
using System.Text;

namespace DataverseToSql.Core.Jobs
{
    /// <summary>
    /// Deploys the following components:
    /// - The core metadata schema to the database
    /// - The Synapse linked services
    /// - The Synapse datasets
    /// - The Synapse Ingestion pipeline
    /// Additionally, it uploads configuration data to the configuration storage container
    /// </summary>
    public class DeployJob
    {
        private readonly ILogger log;
        private readonly LocalEnvironment environment;

        public DeployJob(ILogger log, LocalEnvironment environment)
        {
            this.log = log;
            this.environment = environment;
        }

        public void Run()
        {
            ValidateConfiguration();

            log.LogInformation("Starting deployment.");

            // Deploy the core metadata schema to the database
            log.LogInformation("Generating database model.");
            environment.database.DeployModel();

            // Create the Synapse linked services
            CreateLinkedServices();
            // Create the Synapse datasets
            CreateDatasets();
            // Create the Spark notebooks
            CreateRootNotebook();
            CreateProcessEntityNotebook();
            // Create the Synapse ingestion pipelines
            CreateIncrementalLoadPipeline();
            CreateFullLoadPipeline();

            // Upload the configuration files to the configuration storage container
            UploadConfiguration();

            // Upload the custom data type map file
            UploadCustomDatatypeMap();

            // Upload custom SQL objects to the configuration storage container
            UploadCustomSqlObjects();

            log.LogInformation("Successfully completed deployment.");
        }

        private void ValidateConfiguration()
        {
            if (environment.Config.DataverseStorage.StorageAccount != environment.Config.IncrementalStorage.StorageAccount)
            {
                var ex = new Exception("The storage account of the DataverseStorage and IncrementalStorage must be the same.");
                log.LogError(ex, ex.Message);
                throw ex;
            }
        }

        // Create the Synapse linked services required by the ingeston pipeline.
        // Only create linked services that do not already exist.
        private void CreateLinkedServices()
        {
            log.LogInformation("Creating linked services.");

            var linkedServiceClient = new LinkedServiceClient(
                new Uri(environment.Config.SynapseWorkspace.DevEndpoint()),
                environment.Credential);

            // Retrieve the list of current linked services.
            HashSet<string> existingLinkedServices = new(
                linkedServiceClient
                    .GetLinkedServicesByWorkspace()
                    .Select(l => l.Name.ToLower()));

            // Create the linked service to Azure SQL Database if it does not exist
            var linkedServiceName = Naming.AzureSqlLinkedServiceName();
            if (!existingLinkedServices.Contains(linkedServiceName.ToLower()))
            {
                var scsb = new SqlConnectionStringBuilder
                {
                    IntegratedSecurity = false,
                    Encrypt = true,
                    ConnectTimeout = 30,
                    DataSource = environment.Config.Database.Server,
                    InitialCatalog = environment.Config.Database.Database
                };

                var linkedService = new AzureSqlDatabaseLinkedService(scsb.ToString())
                {
                    ConnectVia = new(
                        IntegrationRuntimeReferenceType.IntegrationRuntimeReference,
                        "AutoResolveIntegrationRuntime")
                };

                CreateLinkedService(linkedServiceClient, linkedServiceName, linkedService);
            }

            // Create the linked service to the Serverless SQL Pool if it does not exist
            linkedServiceName = Naming.ServerlessPoolLinkedServiceName();
            if (!existingLinkedServices.Contains(linkedServiceName.ToLower()))
            {
                var scsb = new SqlConnectionStringBuilder
                {
                    IntegratedSecurity = false,
                    Encrypt = true,
                    ConnectTimeout = 30,
                    DataSource = environment.Config.SynapseWorkspace.ServerlessEndpoint(),
                    InitialCatalog = "master"
                };

                var linkedService = new AzureSqlDatabaseLinkedService(scsb.ToString())
                {
                    ConnectVia = new(
                        IntegrationRuntimeReferenceType.IntegrationRuntimeReference,
                        "AutoResolveIntegrationRuntime")
                };

                CreateLinkedService(linkedServiceClient, linkedServiceName, linkedService);
            }
        }

        // Helper method to create a linked service and check the operation result
        private void CreateLinkedService(
            LinkedServiceClient linkedServiceClient,
            string name,
            AzureSqlDatabaseLinkedService linkedService)
        {
            log.LogInformation("Creating linked service {linkedService}",
                name);

            var linkedServiceResource = new LinkedServiceResource(linkedService);

            var createOp = linkedServiceClient
                .StartCreateOrUpdateLinkedService(name,
                    linkedServiceResource);

            createOp.WaitForCompletion();

            if (!createOp.HasValue)
            {
                var response = createOp.GetRawResponse();

                throw new Exception(
                    $"Creation of linked service {name} failed: {response.Status} {response.ReasonPhrase}");
            }

            var resp = Newtonsoft.Json.JsonConvert.DeserializeObject<dynamic>(
                createOp.GetRawResponse().Content.ToString()
                );

            if (resp?.status?.Value?.ToLower() == "failed" ?? false)
            {
                string message = resp?.error.message.Value.ToString() ?? "unknown";

                throw new Exception(
                    $"Creation of linked service {name} failed: {message}");
            }
        }

        // Create the Synapse datasets to read/write Azure SQL Database and 
        // Serverless SQL Pool
        private void CreateDatasets()
        {
            log.LogInformation("Creating datasets.");

            // Dataset to read metadata from Azure SQL Database

            var dataset = new AzureSqlTableDataset(
                linkedServiceName: new(
                    LinkedServiceReferenceType.LinkedServiceReference,
                    Naming.AzureSqlLinkedServiceName()))
            {
                Folder = new DatasetFolder() { Name = Naming.DatasetFolder() }
            };

            CreateDataset(
                dataset,
                name: Naming.MetadataDatasetName());

            // Dataset to write to Azure SQL Database

            dataset = new AzureSqlTableDataset(
                linkedServiceName: new(
                    LinkedServiceReferenceType.LinkedServiceReference,
                    Naming.AzureSqlLinkedServiceName()))
            {
                SchemaTypePropertiesSchema = "@dataset().Schema",
                Table = "@dataset().Table",

                Folder = new DatasetFolder() { Name = Naming.DatasetFolder() }
            };

            dataset.Parameters.Add("Schema", new(ParameterType.String));
            dataset.Parameters.Add("Table", new(ParameterType.String));

            CreateDataset(
                dataset,
                name: Naming.AzureSqlDatasetName());

            // Dataset to read from Serverless SQL Pool

            dataset = new AzureSqlTableDataset(
                linkedServiceName: new(
                    LinkedServiceReferenceType.LinkedServiceReference,
                    Naming.ServerlessPoolLinkedServiceName()))
            {
                Folder = new DatasetFolder() { Name = Naming.DatasetFolder() }
            };

            CreateDataset(
                dataset,
                name: Naming.ServerlessDatasetName());
        }

        // Helper method to create a dataset and check the operation result
        private void CreateDataset(
            Dataset dataset,
            string name)
        {
            log.LogInformation("Creating dataset {dataset}",
                name);

            var datasetResource = new DatasetResource(dataset);

            var datasetClient = new DatasetClient(
                new Uri(environment.Config.SynapseWorkspace.DevEndpoint()),
                environment.Credential);

            var createOp = datasetClient.StartCreateOrUpdateDataset(
                            name,
                            datasetResource);

            createOp.WaitForCompletion();

            if (!createOp.HasValue)
            {
                var response = createOp.GetRawResponse();

                throw new Exception(
                    $"Creation of dataset {name} failed: {response.Status} {response.ReasonPhrase}");
            }

            var resp = Newtonsoft.Json.JsonConvert.DeserializeObject<dynamic>(
                createOp.GetRawResponse().Content.ToString()
                );

            if (resp?.status?.Value?.ToLower() == "failed" ?? false)
            {
                string message = resp?.error.message.Value.ToString() ?? "unknown";

                throw new Exception($"Creation of dataset {name} failed: {message}");
            }
        }

        // Create the Synapse incremental load pipeline
        private void CreateIncrementalLoadPipeline()
        {
            log.LogInformation("Creating incremental load pipeline");

            var pipeline = new PipelineResource
            {
                Folder = new PipelineFolder()
                {
                    Name = Naming.PipelineFolderName()
                },
                Concurrency = 1 // Set pipeline concurrency to 1 to prevent concurrent ingestion processes
            };

            pipeline.Variables["OptionsetTables"] = new(VariableType.Array)
            {
                DefaultValue = GetOptionsetTablesDefinition()
            };

            // Activity: Load optionset table
            // Load an optionset table

            var loadOptionsetSource = new AzureSqlSource()
            {
                SqlReaderQuery = new Expression(
                    ExpressionType.Expression,
                    "@item().serverlessQuery")
            };

            var loadOptionsetSink = new AzureSqlSink
            {
                SqlWriterStoredProcedureName = new Expression(
                    ExpressionType.Expression,
                    "@concat('[DataverseToSql].[Merge_', item().optionsetTable, ']')"),
                SqlWriterTableType = new Expression(
                    ExpressionType.Expression,
                    "@concat('[DataverseToSql].', item().optionsetTable, '_TableType')"),
                StoredProcedureTableTypeParameterName = "source"
            };

            var loadOptionsetActivity = new CopyActivity(
                "Load optionset table",
                loadOptionsetSource,
                loadOptionsetSink)
            {
                EnableStaging = false
            };

            loadOptionsetActivity.Inputs.Add(new(DatasetReferenceType.DatasetReference,
                Naming.ServerlessDatasetName()));

            var loadOptionsetOutputReference = new DatasetReference(DatasetReferenceType.DatasetReference,
                Naming.AzureSqlDatasetName());

            loadOptionsetOutputReference.Parameters["Schema"] = environment.Config.Database.Schema;
            loadOptionsetOutputReference.Parameters["Table"] = new Expression(
                ExpressionType.Expression,
                "@item().optionsetTable");

            loadOptionsetActivity.Outputs.Add(loadOptionsetOutputReference);

            // Activity: For each optionset table
            // Iterate over all optionset tables from the pipeline's OptionsetTables variable
            // and load them

            var foreachOptionsetTableActivity = new ForEachActivity(
                "For each optionset table",
                new Expression(ExpressionType.Expression,
                    "@variables('OptionsetTables')"),
                new List<Activity>() { loadOptionsetActivity })
            {
                BatchCount = 6
            };

            pipeline.Activities.Add(foreachOptionsetTableActivity);

            // Activity: Lookup ingestion jobs
            // Lookup activity that retrieves the list of ingestion jobs to run
            var lookupIngestionJobsSource = new AzureSqlSource()
            {
                SqlReaderStoredProcedureName = "[DataverseToSql].[IngestionJobs_Get]"
            };

            var lookupIngestionJobsDataset = new DatasetReference(
                DatasetReferenceType.DatasetReference,
                Naming.MetadataDatasetName());

            var lookupIngestionJobsActivity = new LookupActivity(
                "Lookup ingestion jobs",
                lookupIngestionJobsSource,
                lookupIngestionJobsDataset)
            {
                FirstRowOnly = false
            };

            lookupIngestionJobsActivity.DependsOn.Add(
                new ActivityDependency(
                    foreachOptionsetTableActivity.Name,
                    new List<DependencyCondition>() { new("Succeeded") }));

            pipeline.Activities.Add(lookupIngestionJobsActivity);

            // Activity: Incremental load
            // Copy activity that performs the incremental load of a blob to Azure SQL Database.

            // The source is the query for Serverless SQL Pool
            // retrieved by the lookup action
            var incrementalLoadSource = new AzureSqlSource()
            {
                SqlReaderQuery = new Expression(
                    ExpressionType.Expression,
                    "@item().ServerlessQuery")
            };

            // The sink is configured for upsert using a stored procedure.
            // The stored procedure and the corresponsing table type are
            // based on the entity name. Each entity has its on procedure and
            // table type
            var incrementalLoadSink = new AzureSqlSink
            {
                SqlWriterStoredProcedureName = new Expression(
                    ExpressionType.Expression,
                    "@concat('[DataverseToSql].[Merge_', item().EntityName, ']')"),
                SqlWriterTableType = new Expression(
                    ExpressionType.Expression,
                    "@concat('[DataverseToSql].', item().EntityName, '_TableType')"),
                StoredProcedureTableTypeParameterName = "entity"
            };

            var incrementalLoadActivity = new CopyActivity(
                "Incremental load",
                incrementalLoadSource,
                incrementalLoadSink)
            {
                EnableStaging = false
            };

            incrementalLoadActivity.Inputs.Add(new(DatasetReferenceType.DatasetReference,
                Naming.ServerlessDatasetName()));

            var incrementalOutputReference = new DatasetReference(DatasetReferenceType.DatasetReference,
                Naming.AzureSqlDatasetName());

            // The target schema and table name are dynamic and are
            // retrieved by the lookup activity
            incrementalOutputReference.Parameters["Schema"] = new Expression(
                ExpressionType.Expression,
                "@item().TargetSchema");
            incrementalOutputReference.Parameters["Table"] = new Expression(
                ExpressionType.Expression,
                "@item().TargetTable");

            incrementalLoadActivity.Outputs.Add(incrementalOutputReference);

            // Activity: Mark job complete
            // The activity marks an ingestion job as complete
            // The logic is implemented in the [DataverseToSql].[IngestionJobs_Complete]
            // stored procedure.

            var markBlobCompleteActivity = new SqlServerStoredProcedureActivity(
                "Mark job complete",
                "[DataverseToSql].[IngestionJobs_Complete]")
            {
                StoredProcedureParameters = new Dictionary<string, object>
                {
                    ["JobId"] = new StoredProcedureParameter()
                    {
                        Type = StoredProcedureParameterType.Int64,
                        Value = new Expression(
                        ExpressionType.Expression,
                        "@item().JobId"
                        )
                    }
                },
                LinkedServiceName = new(LinkedServiceReferenceType.LinkedServiceReference,
                    Naming.AzureSqlLinkedServiceName())
            };

            markBlobCompleteActivity.DependsOn.Add(
                new ActivityDependency(
                    incrementalLoadActivity.Name,
                    new List<DependencyCondition>() { new("Succeeded") }));

            // Activity: For each blob to ingest
            // The ForEach activity iterates over the result of the lookup activity
            // and invokes the switch activity that in turns invokes the copy

            var foreachBlobActivity = new ForEachActivity(
                "For each ingestion job",
                new Expression(ExpressionType.Expression, "@activity('Lookup ingestion jobs').output.value"),
                new List<Activity>()
                {
                    incrementalLoadActivity,
                    markBlobCompleteActivity
                })
            {
                BatchCount = environment.Config.Ingestion.Parallelism
            };

            foreachBlobActivity.DependsOn.Add(
                new ActivityDependency(
                    lookupIngestionJobsActivity.Name,
                    new List<DependencyCondition>() { new("Succeeded") }));

            pipeline.Activities.Add(foreachBlobActivity);

            // Activity: Fail pipeline
            // The activity runs when either of the foreach activities (optionsets
            // and blobs to ingest) fails.

            var failPipelineActivity = new FailActivity("Fail pipeline", "Pipeline failed", "1");

            failPipelineActivity.DependsOn.Add(
                new ActivityDependency(
                    foreachOptionsetTableActivity.Name,
                    new List<DependencyCondition>() { new("Failed") }));

            failPipelineActivity.DependsOn.Add(
                new ActivityDependency(
                    foreachBlobActivity.Name,
                    new List<DependencyCondition>() { new("Failed") }));

            pipeline.Activities.Add(failPipelineActivity);

            // Create the pipeline

            var pipelineClient = new PipelineClient(
                new Uri(environment.Config.SynapseWorkspace.DevEndpoint()),
                environment.Credential);

            CreatePipeline(
                pipelineClient,
                pipeline,
                Naming.IncrementalLoadPipelineName());
        }

        // Create the Synapse full load pipeline
        private void CreateFullLoadPipeline()
        {
            log.LogInformation("Creating full load pipeline");

            var pipeline = new PipelineResource
            {
                Folder = new PipelineFolder()
                {
                    Name = Naming.PipelineFolderName()
                },
                Concurrency = 1 // Set pipeline concurrency to 1 to prevent concurrent ingestion processes
            };

            // Activity: Root notebook
            var rootNotebookActivity = new SynapseNotebookActivity(
                "Execute root notebook",
                new SynapseNotebookReference(
                    NotebookReferenceType.NotebookReference,
                    Naming.RootNotebookName()))
            {
                SparkPool = new(
                BigDataPoolReferenceType.BigDataPoolReference,
                environment.Config.Spark.SparkPool),
                ExecutorSize = "Medium",
                DriverSize = "Medium",
                Conf = new Dictionary<string, object>()
                {
                    { "spark.dynamicAllocation.enabled", true },
                    { "spark.dynamicAllocation.minExecutors", 4 },
                    { "spark.dynamicAllocation.maxExecutors", 10 }
                }
            };

            rootNotebookActivity.Policy = new()
            {
                Timeout = "7.00:00:00",
                Retry = 0
            };

            rootNotebookActivity.Parameters["target_schema"] = new() { Type = new("string"), Value = environment.Config.Database.Schema };
            rootNotebookActivity.Parameters["storage_account"] = new() { Type = new("string"), Value = environment.Config.DataverseStorage.AccountName() };
            rootNotebookActivity.Parameters["container"] = new() { Type = new("string"), Value = environment.Config.DataverseStorage.Container };
            rootNotebookActivity.Parameters["servername"] = new() { Type = new("string"), Value = environment.Config.Database.Server };
            rootNotebookActivity.Parameters["dbname"] = new() { Type = new("string"), Value = environment.Config.Database.Database };
            rootNotebookActivity.Parameters["entity_concurrency"] = new() { Type = new("int"), Value = environment.Config.Spark.EntityConcurrency };

            pipeline.Activities.Add(rootNotebookActivity);

            // Create the pipeline
            var pipelineClient = new PipelineClient(
                new Uri(environment.Config.SynapseWorkspace.DevEndpoint()),
                environment.Credential);

            CreatePipeline(
                pipelineClient,
                pipeline,
                Naming.FullLoadPipelineName());
        }

        private void CreateRootNotebook()
        {
            var notebook = Encoding.UTF8.GetString(Notebooks.DataverseToSql_RootNotebook);

            CreateNotebook(
                Naming.RootNotebookName(),
                notebook.Replace(
                    "$$PROCESS_ENTITY_NOTEBOOK_NAME$$",
                    Naming.ProcessEntityNotebookName()));
        }

        private void CreateProcessEntityNotebook()
        {
            CreateNotebook(Naming.ProcessEntityNotebookName(), Notebooks.DataverseToSql_ProcessEntity);
        }

        private void CreateNotebook(string name, byte[] content)
        {
            CreateNotebook(name, Encoding.UTF8.GetString(content));
        }

        private void CreateNotebook(string name, string content)
        {
            log.LogInformation("Creating notebook {notebook}", name);

            var notebook = JsonSerializer.Deserialize<Notebook>(content);

            var notebookResource = new NotebookResource(
                name,
                notebook);

            notebookResource.Properties.BigDataPool = new BigDataPoolReference(
                BigDataPoolReferenceType.BigDataPoolReference,
                environment.Config.Spark.SparkPool);

            var notebookClient = new NotebookClient(
                new Uri(environment.Config.SynapseWorkspace.DevEndpoint()),
                environment.Credential);

            var createOp = notebookClient.StartCreateOrUpdateNotebook(
                name,
                notebookResource);

            createOp.WaitForCompletion();

            if (!createOp.HasValue)
            {
                var response = createOp.GetRawResponse();

                throw new Exception(
                    $"Creation of noetbook {notebookResource.Name} failed: {response.Status} {response.ReasonPhrase}");
            }

            var resp = Newtonsoft.Json.JsonConvert.DeserializeObject<dynamic>(
                createOp.GetRawResponse().Content.ToString());

            if (resp?.status?.Value?.ToLower() == "failed" ?? false)
            {
                string message = resp?.error.message.Value.ToString() ?? "unknown";

                throw new Exception($"Creation of notebook {notebookResource.Name} failed: {message}");
            }
        }

        private Dictionary<string, object>[] GetOptionsetTablesDefinition()
        {
            var optionsetTables = new List<Dictionary<string, object>>();

            static string serverlessQuery(Uri blobUri, string columns) => $@"
                SELECT
                    *
                FROM
                    OPENROWSET(
                        BULK '{blobUri}',
                        FORMAT = 'CSV',
                        PARSER_VERSION = '2.0'
                    ) 
                    WITH ({columns})
                    AS [result]";

            var blobUriBuilder = new BlobUriBuilder(environment.Config.DataverseStorage.ContainerUri());

            blobUriBuilder.BlobName = "Microsoft.Athena.TrickleFeedService/*-EntityMetadata.json";
            optionsetTables.Add(new Dictionary<string, object>
            {
                ["optionsetTable"] = "AttributeMetadata",
                ["serverlessQuery"] = $@"
                    SELECT
	                    0 [Id],
	                    [EntityName],
	                    [AttributeName],
	                    [AttributeType],
	                    [AttributeTypeCode],
	                    [Version],
	                    [Timestamp],
	                    [MetadataId],
	                    [Precision]
                    FROM 
	                    OPENROWSET(
                            BULK '{blobUriBuilder.ToUri().ToString().Replace("%2A-EntityMetadata.json", "*-EntityMetadata.json")}',
                            FORMAT = 'csv',
                            FIELDTERMINATOR ='0x0b',
                            FIELDQUOTE = '0x0b',
                            ROWTERMINATOR = '0x0b'
                        ) WITH (doc nvarchar(max)) as rows
	                    CROSS APPLY OPENJSON(doc, '$.AttributeMetadata') WITH (
		                    [EntityName] [nvarchar](64),
		                    [AttributeName] [nvarchar](64),
		                    [AttributeType] [nvarchar](64),
		                    [AttributeTypeCode] [int],
		                    [Version] [bigint],
		                    [Timestamp] [datetime],
		                    [MetadataId] [nvarchar](64),
		                    [Precision] [int],		
		                    [MaxLength] [int]
	                    )"
            });

            blobUriBuilder.BlobName = "OptionsetMetadata/OptionsetMetadata.csv";
            optionsetTables.Add(new Dictionary<string, object>
            {
                ["optionsetTable"] = "OptionsetMetadata",
                ["serverlessQuery"] = serverlessQuery(
                    blobUriBuilder.ToUri(), @"
                    [EntityName] [varchar](128),
                    [OptionSetName] [varchar](128),
                    [Option] [bigint],
                    [IsUserLocalizedLabel] [varchar](6),
                    [LocalizedLabelLanguageCode] [bigint],
                    [LocalizedLabel] [varchar](700)")
            });

            blobUriBuilder.BlobName = "OptionsetMetadata/GlobalOptionsetMetadata.csv";
            optionsetTables.Add(new Dictionary<string, object>
            {
                ["optionsetTable"] = "GlobalOptionsetMetadata",
                ["serverlessQuery"] = serverlessQuery(
                    blobUriBuilder.ToUri(), @"
                    [OptionSetName] [varchar](128),
                    [Option] [bigint],
                    [IsUserLocalizedLabel] [varchar](6),
                    [LocalizedLabelLanguageCode] [bigint],
                    [LocalizedLabel] [varchar](700),
                    [GlobalOptionSetName] [varchar](128),
                    [EntityName] [varchar](128)")
            });

            blobUriBuilder.BlobName = "OptionsetMetadata/StateMetadata.csv";
            optionsetTables.Add(new Dictionary<string, object>
            {
                ["optionsetTable"] = "StateMetadata",
                ["serverlessQuery"] = serverlessQuery(
                    blobUriBuilder.ToUri(), @"
                    [EntityName] [varchar](128),
                    [State] [bigint],
                    [IsUserLocalizedLabel] [varchar](6),
                    [LocalizedLabelLanguageCode] [bigint],
                    [LocalizedLabel] [varchar](700)")
            });

            blobUriBuilder.BlobName = "OptionsetMetadata/StatusMetadata.csv";
            optionsetTables.Add(new Dictionary<string, object>
            {
                ["optionsetTable"] = "StatusMetadata",
                ["serverlessQuery"] = serverlessQuery(
                    blobUriBuilder.ToUri(), @"
                    [EntityName] [varchar](128),
	                [State] [bigint],
	                [Status] [bigint],
	                [IsUserLocalizedLabel] [varchar](6),
	                [LocalizedLabelLanguageCode] [bigint],
	                [LocalizedLabel] [varchar](700)")
            });

            blobUriBuilder.BlobName = "OptionsetMetadata/TargetMetadata.csv";
            optionsetTables.Add(new Dictionary<string, object>
            {
                ["optionsetTable"] = "TargetMetadata",
                ["serverlessQuery"] = serverlessQuery(
                    blobUriBuilder.ToUri(), @"
                    [EntityName] [varchar](128),
                    [AttributeName] [varchar](128),
                    [ReferencedEntity] [varchar](128),
                    [ReferencedAttribute] [varchar](128)")
            });

            return optionsetTables.ToArray();
        }

        // Helper method to create a pipeline and check the operation result
        protected void CreatePipeline(PipelineClient pipelineClient, PipelineResource pipeline, string pipelineName)
        {
            log.LogInformation("Creating pipeline {pipeline}.", pipelineName);

            var createOp = pipelineClient.StartCreateOrUpdatePipeline(
                pipelineName,
                pipeline);

            createOp.WaitForCompletion();

            if (!createOp.HasValue)
            {
                var response = createOp.GetRawResponse();

                throw new Exception(
                    $"Creation of pipeline {pipelineName} failed: {response.Status} {response.ReasonPhrase}");
            }

            var resp = Newtonsoft.Json.JsonConvert.DeserializeObject<dynamic>(
                createOp.GetRawResponse().Content.ToString());

            if (resp?.status?.Value?.ToLower() == "failed" ?? false)
            {
                string message = resp?.error.message.Value.ToString() ?? "unknown";

                throw new Exception($"Creation of pipeline {pipelineName} failed: {message}");
            }
        }

        // Upload the configuration information to the configuration storage container.
        // The information consists of the DataverseToSql.json configuration file.
        private void UploadConfiguration()
        {
            log.LogInformation("Uploading configuration.");

            var blobContainerClient = new BlobContainerClient(
                environment.Config.ConfigurationStorage.ContainerUri(),
                environment.Credential);

            var blobClient = blobContainerClient.GetBlobClient($"/{EnvironmentBase.CONFIG_FILE}");

            blobClient.Upload(
                content: File.Open(environment.Config.ConfigFilePath, FileMode.Open),
                overwrite: true);
        }

        // Upload the custom data type map file, if present
        private void UploadCustomDatatypeMap()
        {
            var customDatatypeMapPath = Path.Join(environment.LocalPath, EnvironmentBase.CUSTOM_DATATYPE_MAP);

            if (File.Exists(customDatatypeMapPath))
            {
                log.LogInformation("Uploading custom data type map.");

                var blobContainerClient = new BlobContainerClient(
                    environment.Config.ConfigurationStorage.ContainerUri(),
                    environment.Credential);

                var blobClient = blobContainerClient.GetBlobClient($"/{EnvironmentBase.CUSTOM_DATATYPE_MAP}");

                blobClient.Upload(
                    content: File.Open(customDatatypeMapPath, FileMode.Open),
                    overwrite: true);
            }
            else
            {
                log.LogInformation("No custom data type map to upload.");
            }
        }

        // Upload custom SQL objects to the configuration storage container.
        private void UploadCustomSqlObjects()
        {
            log.LogInformation("Uploading custom SQL objects.");

            var customSqlObjectsFolder = Path.Combine(environment.LocalPath, EnvironmentBase.CUSTOM_SQL_OBJECTS_FOLDER);

            var parser = new TSql160Parser(true, SqlEngineType.SqlAzure);

            if (Directory.Exists(customSqlObjectsFolder))
            {
                foreach (var item in Directory.EnumerateFiles(customSqlObjectsFolder, "*", new EnumerationOptions() { RecurseSubdirectories = true }))
                {
                    var streamReader = new StreamReader(item, new FileStreamOptions() { Access = FileAccess.Read, Share = FileShare.Read });
                    parser.Parse(streamReader, out var parseErrors);
                    streamReader.Close();

                    // Check if there is any syntax error
                    if (parseErrors.Count > 0)
                    {
                        foreach (var error in parseErrors)
                        {
                            log.LogWarning(
                                "Error in source file {source}: Code {errorcode}, Line {line}, Column {column}, {message}",
                                item,
                                error.Number,
                                error.Line,
                                error.Column,
                                error.Message);
                        }
                    }
                    else
                    {
                        var relativePath = Path.GetRelativePath(customSqlObjectsFolder, item);

                        var blobUriBuilder = new BlobUriBuilder(environment.Config.ConfigurationStorage.ContainerUri());
                        blobUriBuilder.BlobName = EnvironmentBase.CUSTOM_SQL_OBJECTS_FOLDER + "/" + relativePath.Replace("\\", "/");

                        UploadFile(item, blobUriBuilder.ToUri());
                    }
                }
            }
            else
            {
                log.LogWarning("Custom SQL Object folder could not be found: {customSqlObjectsFolder}", customSqlObjectsFolder);
            }
        }

        private void UploadFile(string sourcePath, Uri targetUri)
        {
            log.LogInformation("Uploading {sourcePath} to {targetUri}", sourcePath, targetUri);

            var blobClient = new BlobClient(targetUri, environment.Credential);

            blobClient.Upload(
                content: File.Open(sourcePath, FileMode.Open, FileAccess.Read),
                overwrite: true);
        }
    }
}
