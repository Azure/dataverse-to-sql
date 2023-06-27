// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure;
using Azure.Analytics.Synapse.Artifacts;
using Azure.Analytics.Synapse.Artifacts.Models;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using BlockBlobClientCopyRangeExtension;
using DataverseToSql.Core.Model;
using Microsoft.Azure.Management.Synapse.Models;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;

namespace DataverseToSql.Core.Jobs
{
    /// <summary>
    /// Implements the logic to:
    /// - update the target database schema when the source entity schema changes
    /// - deploy the schema of newly added entities
    /// - perform the initial ingestion of newly added entitites
    /// - detect new partitions added to existing entities
    /// - perform the incremental ingestion of existing entities
    /// - start the Synapse pipeline to copy data into the target database
    /// </summary>
    public class IngestionJob
    {
        private readonly ILogger log;
        private readonly EnvironmentBase environment;

        private readonly string ingestionTimestamp;

        public IngestionJob(ILogger log, EnvironmentBase environment)
        {
            this.log = log;
            this.environment = environment;
            ingestionTimestamp = DateTime.UtcNow.ToString("yyyyMMddHHmmss");
        }

        public async Task RunAsync(CancellationToken cancellationToken = default)
        {
            // Acquire the ingestion lock to avoid two ingestion processes to run concurrently
            var bub = new BlobUriBuilder(environment.Config.ConfigurationStorage.ContainerUri())
            {
                BlobName = "Locks/IncrementalLoad.lck"
            };

            BlobLock loadLock = new(
                blobClient: new(bub.ToUri(), environment.Credential));

            log.LogInformation("Acquiring ingestion lock.");

            if (!loadLock.TryAcquire())
            {
                log.LogWarning("Could not acquire ingestion lock. Another job is already in progress.");
                return;
            }

            try
            {
                // Check if there is any change in the schema and update the database accordingly
                await ProcessSchemaChanges(cancellationToken);

                // Iterate over all managed entities and perform either initial or incremental load
                // based on the state.

                if (!int.TryParse(Environment.GetEnvironmentVariable("ASYNC_PARALLELISM"), out int parallelism))
                    parallelism = 16;

                var parallelOps = new ParallelOptions()
                {
                    MaxDegreeOfParallelism = parallelism,
                    CancellationToken = cancellationToken
                };

                // pre-load CDM entities
                await environment.GetCdmEntityDictAsync(cancellationToken);

                await Parallel.ForEachAsync(
                    await environment.GetManagedEntitiesAsync(cancellationToken),
                    parallelOps,
                    async (managedEntity, ct) =>
                    {
                        var (found, cdmEntity) = await environment.TryGetCdmEntityAsync(managedEntity, ct);
                        if (found && cdmEntity is not null)
                        {
                            switch (managedEntity.State)
                            {
                                case ManagedEntityState.PendingInitialIngestion:
                                    await InitialLoad(managedEntity, cdmEntity, ct);
                                    break;
                                case ManagedEntityState.Ready:
                                    await IncrementalLoad(managedEntity, cdmEntity, ct);
                                    break;
                            }
                        }
                        else
                        {
                            log.LogWarning("Entity {entity}: the storage account does not contain the entity.", managedEntity.Name);
                        }
                    });

                // If there is any blob to be ingested, start the Synapse ingestion pipeline
                if (await environment.database.AreBlobsPendingIngestion(cancellationToken))
                    StartIngestionPipeline();
                else
                    log.LogInformation("No new data to process. The ingestion pipeline will not be started.");

                // Remove completed incremental blobs
                await DeleteCompletedBlobsToIngestAsync(parallelOps, cancellationToken);
            }
            finally
            {
                loadLock.Release();
            }
        }

        private async Task DeleteCompletedBlobsToIngestAsync(ParallelOptions parallelOps, CancellationToken cancellationToken)
        {
            await Parallel.ForEachAsync(
                await environment.database.GetCompletedBlobsToIngest(cancellationToken),
                parallelOps,
                async (blob, ct) =>
                {
                    log.LogInformation("Deleting completed incremental blob {incrementalBlobName}", blob.BlobName);

                    var blobClient = new BlobClient(new Uri(blob.BlobName), environment.Credential);
                    await blobClient.DeleteAsync(cancellationToken: cancellationToken);

                    await environment.database.DeleteBlobToIngest(blob.Id, cancellationToken);
                });
        }

        private async Task ProcessSchemaChanges(CancellationToken cancellationToken)
        {
            // Check if there is any new entity that must be created
            // or any existing entity whose schema changed since last deployment.
            // Check for EnableSchemaUpgradeForExistingTables setting; if disabled
            // do not take existing tables into consideration.
            if (    // Any new entities?
                    (await environment.GetManagedEntitiesAsync(cancellationToken))
                    .Where(e => e.State == ManagedEntityState.New).Any()
                    // Any changed/new custom script?
                    || await HasAnyCustomScriptChangedAsync(cancellationToken)
                    // Any existing entity changed?
                    || (environment.Config.SchemaHandling.EnableSchemaUpgradeForExistingTables
                        && await HasAnyExistingSchemaChangedAsync(cancellationToken)))
            {
                log.LogInformation("Detected schema changes.");
                await DeploySqlSchemaAsync(cancellationToken);
                await UpdateServerlessQueries(cancellationToken);
            }
        }

        private async Task UpdateServerlessQueries(CancellationToken cancellationToken)
        {
            log.LogInformation("Updating serverless queries.");

            foreach (var managedEntity in await environment.GetManagedEntitiesAsync(cancellationToken))
            {
                var (found, cdmEntity) = await environment.TryGetCdmEntityAsync(managedEntity, cancellationToken);
                if (found && cdmEntity is not null)
                {
                    managedEntity.OpenrowsetQuery = cdmEntity.GetServerlessOpenrowsetQuery();

                    var targetColumns = await environment.database.GetTableTypeColumnsAsync(
                        tableName: managedEntity.Name,
                        cancellationToken: cancellationToken);

                    managedEntity.InnerQuery = cdmEntity.GetServerlessInnerQuery(targetColumns);

                    await environment.database.UpsertAsync(managedEntity, cancellationToken);
                }
            }
        }

        // Compare the hash of each custom script with what previously stored
        private async Task<bool> HasAnyCustomScriptChangedAsync(CancellationToken cancellationToken)
        {
            foreach (var script in await environment.GetCustomScriptsAsync(cancellationToken))
            {
                var managedCustomScript = await environment.GetManagedCustomScript(script.name, cancellationToken);

                if (managedCustomScript is null) return true; // Detected a new custom script
                if (managedCustomScript.Hash != script.script.Sha1()) return true; // Detected change in the script
            }

            return false; // No change detected
        }

        // Compare the hash of the schema of each entity with what previously stored
        private async Task<bool> HasAnyExistingSchemaChangedAsync(CancellationToken cancellationToken)
        {
            // Check only existing (non-new) entities
            foreach (var managedEntity in (await environment.GetManagedEntitiesAsync(cancellationToken))
                .Where(e => e.State == ManagedEntityState.Ready))
            {
                var (found, cdmEntity) = await environment.TryGetCdmEntityAsync(managedEntity, cancellationToken);
                if (found && cdmEntity is not null)
                {
                    var entityScripts = cdmEntity.SqlScripts(environment.Config.Database.Schema
                        , environment.Config.SchemaHandling.SkipIsDeleteColumn);
                    if (managedEntity.SchemaHash != entityScripts.Sha1())
                        return true;
                }
            }
            return false;
        }

        // Deploy the latest version of the schema to the database and store
        // the schema hash
        private async Task DeploySqlSchemaAsync(CancellationToken cancellationToken)
        {
            log.LogInformation("Generating database model.");

            // Add the scripts to generate objects belonging to each entity
            // Consider only new entities if the EnableSchemaUpgradeForExistingTables is disabled
            var scriptedManagedEntities = new List<ManagedEntity>();
            foreach (var managedEntity in (await environment.GetManagedEntitiesAsync(cancellationToken))
                .Where(e => environment.Config.SchemaHandling.EnableSchemaUpgradeForExistingTables
                || e.State == ManagedEntityState.New))
            {
                var (found, cdmEntity) = await environment.TryGetCdmEntityAsync(managedEntity, cancellationToken);
                if (found && cdmEntity is not null)
                {
                    if (managedEntity.SchemaHash != cdmEntity.SqlScripts(environment.Config.Database.Schema,
                        environment.Config.SchemaHandling.SkipIsDeleteColumn))
                    {
                        var entitySqlScripts = cdmEntity.SqlScripts(environment.Config.Database.Schema,
                            environment.Config.SchemaHandling.SkipIsDeleteColumn);
                        managedEntity.SchemaHash = entitySqlScripts.Sha1();
                        scriptedManagedEntities.Add(managedEntity);
                        environment.database.AddObjects(entitySqlScripts);
                    }
                }
            }

            // Add custom objects
            environment.database.TryAddObjects(
                (await environment.GetCustomScriptsAsync(cancellationToken)));

            // Deploy the schema
            environment.database.DeployModel();

            // Update metadata
            foreach (var managedEntity in scriptedManagedEntities)
            {
                // Set processed entities state to PendingInitialIngestion if they where new
                if (managedEntity.State == ManagedEntityState.New)
                    managedEntity.State = ManagedEntityState.PendingInitialIngestion;
                await environment.database.UpsertAsync(managedEntity, cancellationToken);
            }

            // Update custom scripts metadata
            foreach (var (name, script) in (await environment.GetCustomScriptsAsync(cancellationToken)))
            {
                var managedCustomScript = new ManagedCustomScript(
                    name,
                    script.Sha1());

                await environment.database.UpsertAsync(managedCustomScript, cancellationToken);
            }
        }

        // Perform the initial load of a newly added entity
        private async Task InitialLoad(ManagedEntity managedEntity, CdmModel.CdmEntity cdmEntity, CancellationToken cancellationToken)
        {
            // Check that the target table is empty
            if (!await environment.database.IsTableEmptyAsync(
                    name: managedEntity.Name,
                    schema: environment.Config.Database.Schema
                    , cancellationToken))
            {
                log.LogError(
                    "Entity {entity}: the entity is pending initial load and the target table is not empty. The entity will be skipped.",
                    managedEntity.Name);
                return;
            }

            // Process each entity's partition individually
            var partitionCount = 0;
            foreach (var partitionGroup in cdmEntity.PartitionGroups)
            {
                log.LogInformation(
                    "Entity {entity}: processing partition group {partitionGroup}.",
                    managedEntity.Name,
                    partitionGroup.Name);

                var targetPartitionUris = new List<Uri>();

                var blobsToIngest = new List<BlobToIngest>();

                foreach (var partition in partitionGroup.Partitions)
                {
                    log.LogInformation(
                        "Entity {entity}: adding partition {partition} for initial ingestion.",
                        managedEntity.Name,
                        partition.Name);

                    partitionCount++;

                    var sourcePartitionUri = new BlobUriBuilder(environment.Config.DataverseStorage.ContainerUri())
                    {
                        BlobName = $"{managedEntity.Name}/{partition.Name}.csv"
                    }.ToUri();

                    var targetPartitionUri = new BlobUriBuilder(environment.Config.IncrementalStorage.ContainerUri())
                    {
                        BlobName = $"{managedEntity.Name}/{partition.Name}_{ingestionTimestamp}.csv"
                    }.ToUri();

                    // Copy the current partition data to the incremental storage
                    await CopyBlob(sourcePartitionUri, targetPartitionUri, cancellationToken);

                    var blobClient = new BlobClient(targetPartitionUri, environment.Credential);

                    // Record the size of the partition into the ManagedBlobs table
                    // as the starting offset of later incremental ingestion
                    var managedBlob = new ManagedBlob(
                        managedEntity,
                        $"{partition.Name}.csv",
                        (await blobClient.GetPropertiesAsync(cancellationToken: cancellationToken)).Value.ContentLength
                        );
                    await environment.database.UpsertAsync(managedBlob, cancellationToken);

                    targetPartitionUris.Add(targetPartitionUri);

                    // Record the metadata of the blob for ingestion
                    blobsToIngest.Add(new BlobToIngest(
                        managedEntity,
                        targetPartitionUri.ToString(),
                        partitionGroup.Name,
                        LoadType.Full));
                }

                // Write blobs belonging to the same partition using
                // a transaction
                await environment.database.InsertAsync(blobsToIngest, cancellationToken);
            }
            if (partitionCount > 0)
            {
                managedEntity.State = ManagedEntityState.IngestionInProgress;
                await environment.database.UpsertAsync(managedEntity, cancellationToken);
            }
            else
            {
                log.LogInformation("Entity {entity}: no partition exists yet for initial ingestion.", managedEntity.Name);
            }
        }

        // Copy blob with retry logic when changes are detected
        private async Task CopyBlob(Uri sourceBlob, Uri targetBlob, CancellationToken cancellationToken)
        {
            log.LogInformation("Copying {sourceBlob} to {targetBlob}", sourceBlob, targetBlob);

            var retryCount = 0;
            var blockClient = new BlobClient(targetBlob, environment.Credential);

            while (true)
            {
                try
                {
                    var copy = await blockClient.StartCopyFromUriAsync(sourceBlob, cancellationToken: cancellationToken);
                    await copy.WaitForCompletionAsync(cancellationToken);
                    return;
                }
                catch (RequestFailedException ex)
                {
                    if (ex.ErrorCode != "BlobModifiedWhileReading")
                        throw;

                    var retryIntervalMs = 1000 << retryCount++;

                    log.LogWarning(
                        "Skipping modified blob {sourceBlob}, waiting {retryIntervalMs} ms.",
                        sourceBlob,
                        retryIntervalMs);

                    Thread.Sleep(retryIntervalMs);

                    if (retryCount >= 5)
                        throw;
                }
            }
        }

        // Perform the incremental load of an existing entity
        private async Task IncrementalLoad(ManagedEntity managedEntity, CdmModel.CdmEntity cdmEntity, CancellationToken cancellationToken)
        {
            // Generate the Authorization header for the storage copy API.
            var sourceAuthentication = new HttpAuthorization(
                "Bearer",
                environment.Credential.GetToken(new[] { "https://storage.azure.com/.default" }));

            // Get the list of known (managed) blobs of the entity
            var managedBlobs = await environment.database.GetManagedBlobsAsync(managedEntity, cancellationToken);

            // Check if the the entity has new partitions and add them
            // to the list of managed blobs
            await ProcessNewPartitions(managedEntity, managedBlobs, cancellationToken);

            log.LogInformation("Entity {entity}: performing incremental load.", managedEntity.Name);

            var newData = false;

            // Process each blob separately
            foreach (var managedBlob in managedBlobs)
            {
                var sourceBlobUri = new BlobUriBuilder(environment.Config.DataverseStorage.ContainerUri())
                {
                    BlobName = $"{managedEntity.Name}/{managedBlob.Name}"
                }.ToUri();

                var blockClient = new BlockBlobClient(
                    sourceBlobUri,
                    environment.Credential
                    );

                var currentBlobSize = (await blockClient.GetPropertiesAsync(cancellationToken: cancellationToken)).Value.ContentLength;

                // Check if there are new data in the blob by comparing the current size with the previously
                // stored size (offset). Since the partition is append-only, a bigger blob means new data.
                // If there are new data, copy them to a separate blob that will later be ingested by the
                // dedicated Synapse pipeline.
                if (currentBlobSize > managedBlob.Offset)
                {
                    newData = true;

                    var newBlockSize = currentBlobSize - managedBlob.Offset;

                    var targetBlobName = string.Format(
                        "{0}_{1}.csv",
                        Path.GetFileNameWithoutExtension(managedBlob.Name),
                        ingestionTimestamp);

                    var targetBlobUri = new BlobUriBuilder(environment.Config.IncrementalStorage.ContainerUri())
                    {
                        BlobName = $"{managedEntity.Name}/{targetBlobName}"
                    }.ToUri();

                    log.LogInformation("Entity {entity}: copying {bytes} bytes from {sourceUri} to {targetUri}.",
                        managedEntity.Name,
                        newBlockSize,
                        sourceBlobUri.ToString(),
                        targetBlobUri.ToString());

                    var targetBlockClient = new BlockBlobClient(
                        targetBlobUri,
                        environment.Credential);

                    // Copy the new block of data from the end of the source blob
                    // to the target blob.
                    await targetBlockClient.CopyRangeFromUriAsync(
                        sourceBlobUri,
                        sourceAuthentication,
                        managedBlob.Offset,
                        newBlockSize,
                        cancellationToken: cancellationToken);

                    managedBlob.Offset = currentBlobSize;
                    await environment.database.UpsertAsync(managedBlob, cancellationToken);

                    // Record the metadata of the new blob for ingestion
                    await environment.database.InsertAsync(
                        new List<BlobToIngest> {
                            new BlobToIngest(
                                managedEntity,
                                targetBlobUri.ToString(),
                                "",
                                LoadType.Incremental
                                ) },
                        cancellationToken);
                }
            }

            if (!newData)
                log.LogInformation("Entity {entity}: no new data.", managedEntity.Name);
        }

        // Check if the the entity has new partitions (blobs) that
        // are not yet tracked and add them to the list of managed blobs.
        // Record the new blobs to the database and add them to the
        // managedBlob list.
        private async Task ProcessNewPartitions(
            ManagedEntity managedEntity,
            IList<ManagedBlob> managedBlobs,
            CancellationToken cancellationToken)
        {
            log.LogInformation("Entity {entity}: checking for new partitions.", managedEntity.Name);

            HashSet<string> managedBlobNames = new(managedBlobs.Select(f => f.Name.ToLower()));

            var containerClient = new BlobContainerClient(
                environment.Config.DataverseStorage.ContainerUri(),
                environment.Credential);

            // Enumerate the blobs under the entity directory on the storage account
            await foreach (var item in containerClient.GetBlobsByHierarchyAsync(
                delimiter: "/",
                prefix: $"{managedEntity.Name}/",
                cancellationToken: cancellationToken))
            {
                if (item is not null && item.IsBlob)
                {
                    var blobname = item.Blob.Name.Split("/").Last();

                    // Check if the blob name ends with .csv and whether it is
                    // already part of the managed blobs.
                    // If not part already, add it to the managed blobs.
                    if (blobname.ToLower().EndsWith(".csv")
                        && !managedBlobNames.Contains(blobname.ToLower()))
                    {
                        log.LogInformation(
                            "Entity {entity}: Detected new partition {blobname}.",
                            managedEntity.Name,
                            blobname);

                        var newManagedBlob = new ManagedBlob(
                            managedEntity,
                            blobname,
                            offset: 0);

                        await environment.database.UpsertAsync(newManagedBlob, cancellationToken);
                        managedBlobs.Add(newManagedBlob);
                    }
                }
            }
        }

        // Start the Synapse ingestion pipeline
        private void StartIngestionPipeline()
        {
            // Check if the pipeline is already running or queued
            // for runs in the last 24 hours
            var runClient = new PipelineRunClient(
                new Uri(environment.Config.SynapseWorkspace.DevEndpoint()),
                environment.Credential);

            // Filter by runs in the last 24 hours
            var filter = new RunFilterParameters(
                DateTime.UtcNow.AddDays(-1),
                DateTime.UtcNow);

            // Filter runs for the specific pipeline
            filter.Filters.Add(new(
                new("PipelineName"),
                RunQueryFilterOperator.EqualsValue,
                new List<string>() { Naming.IngestionPipelineName() }));

            // Filter runs status InProgress or Queued
            filter.Filters.Add(new(
                new("Status"),
                RunQueryFilterOperator.In,
                new List<string>() { "InProgress", "Queued" }));

            // Query the runs
            var queryResponse = runClient.QueryPipelineRunsByWorkspace(filter).Value;

            // If there is any run either in progress or queued, skip the execution
            if (queryResponse.Value.Count > 0)
            {
                log.LogInformation(
                    "Pipeline {pipeline} already running. Check pipeline runs in Synapse Studio for progress.",
                    Naming.IngestionPipelineName());
                return;
            }

            var pipelineClient = new PipelineClient(
                new Uri(environment.Config.SynapseWorkspace.DevEndpoint()),
                environment.Credential);

            // Create the pipeline run
            var response = pipelineClient.CreatePipelineRun(
                Naming.IngestionPipelineName()).Value;

            log.LogInformation(
                "Pipeline {pipeline} successfully started with Run ID {runID}. Check pipeline runs in Synapse Studio for progress.",
                Naming.IngestionPipelineName(),
                response.RunId);
        }
    }
}
