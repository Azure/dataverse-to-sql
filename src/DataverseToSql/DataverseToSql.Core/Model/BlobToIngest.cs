namespace DataverseToSql.Core.Model
{
    /// <summary>
    /// Represents a blob to be ingested by the Synapse ingestion pipeline.
    /// Data are stored in the BlobsToIngest table in Azure SQL Database.
    /// </summary>
    internal class BlobToIngest
    {
        internal BlobToIngest(
            ManagedEntity entity,
            string name,
            string targetSchema,
            string targetTable,
            string serverlessQuery,
            LoadType loadType
            )
        {
            Entity = entity;
            Name = name;
            TargetSchema = targetSchema;
            TargetTable = targetTable;
            ServerlessQuery = serverlessQuery;
            LoadType = loadType;
        }

        // Reference to the managed entity the blob belongs to
        public ManagedEntity Entity { get; }
        // Name of the blob, as a relative path in the container
        public string Name { get; }
        // Schema of the table the data are loaded to
        public string TargetSchema { get; }
        // Name of the table the data are loaded to
        public string TargetTable { get; }
        // Query to be executed in Serverless SQL Pool to read and deduplicate the source data
        public string ServerlessQuery { get; }
        // Type of load operation, either full or incremenetal
        public LoadType LoadType { get; }
    }

    internal enum LoadType
    { 
        Full = 0,
        Incremental = 1
    }
}