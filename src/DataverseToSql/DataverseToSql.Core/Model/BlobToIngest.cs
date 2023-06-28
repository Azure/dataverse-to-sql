// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

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
            string basePath,
            string timestamp,
            string partition,
            LoadType loadType
            )
        {
            Entity = entity;
            Name = name;
            BasePath = basePath;
            Timestamp = timestamp;
            Partition = partition;
            LoadType = loadType;
        }

        // Reference to the managed entity the blob belongs to
        public ManagedEntity Entity { get; }
        // Name of the blob, as a full path in the container
        public string Name { get; }
        // Base path of the blob up to the entity level
        public string BasePath { get; }
        // Timestamp of the blob
        public string Timestamp { get; }
        // Name of the partition the blob belongs to
        public string Partition { get; }
        // Type of load operation, either full or incremenetal
        public LoadType LoadType { get; }
    }

    internal enum LoadType
    { 
        Full = 0,
        Incremental = 1
    }
}