// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DataverseToSql.Core.Model
{
    /// <summary>
    /// Represents an entity tracked for incremental ingestion.
    /// </summary>
    public class ManagedEntity
    {
        // Name of the entity
        public string Name { get; set; } = "";
        // State of the entity inside the ingestion lifecycle
        public ManagedEntityState State { get; set; }
            = ManagedEntityState.New;
        // BASE64 encoded SHA1 of the latest SQL schema deployed for the entity
        public string? SchemaHash { get; set; } = null;
        // Name of the schema containing the table in the target SQL database
        public string TargetSchema { get; set; } = "";
        // Name of the the table in the target SQL database
        public string TargetTable { get; set; } = "";
        // Serverless query for deduplication of full loads
        public string? FullLoadInnerQuery { get; set; } = null;
        // Serverless query for deduplication of incremental loads
        public string? IncrementalInnerQuery { get; set; } = null;
        // Serverless OPENROWSET query
        public string? OpenrowsetQuery { get; set; } = null;
    }

    public enum ManagedEntityState
    {
        // New = the entity has been added but its schema has not yet been created
        New = 0,

        // PendingInitialIngestion = the schema of the entity has been created,
        // and the entity is waiting its initial load
        PendingInitialIngestion = 1,

        // IngestionInProgress = the intial ingestion of the entity is in progress
        IngestionInProgress = 2,

        // Ready = the initial load of the entity is complete and the entity is
        // ready for incremental loads
        Ready = 3
    }
}
