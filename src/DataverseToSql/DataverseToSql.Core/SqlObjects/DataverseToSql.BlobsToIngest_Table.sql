-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT License.

CREATE TABLE [DataverseToSql].[BlobsToIngest] (
	[Id] BIGINT IDENTITY PRIMARY KEY,
	[EntityName] [DataverseToSql].[EntityType] NOT NULL,
	[BlobName] [DataverseToSql].[BlobNameType] NOT NULL,
	[Partition] [DataverseToSql].[BlobPartitionType] NOT NULL,
	[LoadType] INT NOT NULL, -- Full=0, Incremental=1 (from LoadType enum)
	[Complete] INT NOT NULL DEFAULT 0,
	CONSTRAINT UQ_BlobsToIngest UNIQUE ([EntityName], [BlobName]),
	CONSTRAINT FK_BlobsToIngest_Entities
		FOREIGN KEY ([EntityName]) REFERENCES [DataverseToSql].[ManagedEntities]([EntityName])
);
GO

CREATE INDEX [IX_BlobsToIngest_EntityName]
	ON [DataverseToSql].[BlobsToIngest]([EntityName])