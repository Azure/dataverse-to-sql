-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT License.

CREATE TABLE [DataverseToSql].[BlobsToIngest] (
	[Id] INT IDENTITY PRIMARY KEY,
	[EntityName] [DataverseToSql].[EntityType] NOT NULL,
	[BlobName] [DataverseToSql].[BlobNameType] NOT NULL,
	[TargetSchema] SYSNAME NOT NULL,
	[TargetTable] SYSNAME NOT NULL,
	[ServerlessQuery] NVARCHAR(MAX) NOT NULL,
	[LoadType] INT NOT NULL,
	[Complete] INT NOT NULL DEFAULT 0,
	CONSTRAINT UQ_BlobsToIngest UNIQUE ([EntityName], [BlobName]),
	CONSTRAINT FK_BlobsToIngest_Entities
		FOREIGN KEY ([EntityName]) REFERENCES [DataverseToSql].[ManagedEntities]([EntityName])
);