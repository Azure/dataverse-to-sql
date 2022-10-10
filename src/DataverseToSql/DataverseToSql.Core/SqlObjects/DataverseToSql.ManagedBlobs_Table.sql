-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT License.

CREATE TABLE [DataverseToSql].[ManagedBlobs] (
	[EntityName] [DataverseToSql].[EntityType] NOT NULL,
	[BlobName] [DataverseToSql].[BlobNameType] NOT NULL,
	[FileOffset] BIGINT NOT NULL,
	CONSTRAINT PK_ManagedBlobs PRIMARY KEY ([EntityName], [BlobName]),
	CONSTRAINT FK_ManagedBlobs_Entities FOREIGN KEY ([EntityName]) REFERENCES [DataverseToSql].[ManagedEntities]([EntityName])
);