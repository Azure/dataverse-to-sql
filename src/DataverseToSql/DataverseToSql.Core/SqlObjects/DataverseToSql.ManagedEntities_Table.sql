-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT License.

CREATE TABLE [DataverseToSql].[ManagedEntities] (
	[EntityName] [DataverseToSql].[EntityType] NOT NULL PRIMARY KEY,
	[State] INT NOT NULL,
	[SchemaHash] NVARCHAR(128) NULL,
	[TargetSchema] SYSNAME NOT NULL,
	[TargetTable] SYSNAME NOT NULL,
	[FullLoadInnerQuery] NVARCHAR(MAX) NULL,
	[IncrementalInnerQuery] NVARCHAR(MAX) NULL
)