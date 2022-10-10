-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT License.

CREATE TABLE [$$SCHEMA$$].[TargetMetadata] (
	[EntityName] [varchar](128) NULL,
	[AttributeName] [varchar](128) NULL,
	[ReferencedEntity] [varchar](128) NULL,
	[ReferencedAttribute] [varchar](128) NULL
);
GO

CREATE TYPE [DataverseToSql].[TargetMetadata_TableType] AS TABLE (
	[EntityName] [varchar](128) NULL,
	[AttributeName] [varchar](128) NULL,
	[ReferencedEntity] [varchar](128) NULL,
	[ReferencedAttribute] [varchar](128) NULL
);
GO

CREATE PROCEDURE [DataverseToSql].[Merge_TargetMetadata]
	@source [DataverseToSql].[TargetMetadata_TableType] READONLY
AS
BEGIN TRAN

TRUNCATE TABLE [$$SCHEMA$$].[TargetMetadata]

INSERT [$$SCHEMA$$].[TargetMetadata]
SELECT * FROM @source

COMMIT