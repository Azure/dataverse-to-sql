﻿-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT License.

CREATE TABLE [$$SCHEMA$$].[TargetMetadata] (
	[EntityName] [nvarchar](64) NULL,
	[AttributeName] [nvarchar](64) NULL,
	[ReferencedEntity] [nvarchar](64) NULL,
	[ReferencedAttribute] [nvarchar](64) NULL
);
GO

CREATE TYPE [DataverseToSql].[TargetMetadata_TableType] AS TABLE (
	[EntityName] [nvarchar](64) NULL,
	[AttributeName] [nvarchar](64) NULL,
	[ReferencedEntity] [nvarchar](64) NULL,
	[ReferencedAttribute] [nvarchar](64) NULL
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