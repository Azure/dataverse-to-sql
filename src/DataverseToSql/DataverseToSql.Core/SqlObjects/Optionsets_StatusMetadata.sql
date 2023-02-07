-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT License.

CREATE TABLE [$$SCHEMA$$].[StatusMetadata] (
	[EntityName] [nvarchar](64) NULL,
	[State] [int] NULL,
	[Status] [int] NULL,
	[IsUserLocalizedLabel] [bit] NULL,
	[LocalizedLabelLanguageCode] [int] NULL,
	[LocalizedLabel] [nvarchar](350) NULL
);
GO

CREATE TYPE [DataverseToSql].[StatusMetadata_TableType] AS TABLE (
	[EntityName] [nvarchar](64) NULL,
	[State] [int] NULL,
	[Status] [int] NULL,
	[IsUserLocalizedLabel] [bit] NULL,
	[LocalizedLabelLanguageCode] [int] NULL,
	[LocalizedLabel] [nvarchar](350) NULL
);
GO

CREATE PROCEDURE [DataverseToSql].[Merge_StatusMetadata]
	@source [DataverseToSql].[StatusMetadata_TableType] READONLY
AS
BEGIN TRAN

TRUNCATE TABLE [$$SCHEMA$$].[StatusMetadata]

INSERT [$$SCHEMA$$].[StatusMetadata]
SELECT * FROM @source

COMMIT