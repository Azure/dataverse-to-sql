-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT License.

CREATE TABLE [$$SCHEMA$$].[GlobalOptionsetMetadata] (
	[OptionSetName] [nvarchar](64) NULL,
	[Option] [int] NULL,
	[IsUserLocalizedLabel] [bit] NULL,
	[LocalizedLabelLanguageCode] [int] NULL,
	[LocalizedLabel] [nvarchar](350) NULL,
	[GlobalOptionSetName] [nvarchar](64) NULL,
	[EntityName] [nvarchar](64) NULL
);
GO

CREATE TYPE [DataverseToSql].[GlobalOptionsetMetadata_TableType] AS TABLE (
	[OptionSetName] [nvarchar](64) NULL,
	[Option] [int] NULL,
	[IsUserLocalizedLabel] [bit] NULL,
	[LocalizedLabelLanguageCode] [int] NULL,
	[LocalizedLabel] [nvarchar](350) NULL,
	[GlobalOptionSetName] [nvarchar](64) NULL,
	[EntityName] [nvarchar](64) NULL
);
GO

CREATE PROCEDURE [DataverseToSql].[Merge_GlobalOptionsetMetadata]
	@source [DataverseToSql].[GlobalOptionsetMetadata_TableType] READONLY
AS
BEGIN TRAN

TRUNCATE TABLE [$$SCHEMA$$].[GlobalOptionsetMetadata]

INSERT [$$SCHEMA$$].[GlobalOptionsetMetadata]
SELECT * FROM @source

COMMIT