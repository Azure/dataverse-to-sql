-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT License.

CREATE TABLE [$$SCHEMA$$].[OptionsetMetadata] (
	[EntityName] [nvarchar](64) NULL,
	[OptionSetName] [nvarchar](64)  NULL,
	[Option] [int] NULL,
	[IsUserLocalizedLabel] [bit] NULL,
	[LocalizedLabelLanguageCode] [int] NULL,
	[LocalizedLabel] [nvarchar](350)  NULL
);
GO

CREATE TYPE [DataverseToSql].[OptionsetMetadata_TableType] AS TABLE (
	[EntityName] [nvarchar](64) NULL,
	[OptionSetName] [nvarchar](64)  NULL,
	[Option] [int] NULL,
	[IsUserLocalizedLabel] [bit] NULL,
	[LocalizedLabelLanguageCode] [int] NULL,
	[LocalizedLabel] [nvarchar](350)  NULL
);
GO

CREATE PROCEDURE [DataverseToSql].[Merge_OptionsetMetadata]
	@source [DataverseToSql].[OptionsetMetadata_TableType] READONLY
AS
BEGIN TRAN

TRUNCATE TABLE [$$SCHEMA$$].[OptionsetMetadata]

INSERT [$$SCHEMA$$].[OptionsetMetadata]
SELECT * FROM @source

COMMIT