-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT License.

CREATE TABLE [$$SCHEMA$$].[OptionsetMetadata] (
	[EntityName] [varchar](128) NULL,
	[OptionSetName] [varchar](128)  NULL,
	[Option] [bigint] NULL,
	[IsUserLocalizedLabel] [varchar](6) NULL,
	[LocalizedLabelLanguageCode] [bigint] NULL,
	[LocalizedLabel] [varchar](700)  NULL
);
GO

CREATE TYPE [DataverseToSql].[OptionsetMetadata_TableType] AS TABLE (
	[EntityName] [varchar](128) NULL,
	[OptionSetName] [varchar](128)  NULL,
	[Option] [bigint] NULL,
	[IsUserLocalizedLabel] [varchar](6) NULL,
	[LocalizedLabelLanguageCode] [bigint] NULL,
	[LocalizedLabel] [varchar](700)  NULL
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