CREATE TABLE [$$SCHEMA$$].[GlobalOptionsetMetadata] (
	[OptionSetName] [varchar](128) NULL,
	[Option] [bigint] NULL,
	[IsUserLocalizedLabel] [varchar](6) NULL,
	[LocalizedLabelLanguageCode] [bigint] NULL,
	[LocalizedLabel] [varchar](700) NULL,
	[GlobalOptionSetName] [varchar](128) NULL,
	[EntityName] [varchar](128) NULL
);
GO

CREATE TYPE [DataverseToSql].[GlobalOptionsetMetadata_TableType] AS TABLE (
	[OptionSetName] [varchar](128) NULL,
	[Option] [bigint] NULL,
	[IsUserLocalizedLabel] [varchar](6) NULL,
	[LocalizedLabelLanguageCode] [bigint] NULL,
	[LocalizedLabel] [varchar](700) NULL,
	[GlobalOptionSetName] [varchar](128) NULL,
	[EntityName] [varchar](128) NULL
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