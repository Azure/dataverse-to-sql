-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT License.

CREATE TABLE [$$SCHEMA$$].[StateMetadata] (
	[EntityName] [varchar](128) NULL,
	[State] [bigint] NULL,
	[IsUserLocalizedLabel] [varchar](6) NULL,
	[LocalizedLabelLanguageCode] [bigint] NULL,
	[LocalizedLabel] [varchar](700) NULL
);
GO

CREATE TYPE [DataverseToSql].[StateMetadata_TableType] AS TABLE (
	[EntityName] [varchar](128) NULL,
	[State] [bigint] NULL,
	[IsUserLocalizedLabel] [varchar](6) NULL,
	[LocalizedLabelLanguageCode] [bigint] NULL,
	[LocalizedLabel] [varchar](700) NULL
);
GO

CREATE PROCEDURE [DataverseToSql].[Merge_StateMetadata]
	@source [DataverseToSql].[StateMetadata_TableType] READONLY
AS
BEGIN TRAN

TRUNCATE TABLE [$$SCHEMA$$].[StateMetadata]

INSERT [$$SCHEMA$$].[StateMetadata]
SELECT * FROM @source

COMMIT