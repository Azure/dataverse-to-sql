-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT License.

CREATE TABLE [$$SCHEMA$$].[StatusMetadata] (
	[EntityName] [varchar](128) NULL,
	[State] [bigint] NULL,
	[Status] [bigint] NULL,
	[IsUserLocalizedLabel] [varchar](6) NULL,
	[LocalizedLabelLanguageCode] [bigint] NULL,
	[LocalizedLabel] [varchar](700) NULL
);
GO

CREATE TYPE [DataverseToSql].[StatusMetadata_TableType] AS TABLE (
	[EntityName] [varchar](128) NULL,
	[State] [bigint] NULL,
	[Status] [bigint] NULL,
	[IsUserLocalizedLabel] [varchar](6) NULL,
	[LocalizedLabelLanguageCode] [bigint] NULL,
	[LocalizedLabel] [varchar](700) NULL
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