-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT License.

CREATE TABLE [$$SCHEMA$$].[StateMetadata] (
	[EntityName] [nvarchar](64) NULL,
	[State] [int] NULL,
	[IsUserLocalizedLabel] [bit] NULL,
	[LocalizedLabelLanguageCode] [int] NULL,
	[LocalizedLabel] [nvarchar](350) NULL
);
GO

CREATE TYPE [DataverseToSql].[StateMetadata_TableType] AS TABLE (
	[EntityName] [nvarchar](64) NULL,
	[State] [int] NULL,
	[IsUserLocalizedLabel] [bit] NULL,
	[LocalizedLabelLanguageCode] [int] NULL,
	[LocalizedLabel] [nvarchar](350) NULL
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