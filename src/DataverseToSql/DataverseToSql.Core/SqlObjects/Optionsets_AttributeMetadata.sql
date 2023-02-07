-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT License.

CREATE TABLE [$$SCHEMA$$].[AttributeMetadata] (
	[Id] [bigint] NULL,
	[EntityName] [nvarchar](64) NULL,
	[AttributeName] [nvarchar](64) NULL,
	[AttributeType] [nvarchar](64) NULL,
	[AttributeTypeCode] [int] NULL,
	[Version] [bigint] NULL,
	[Timestamp] [datetime] NULL,
	[MetadataId] [nvarchar](64) NULL,
	[Precision] [int] NULL
);
GO

CREATE TYPE [DataverseToSql].[AttributeMetadata_TableType] AS TABLE (
	[Id] [bigint] NULL,
	[EntityName] [nvarchar](64) NULL,
	[AttributeName] [nvarchar](64) NULL,
	[AttributeType] [nvarchar](64) NULL,
	[AttributeTypeCode] [int] NULL,
	[Version] [bigint] NULL,
	[Timestamp] [datetime] NULL,
	[MetadataId] [nvarchar](64) NULL,
	[Precision] [int] NULL
);
GO

CREATE PROCEDURE [DataverseToSql].[Merge_AttributeMetadata]
	@source [DataverseToSql].[AttributeMetadata_TableType] READONLY
AS
BEGIN TRAN

TRUNCATE TABLE [$$SCHEMA$$].[AttributeMetadata]

INSERT [$$SCHEMA$$].[AttributeMetadata]
SELECT * FROM @source

COMMIT