-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT License.

CREATE TYPE [DataverseToSql].[EntityType]
	FROM NVARCHAR(128);
GO

CREATE TYPE [DataverseToSql].[BlobNameType]
	FROM NVARCHAR(722);
GO

CREATE TYPE [DataverseToSql].[CustomScriptNameType]
	FROM NVARCHAR(512);
GO

CREATE TYPE [DataverseToSql].[BlobPartitionType]
	FROM NVARCHAR(512);
GO

CREATE TYPE [DataverseToSql].[JobIdType]
	FROM BIGINT;
GO