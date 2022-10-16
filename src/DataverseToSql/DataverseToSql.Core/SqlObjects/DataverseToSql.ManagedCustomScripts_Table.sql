-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT License.

CREATE TABLE [DataverseToSql].[ManagedCustomScripts] (
	[ScriptName] [DataverseToSql].[CustomScriptNameType] NOT NULL PRIMARY KEY,
	[Hash] NVARCHAR(128) NULL
)