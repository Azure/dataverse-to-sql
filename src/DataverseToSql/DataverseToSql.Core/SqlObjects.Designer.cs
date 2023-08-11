﻿//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated by a tool.
//     Runtime Version:4.0.30319.42000
//
//     Changes to this file may cause incorrect behavior and will be lost if
//     the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

namespace DataverseToSql.Core {
    using System;
    
    
    /// <summary>
    ///   A strongly-typed resource class, for looking up localized strings, etc.
    /// </summary>
    // This class was auto-generated by the StronglyTypedResourceBuilder
    // class via a tool like ResGen or Visual Studio.
    // To add or remove a member, edit your .ResX file then rerun ResGen
    // with the /str option, or rebuild your VS project.
    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("System.Resources.Tools.StronglyTypedResourceBuilder", "17.0.0.0")]
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
    [global::System.Runtime.CompilerServices.CompilerGeneratedAttribute()]
    internal class SqlObjects {
        
        private static global::System.Resources.ResourceManager resourceMan;
        
        private static global::System.Globalization.CultureInfo resourceCulture;
        
        [global::System.Diagnostics.CodeAnalysis.SuppressMessageAttribute("Microsoft.Performance", "CA1811:AvoidUncalledPrivateCode")]
        internal SqlObjects() {
        }
        
        /// <summary>
        ///   Returns the cached ResourceManager instance used by this class.
        /// </summary>
        [global::System.ComponentModel.EditorBrowsableAttribute(global::System.ComponentModel.EditorBrowsableState.Advanced)]
        internal static global::System.Resources.ResourceManager ResourceManager {
            get {
                if (object.ReferenceEquals(resourceMan, null)) {
                    global::System.Resources.ResourceManager temp = new global::System.Resources.ResourceManager("DataverseToSql.Core.SqlObjects", typeof(SqlObjects).Assembly);
                    resourceMan = temp;
                }
                return resourceMan;
            }
        }
        
        /// <summary>
        ///   Overrides the current thread's CurrentUICulture property for all
        ///   resource lookups using this strongly typed resource class.
        /// </summary>
        [global::System.ComponentModel.EditorBrowsableAttribute(global::System.ComponentModel.EditorBrowsableState.Advanced)]
        internal static global::System.Globalization.CultureInfo Culture {
            get {
                return resourceCulture;
            }
            set {
                resourceCulture = value;
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to -- Copyright (c) Microsoft Corporation.
        ///-- Licensed under the MIT License.
        ///
        ///CREATE PROCEDURE [DataverseToSql].[BlobsToIngest_Insert]
        ///	@EntityName [DataverseToSql].[EntityType],
        ///	@BlobName [DataverseToSql].[BlobNameType],
        ///	@BasePath [DataverseToSql].[BlobNameType],
        ///	@Timestamp [DataverseToSql].[TimestampType],
        ///	@Partition [DataverseToSql].[BlobPartitionType],
        ///	@LoadType INT
        ///AS
        ///IF NOT EXISTS (
        ///	SELECT * FROM [DataverseToSql].[BlobsToIngest]
        ///	WHERE
        ///		[EntityName] = @EntityName
        ///		AND [BlobName] = [rest of string was truncated]&quot;;.
        /// </summary>
        internal static string DataverseToSql_BlobsToIngest_Insert_Proc {
            get {
                return ResourceManager.GetString("DataverseToSql_BlobsToIngest_Insert_Proc", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to -- Copyright (c) Microsoft Corporation.
        ///-- Licensed under the MIT License.
        ///
        ///CREATE TABLE [DataverseToSql].[BlobsToIngest] (
        ///	[Id] BIGINT IDENTITY PRIMARY KEY,
        ///	[EntityName] [DataverseToSql].[EntityType] NOT NULL,
        ///	[BlobName] [DataverseToSql].[BlobNameType] NOT NULL,
        ///	[BasePath] [DataverseToSql].[BlobNameType] NOT NULL,
        ///	[Timestamp] [DataverseToSql].[TimestampType] NOT NULL,
        ///	[Partition] [DataverseToSql].[BlobPartitionType] NOT NULL,
        ///	[LoadType] INT NOT NULL, -- Full=0, Incremental=1 (from LoadType [rest of string was truncated]&quot;;.
        /// </summary>
        internal static string DataverseToSql_BlobsToIngest_Table {
            get {
                return ResourceManager.GetString("DataverseToSql_BlobsToIngest_Table", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to -- Copyright (c) Microsoft Corporation.
        ///-- Licensed under the MIT License.
        ///
        ///CREATE PROCEDURE [DataverseToSql].[IngestionJobs_Complete]
        ///	@JobId [DataverseToSql].[JobIdType]
        ///AS
        ///
        ///DECLARE @CompletedBlobs TABLE (
        ///	[EntityName] [DataverseToSql].[EntityType],
        ///	[LoadType] INT
        ///)
        ///
        ///BEGIN TRAN
        ///
        ///UPDATE	a
        ///SET
        ///	[Complete] = 1
        ///OUTPUT inserted.[EntityName], inserted.[LoadType]
        ///	INTO @CompletedBlobs
        ///FROM
        ///	[DataverseToSql].[BlobsToIngest] a
        ///	INNER JOIN [DataverseToSql].[BlobsToIngest] b
        ///		ON a.[Id] &lt;= b. [rest of string was truncated]&quot;;.
        /// </summary>
        internal static string DataverseToSql_IngestionJobs_Complete_Proc {
            get {
                return ResourceManager.GetString("DataverseToSql_IngestionJobs_Complete_Proc", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to -- Copyright (c) Microsoft Corporation.
        ///-- Licensed under the MIT License.
        ///
        ///CREATE PROCEDURE [DataverseToSql].[IngestionJobs_Get]
        ///AS
        ///
        ///WITH 
        ///jobs AS (
        ///	SELECT
        ///		MAX([Id]) AS [JobId],
        ///		[EntityName],
        ///		[BasePath],
        ///		MIN([Timestamp]) AS [TimestampFrom],
        ///		MAX([Timestamp]) AS [TimestampTo],
        ///		[Partition],
        ///		[LoadType]
        ///	FROM
        ///		[DataverseToSql].[BlobsToIngest]
        ///	WHERE
        ///		[Complete] = 0
        ///	GROUP BY
        ///		[EntityName],
        ///		[BasePath],
        ///		[Partition],
        ///		[LoadType]
        ///)
        ///SELECT
        ///	j.[JobId],
        ///	e.[EntityName [rest of string was truncated]&quot;;.
        /// </summary>
        internal static string DataverseToSql_IngestionJobs_Get_Proc {
            get {
                return ResourceManager.GetString("DataverseToSql_IngestionJobs_Get_Proc", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to -- Copyright (c) Microsoft Corporation.
        ///-- Licensed under the MIT License.
        ///
        ///CREATE TABLE [DataverseToSql].[ManagedBlobs] (
        ///	[EntityName] [DataverseToSql].[EntityType] NOT NULL,
        ///	[BlobName] [DataverseToSql].[BlobNameType] NOT NULL,
        ///	[FileOffset] BIGINT NOT NULL,
        ///	CONSTRAINT PK_ManagedBlobs PRIMARY KEY ([EntityName], [BlobName]),
        ///	CONSTRAINT FK_ManagedBlobs_Entities FOREIGN KEY ([EntityName]) REFERENCES [DataverseToSql].[ManagedEntities]([EntityName])
        ///);.
        /// </summary>
        internal static string DataverseToSql_ManagedBlobs_Table {
            get {
                return ResourceManager.GetString("DataverseToSql_ManagedBlobs_Table", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to -- Copyright (c) Microsoft Corporation.
        ///-- Licensed under the MIT License.
        ///
        ///CREATE PROCEDURE [DataverseToSql].[ManagedBlobs_Upsert]
        ///	@EntityName [DataverseToSql].[EntityType],
        ///	@BlobName [DataverseToSql].[BlobNameType],
        ///	@FileOffset BIGINT
        ///AS
        ///IF NOT EXISTS (
        ///	SELECT * FROM [DataverseToSql].[ManagedBlobs]
        ///	WHERE
        ///		[EntityName] = @EntityName
        ///		AND [BlobName] = @BlobName
        ///)
        ///BEGIN
        ///	INSERT INTO [DataverseToSql].[ManagedBlobs](
        ///		[EntityName],
        ///		[BlobName],
        ///		[FileOffset]
        ///	)
        ///	VALUES (
        ///		@Entit [rest of string was truncated]&quot;;.
        /// </summary>
        internal static string DataverseToSql_ManagedBlobs_Upsert_Proc {
            get {
                return ResourceManager.GetString("DataverseToSql_ManagedBlobs_Upsert_Proc", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to -- Copyright (c) Microsoft Corporation.
        ///-- Licensed under the MIT License.
        ///
        ///CREATE TABLE [DataverseToSql].[ManagedCustomScripts] (
        ///	[ScriptName] [DataverseToSql].[CustomScriptNameType] NOT NULL PRIMARY KEY,
        ///	[Hash] NVARCHAR(128) NULL
        ///).
        /// </summary>
        internal static string DataverseToSql_ManagedCustomScripts_Table {
            get {
                return ResourceManager.GetString("DataverseToSql_ManagedCustomScripts_Table", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to -- Copyright (c) Microsoft Corporation.
        ///-- Licensed under the MIT License.
        ///
        ///CREATE PROCEDURE [DataverseToSql].[ManagedCustomScripts_Upsert]
        ///	@ScriptName [DataverseToSql].[CustomScriptNameType],
        ///	@Hash  NVARCHAR(128)
        ///AS
        ///IF NOT EXISTS (
        ///	SELECT * FROM [DataverseToSql].[ManagedCustomScripts]
        ///	WHERE
        ///		[ScriptName] = @ScriptName
        ///)
        ///BEGIN
        ///	INSERT INTO [DataverseToSql].[ManagedCustomScripts](
        ///		[ScriptName],
        ///		[Hash]
        ///	)
        ///	VALUES (
        ///		@ScriptName,
        ///		@Hash
        ///	)
        ///END
        ///ELSE
        ///BEGIN
        ///	UPDATE [DataverseToS [rest of string was truncated]&quot;;.
        /// </summary>
        internal static string DataverseToSql_ManagedCustomScripts_Upsert_Proc {
            get {
                return ResourceManager.GetString("DataverseToSql_ManagedCustomScripts_Upsert_Proc", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to -- Copyright (c) Microsoft Corporation.
        ///-- Licensed under the MIT License.
        ///
        ///CREATE TABLE [DataverseToSql].[ManagedEntities] (
        ///	[EntityName] [DataverseToSql].[EntityType] NOT NULL PRIMARY KEY,
        ///	[State] INT NOT NULL,
        ///	[SchemaHash] NVARCHAR(128) NULL,
        ///	[TargetSchema] SYSNAME NOT NULL,
        ///	[TargetTable] SYSNAME NOT NULL,
        ///	[FullLoadInnerQuery] NVARCHAR(MAX) NULL,
        ///	[IncrementalInnerQuery] NVARCHAR(MAX) NULL
        ///).
        /// </summary>
        internal static string DataverseToSql_ManagedEntities_Table {
            get {
                return ResourceManager.GetString("DataverseToSql_ManagedEntities_Table", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to -- Copyright (c) Microsoft Corporation.
        ///-- Licensed under the MIT License.
        ///
        ///CREATE PROCEDURE [DataverseToSql].[ManagedEntities_Upsert]
        ///	@EntityName [DataverseToSql].[EntityType],
        ///	@TargetSchema SYSNAME,
        ///	@TargetTable SYSNAME,
        ///	@State INT = NULL,
        ///	@SchemaHash NVARCHAR(128) = NULL,
        ///	@FullLoadInnerQuery NVARCHAR(MAX) = NULL,
        ///	@IncrementalInnerQuery NVARCHAR(MAX) = NULL
        ///AS
        ///IF NOT EXISTS (
        ///	SELECT * FROM [DataverseToSql].[ManagedEntities]
        ///	WHERE [EntityName] = @EntityName
        ///)
        ///BEGIN
        ///	INSERT INTO [D [rest of string was truncated]&quot;;.
        /// </summary>
        internal static string DataverseToSql_ManagedEntities_Upsert_Proc {
            get {
                return ResourceManager.GetString("DataverseToSql_ManagedEntities_Upsert_Proc", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to -- Copyright (c) Microsoft Corporation.
        ///-- Licensed under the MIT License.
        ///
        ///CREATE SCHEMA [DataverseToSql];.
        /// </summary>
        internal static string DataverseToSql_Schema {
            get {
                return ResourceManager.GetString("DataverseToSql_Schema", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to -- Copyright (c) Microsoft Corporation.
        ///-- Licensed under the MIT License.
        ///
        ///CREATE TYPE [DataverseToSql].[EntityType]
        ///	FROM NVARCHAR(128);
        ///GO
        ///
        ///CREATE TYPE [DataverseToSql].[BlobNameType]
        ///	FROM NVARCHAR(722);
        ///GO
        ///
        ///CREATE TYPE [DataverseToSql].[CustomScriptNameType]
        ///	FROM NVARCHAR(512);
        ///GO
        ///
        ///CREATE TYPE [DataverseToSql].[BlobPartitionType]
        ///	FROM NVARCHAR(512);
        ///GO
        ///
        ///CREATE TYPE [DataverseToSql].[JobIdType]
        ///	FROM BIGINT;
        ///GO
        ///
        ///CREATE TYPE [DataverseToSql].[TimestampType]
        ///	FROM NVARCHAR(14);
        /// [rest of string was truncated]&quot;;.
        /// </summary>
        internal static string DataverseToSql_Types {
            get {
                return ResourceManager.GetString("DataverseToSql_Types", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to -- Copyright (c) Microsoft Corporation.
        ///-- Licensed under the MIT License.
        ///
        ///CREATE TABLE [$$SCHEMA$$].[AttributeMetadata] (
        ///	[Id] [bigint] NULL,
        ///	[EntityName] [nvarchar](64) NULL,
        ///	[AttributeName] [nvarchar](64) NULL,
        ///	[AttributeType] [nvarchar](64) NULL,
        ///	[AttributeTypeCode] [int] NULL,
        ///	[Version] [bigint] NULL,
        ///	[Timestamp] [datetime] NULL,
        ///	[MetadataId] [nvarchar](64) NULL,
        ///	[Precision] [int] NULL
        ///);
        ///GO
        ///
        ///CREATE TYPE [DataverseToSql].[AttributeMetadata_TableType] AS TABLE (
        ///	[Id] [bigint] [rest of string was truncated]&quot;;.
        /// </summary>
        internal static string Optionsets_AttributeMetadata {
            get {
                return ResourceManager.GetString("Optionsets_AttributeMetadata", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to -- Copyright (c) Microsoft Corporation.
        ///-- Licensed under the MIT License.
        ///
        ///CREATE TABLE [$$SCHEMA$$].[GlobalOptionsetMetadata] (
        ///	[OptionSetName] [nvarchar](64) NULL,
        ///	[Option] [int] NULL,
        ///	[IsUserLocalizedLabel] [bit] NULL,
        ///	[LocalizedLabelLanguageCode] [int] NULL,
        ///	[LocalizedLabel] [nvarchar](350) NULL,
        ///	[GlobalOptionSetName] [nvarchar](64) NULL,
        ///	[EntityName] [nvarchar](64) NULL
        ///);
        ///GO
        ///
        ///CREATE TYPE [DataverseToSql].[GlobalOptionsetMetadata_TableType] AS TABLE (
        ///	[OptionSetName] [nvarchar]( [rest of string was truncated]&quot;;.
        /// </summary>
        internal static string Optionsets_GlobalOptionsetMetadata {
            get {
                return ResourceManager.GetString("Optionsets_GlobalOptionsetMetadata", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to -- Copyright (c) Microsoft Corporation.
        ///-- Licensed under the MIT License.
        ///
        ///CREATE TABLE [$$SCHEMA$$].[OptionsetMetadata] (
        ///	[EntityName] [nvarchar](64) NULL,
        ///	[OptionSetName] [nvarchar](64)  NULL,
        ///	[Option] [int] NULL,
        ///	[IsUserLocalizedLabel] [bit] NULL,
        ///	[LocalizedLabelLanguageCode] [int] NULL,
        ///	[LocalizedLabel] [nvarchar](350)  NULL
        ///);
        ///GO
        ///
        ///CREATE TYPE [DataverseToSql].[OptionsetMetadata_TableType] AS TABLE (
        ///	[EntityName] [nvarchar](64) NULL,
        ///	[OptionSetName] [nvarchar](64)  NULL,
        ///	[Optio [rest of string was truncated]&quot;;.
        /// </summary>
        internal static string Optionsets_OptionsetMetadata {
            get {
                return ResourceManager.GetString("Optionsets_OptionsetMetadata", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to -- Copyright (c) Microsoft Corporation.
        ///-- Licensed under the MIT License.
        ///
        ///CREATE TABLE [$$SCHEMA$$].[StateMetadata] (
        ///	[EntityName] [nvarchar](64) NULL,
        ///	[State] [int] NULL,
        ///	[IsUserLocalizedLabel] [bit] NULL,
        ///	[LocalizedLabelLanguageCode] [int] NULL,
        ///	[LocalizedLabel] [nvarchar](350) NULL
        ///);
        ///GO
        ///
        ///CREATE TYPE [DataverseToSql].[StateMetadata_TableType] AS TABLE (
        ///	[EntityName] [nvarchar](64) NULL,
        ///	[State] [int] NULL,
        ///	[IsUserLocalizedLabel] [bit] NULL,
        ///	[LocalizedLabelLanguageCode] [int] NU [rest of string was truncated]&quot;;.
        /// </summary>
        internal static string Optionsets_StateMetadata {
            get {
                return ResourceManager.GetString("Optionsets_StateMetadata", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to -- Copyright (c) Microsoft Corporation.
        ///-- Licensed under the MIT License.
        ///
        ///CREATE TABLE [$$SCHEMA$$].[StatusMetadata] (
        ///	[EntityName] [nvarchar](64) NULL,
        ///	[State] [int] NULL,
        ///	[Status] [int] NULL,
        ///	[IsUserLocalizedLabel] [bit] NULL,
        ///	[LocalizedLabelLanguageCode] [int] NULL,
        ///	[LocalizedLabel] [nvarchar](350) NULL
        ///);
        ///GO
        ///
        ///CREATE TYPE [DataverseToSql].[StatusMetadata_TableType] AS TABLE (
        ///	[EntityName] [nvarchar](64) NULL,
        ///	[State] [int] NULL,
        ///	[Status] [int] NULL,
        ///	[IsUserLocalizedLabel] [bi [rest of string was truncated]&quot;;.
        /// </summary>
        internal static string Optionsets_StatusMetadata {
            get {
                return ResourceManager.GetString("Optionsets_StatusMetadata", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to -- Copyright (c) Microsoft Corporation.
        ///-- Licensed under the MIT License.
        ///
        ///CREATE TABLE [$$SCHEMA$$].[TargetMetadata] (
        ///	[EntityName] [nvarchar](64) NULL,
        ///	[AttributeName] [nvarchar](64) NULL,
        ///	[ReferencedEntity] [nvarchar](64) NULL,
        ///	[ReferencedAttribute] [nvarchar](64) NULL
        ///);
        ///GO
        ///
        ///CREATE TYPE [DataverseToSql].[TargetMetadata_TableType] AS TABLE (
        ///	[EntityName] [nvarchar](64) NULL,
        ///	[AttributeName] [nvarchar](64) NULL,
        ///	[ReferencedEntity] [nvarchar](64) NULL,
        ///	[ReferencedAttribute] [nvarcha [rest of string was truncated]&quot;;.
        /// </summary>
        internal static string Optionsets_TargetMetadata {
            get {
                return ResourceManager.GetString("Optionsets_TargetMetadata", resourceCulture);
            }
        }
    }
}
