CREATE PROCEDURE [DataverseToSql].[BlobsToIngest_Complete]
	@EntityName [DataverseToSql].[EntityType],
	@BlobName [DataverseToSql].[BlobNameType]
AS

-- Set complete to 1 if the blob load type is "full"
UPDATE	[DataverseToSql].[BlobsToIngest]
SET		[Complete] = 1
WHERE
		[EntityName] = @EntityName
		AND [BlobName] = @BlobName
		AND [LoadType] = 0 -- Full

-- Delete the blob record if the load is "incremental"
DELETE	[DataverseToSql].[BlobsToIngest]
WHERE
		[EntityName] = @EntityName
		AND [BlobName] = @BlobName
		AND [LoadType] = 1 -- Incremental

