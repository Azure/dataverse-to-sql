-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT License.

CREATE PROCEDURE [DataverseToSql].[IngestionJobs_Complete]
	@JobId [DataverseToSql].[JobIdType]
AS

DECLARE @CompletedBlobs TABLE (
	[EntityName] [DataverseToSql].[EntityType],
	[LoadType] INT
)

BEGIN TRAN

UPDATE	a
SET
	[Complete] = 1
OUTPUT inserted.[EntityName], inserted.[LoadType]
	INTO @CompletedBlobs
FROM
	[DataverseToSql].[BlobsToIngest] a
	INNER JOIN [DataverseToSql].[BlobsToIngest] b
		ON a.[Id] <= b.Id
		AND a.[EntityName] = b.[EntityName]
		AND a.[LoadType] = b.[LoadType]
		and a.[Partition] = b.[Partition]
WHERE
	b.[Id] = @JobId

DECLARE @Entity [DataverseToSql].[EntityType]
DECLARE @LoadType INT
SELECT TOP 1 @Entity=[EntityName], @LoadType=[LoadType] FROM @CompletedBlobs

-- For full loads, set entity state to Ready if the initial
-- load of all blobs of the entity is complete
IF	@LoadType = 0 -- Full load
	AND NOT EXISTS (
		SELECT *
		FROM [DataverseToSql].[BlobsToIngest]
		WHERE
			[EntityName] = @Entity
			AND [LoadType] = 0 -- Full load
			AND [Complete] = 0)
BEGIN
	UPDATE [DataverseToSql].[ManagedEntities] 
	SET [State] = 3 -- Ready
	WHERE
		[State] = 2 -- IngestionInProgress
		AND [EntityName] = @Entity
END

COMMIT