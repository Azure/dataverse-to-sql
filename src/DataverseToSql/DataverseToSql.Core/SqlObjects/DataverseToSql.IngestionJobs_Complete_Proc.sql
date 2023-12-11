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

DECLARE @Entity [DataverseToSql].[EntityType]
DECLARE @LoadType INT

SELECT
	@Entity=[EntityName],
	@LoadType=[LoadType]
FROM
	[DataverseToSql].[BlobsToIngest] b
WHERE
	b.[Id] = @JobId

IF	@LoadType = 1 -- Incremental load
BEGIN
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
END
ELSE -- Full Load
BEGIN
	-- Check if the entity is not partitioned
	-- The condition is that there is a partition named "1"
	IF EXISTS (
		SELECT *
		FROM [DataverseToSql].[BlobsToIngest]
		WHERE
			[Partition] = '1'
			AND [EntityName] = @Entity
	)
	BEGIN -- Entity is not partitioned
		UPDATE a
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
		WHERE
			b.[Id] = @JobId		
	END
	ELSE -- Entity is partitioned by Year
	BEGIN
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
	END

	-- For full loads, set entity state to Ready if the initial
	-- load of all blobs of the entity is complete
	IF NOT EXISTS (
			SELECT *
			FROM [DataverseToSql].[BlobsToIngest]
			WHERE
				[EntityName] = @Entity
				AND [LoadType] IN (0, 2) -- Full load
				AND [Complete] = 0)
	BEGIN
		UPDATE [DataverseToSql].[ManagedEntities] 
		SET [State] = 3 -- Ready
		WHERE
			[State] = 2 -- IngestionInProgress
			AND [EntityName] = @Entity
	END
END

COMMIT