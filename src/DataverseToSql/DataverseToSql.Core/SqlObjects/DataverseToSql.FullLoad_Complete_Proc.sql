-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT License.

CREATE PROCEDURE [DataverseToSql].[FullLoad_Complete]
AS
BEGIN TRAN

DECLARE @CompletedEntities TABLE(
	[EntityName] [DataverseToSql].[EntityType]
)

UPDATE [DataverseToSql].[ManagedEntities]
SET [State] = 3 -- Ready
OUTPUT inserted.[EntityName] 
	INTO @CompletedEntities
WHERE [EntityName] IN (
	SELECT
		[EntityName]
	FROM
		[DataverseToSql].[BlobsToIngest]
	GROUP BY
		[EntityName]
	HAVING
		MIN([Complete]) = 1
		AND MAX([Complete]) = 1
)

DELETE f
FROM [DataverseToSql].[BlobsToIngest] f
	INNER JOIN @CompletedEntities e
		ON f.[EntityName] = e.[EntityName]

COMMIT