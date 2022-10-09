﻿CREATE PROCEDURE [DataverseToSql].[BlobsToIngest_Get]
AS
SELECT
	[EntityName],
	[BlobName],
	[TargetSchema],
	[TargetTable],
	[ServerlessQuery],
	[LoadType]
FROM
	[DataverseToSql].[BlobsToIngest]
WHERE
	[Complete] = 0
ORDER BY
	[BlobName]