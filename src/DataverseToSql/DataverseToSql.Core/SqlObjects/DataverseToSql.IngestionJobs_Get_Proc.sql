-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT License.

CREATE PROCEDURE [DataverseToSql].[IngestionJobs_Get]
AS

WITH numbered_blobs AS (
	SELECT
		*,
		ROW_NUMBER() OVER (PARTITION BY [EntityName] ORDER BY [BlobName]) AS blob_number
	FROM
		[DataverseToSql].[BlobsToIngest]
	WHERE
		[Complete] = 0
),
jobs AS (
	SELECT
		MAX(b.[Id]) AS [JobId],
		b.[EntityName],
		STRING_AGG(
			REPLACE (
				REPLACE (
					e.[OpenrowsetQuery],
					'<<<BLOB_PLACEHOLDER>>>',
					b.[BlobName]),
				'<<<ORDINAL_PLACEHOLDER>>>',
				blob_number),
			' UNION ALL ')
		AS [OpenrowsetQueries],
		b.[LoadType]
	FROM
		numbered_blobs b
		INNER JOIN [DataverseToSql].[ManagedEntities] e
			ON b.[EntityName] = e.[EntityName]
	GROUP BY
		b.[EntityName],
		b.[Partition],
		b.[LoadType]
)

SELECT
	j.[JobId],
	e.[EntityName],
	e.[TargetSchema],
	e.[TargetTable],
	[DataverseToSql].[ServerlessQuery](
		j.[OpenrowsetQueries],
		e.[InnerQuery],
		j.[LoadType]
	) AS [ServerlessQuery],
	j.[LoadType]
FROM
	jobs j
	INNER JOIN [DataverseToSql].[ManagedEntities] e
		ON j.[EntityName] = e.[EntityName]