-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT License.

CREATE PROCEDURE [DataverseToSql].[IngestionJobs_Get]
AS

WITH 
jobs AS (
	SELECT
		MAX([Id]) AS [JobId],
		[EntityName],
		[BasePath],
		MIN([Timestamp]) AS [TimestampFrom],
		MAX([Timestamp]) AS [TimestampTo],
		[Partition],
		[LoadType]
	FROM
		[DataverseToSql].[BlobsToIngest]
	WHERE
		[Complete] = 0
	GROUP BY
		[EntityName],
		[BasePath],
		[Partition],
		[LoadType]
)
SELECT
	j.[JobId],
	e.[EntityName],
	e.[TargetSchema],
	e.[TargetTable],
		-- OPENROWSET query
		REPLACE(REPLACE(REPLACE(
			CASE j.[LoadType]
				-- Full load
				WHEN 0 THEN e.[FullLoadInnerQuery]
				-- Incremental load
				WHEN 1 THEN e.[IncrementalInnerQuery]
			END,
			'<<<TIMESTAMP_FROM_PLACEHOLDER>>>',
			j.[TimestampFrom]),
			'<<<TIMESTAMP_TO_PLACEHOLDER>>>',
			j.[TimestampTo]),
			'<<<BLOB_PLACEHOLDER>>>',
			TRIM('/' FROM j.[BasePath]) + '/*/' + CASE j.[LoadType]
				-- Full load
				WHEN 0 THEN j.[Partition] + '*.csv'
				-- Incremental load
				WHEN 1 THEN '*.csv'
			END)
	AS [ServerlessQuery],
	j.[LoadType]
FROM
	jobs j
	INNER JOIN [DataverseToSql].[ManagedEntities] e
		ON j.[EntityName] = e.[EntityName]