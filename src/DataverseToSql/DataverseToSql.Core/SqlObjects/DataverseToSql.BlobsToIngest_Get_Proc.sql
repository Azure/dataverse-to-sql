-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT License.

CREATE PROCEDURE [DataverseToSql].[BlobsToIngest_Get]
AS
SELECT
	[EntityName],
	[BlobName]
FROM
	[DataverseToSql].[BlobsToIngest]
WHERE
	[Complete] = 0
ORDER BY
	[BlobName]