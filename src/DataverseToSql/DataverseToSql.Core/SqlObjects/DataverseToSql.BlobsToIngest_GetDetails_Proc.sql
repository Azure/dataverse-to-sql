-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT License.

CREATE PROCEDURE [DataverseToSql].[BlobsToIngest_GetDetails]
	@EntityName [DataverseToSql].[EntityType],
	@BlobName [DataverseToSql].[BlobNameType]
AS
SELECT
	[TargetSchema],
	[TargetTable],
	[ServerlessQuery],
	[LoadType]
FROM
	[DataverseToSql].[BlobsToIngest]
WHERE
	[EntityName] = @EntityName
	AND [BlobName] = @BlobName
ORDER BY
	[BlobName]