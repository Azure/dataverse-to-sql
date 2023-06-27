-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT License.

CREATE PROCEDURE [DataverseToSql].[BlobsToIngest_Insert]
	@EntityName [DataverseToSql].[EntityType],
	@BlobName [DataverseToSql].[BlobNameType],
	@Partition [DataverseToSql].[BlobPartitionType],
	@LoadType INT
AS
IF NOT EXISTS (
	SELECT * FROM [DataverseToSql].[BlobsToIngest]
	WHERE
		[EntityName] = @EntityName
		AND [BlobName] = @BlobName
)
BEGIN
	INSERT INTO [DataverseToSql].[BlobsToIngest](
		[EntityName],
		[BlobName],
		[Partition],
		[LoadType]
	)
	VALUES (
		@EntityName,
		@BlobName,
		@Partition,
		@LoadType
	)
END