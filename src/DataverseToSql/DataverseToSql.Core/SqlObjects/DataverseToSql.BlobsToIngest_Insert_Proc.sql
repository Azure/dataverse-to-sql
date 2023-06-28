-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT License.

CREATE PROCEDURE [DataverseToSql].[BlobsToIngest_Insert]
	@EntityName [DataverseToSql].[EntityType],
	@BlobName [DataverseToSql].[BlobNameType],
	@BasePath [DataverseToSql].[BlobNameType],
	@Timestamp [DataverseToSql].[TimestampType],
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
		[BasePath],
		[Timestamp],
		[Partition],
		[LoadType]
	)
	VALUES (
		@EntityName,
		@BlobName,
		@BasePath,
		@Timestamp,
		@Partition,
		@LoadType
	)
END