-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT License.

CREATE PROCEDURE [DataverseToSql].[ManagedBlobs_Upsert]
	@EntityName [DataverseToSql].[EntityType],
	@BlobName [DataverseToSql].[BlobNameType],
	@FileOffset BIGINT
AS
IF NOT EXISTS (
	SELECT * FROM [DataverseToSql].[ManagedBlobs]
	WHERE
		[EntityName] = @EntityName
		AND [BlobName] = @BlobName
)
BEGIN
	INSERT INTO [DataverseToSql].[ManagedBlobs](
		[EntityName],
		[BlobName],
		[FileOffset]
	)
	VALUES (
		@EntityName,
		@BlobName,
		@FileOffset
	)
END
ELSE
BEGIN
	UPDATE [DataverseToSql].[ManagedBlobs]
	SET
		[FileOffset] = @FileOffset
	WHERE
		[EntityName] = @EntityName
		AND [BlobName] = @BlobName
END