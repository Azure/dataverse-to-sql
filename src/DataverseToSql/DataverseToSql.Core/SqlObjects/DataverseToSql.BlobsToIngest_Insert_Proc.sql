CREATE PROCEDURE [DataverseToSql].[BlobsToIngest_Insert]
	@EntityName [DataverseToSql].[EntityType],
	@BlobName [DataverseToSql].[BlobNameType],
	@TargetSchema SYSNAME,
	@TargetTable SYSNAME,
	@ServerlessQuery NVARCHAR(MAX),
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
		[TargetSchema],
		[TargetTable],
		[ServerlessQuery],
		[LoadType]
	)
	VALUES (
		@EntityName,
		@BlobName,
		@TargetSchema,
		@TargetTable,
		@ServerlessQuery,
		@LoadType
	)
END