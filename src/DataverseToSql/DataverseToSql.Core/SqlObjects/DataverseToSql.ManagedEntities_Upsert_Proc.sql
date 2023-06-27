-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT License.

CREATE PROCEDURE [DataverseToSql].[ManagedEntities_Upsert]
	@EntityName [DataverseToSql].[EntityType],
	@TargetSchema SYSNAME,
	@TargetTable SYSNAME,
	@State INT = NULL,
	@SchemaHash NVARCHAR(128) = NULL,
	@InnerQuery NVARCHAR(MAX) = NULL,
	@OpenrowsetQuery NVARCHAR(MAX) = NULL
AS
IF NOT EXISTS (
	SELECT * FROM [DataverseToSql].[ManagedEntities]
	WHERE [EntityName] = @EntityName
)
BEGIN
	INSERT INTO [DataverseToSql].[ManagedEntities](
		[EntityName],
		[State],
		[SchemaHash],
		[TargetSchema],
		[TargetTable],
		[InnerQuery],
		[OpenrowsetQuery]
	)
	VALUES (
		@EntityName,
		@State,
		@SchemaHash,
		@TargetSchema,
		@TargetTable,
		@InnerQuery,
		@OpenrowsetQuery
	)
END
ELSE
BEGIN
	UPDATE [DataverseToSql].[ManagedEntities]
	SET
		[State] = ISNULL(@State, [State]),
		[SchemaHash] = ISNULL(@SchemaHash, [SchemaHash]),
		[InnerQuery] = ISNULL(@InnerQuery, [InnerQuery]),
		[OpenrowsetQuery] = ISNULL(@OpenrowsetQuery, [OpenrowsetQuery])
	WHERE
		[EntityName] = @EntityName
END