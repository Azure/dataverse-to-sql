-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT License.

CREATE PROCEDURE [DataverseToSql].[ManagedEntities_Upsert]
	@EntityName [DataverseToSql].[EntityType],
	@State INT = NULL,
	@SchemaHash NVARCHAR(128) = NULL
AS
IF NOT EXISTS (
	SELECT * FROM [DataverseToSql].[ManagedEntities]
	WHERE [EntityName] = @EntityName
)
BEGIN
	INSERT INTO [DataverseToSql].[ManagedEntities](
		[EntityName],
		[State],
		[SchemaHash]
	)
	VALUES (
		@EntityName,
		@State,
		@SchemaHash
	)
END
ELSE
BEGIN
	UPDATE [DataverseToSql].[ManagedEntities]
	SET
		[State] = ISNULL(@State, [State]),
		[SchemaHash] = ISNULL(@SchemaHash, [SchemaHash])
	WHERE
		[EntityName] = @EntityName
END