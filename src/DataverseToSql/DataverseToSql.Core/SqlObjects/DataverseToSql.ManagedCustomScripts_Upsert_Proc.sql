-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT License.

CREATE PROCEDURE [DataverseToSql].[ManagedCustomScripts_Upsert]
	@ScriptName [DataverseToSql].[CustomScriptNameType],
	@Hash  NVARCHAR(128)
AS
IF NOT EXISTS (
	SELECT * FROM [DataverseToSql].[ManagedCustomScripts]
	WHERE
		[ScriptName] = @ScriptName
)
BEGIN
	INSERT INTO [DataverseToSql].[ManagedCustomScripts](
		[ScriptName],
		[Hash]
	)
	VALUES (
		@ScriptName,
		@Hash
	)
END
ELSE
BEGIN
	UPDATE [DataverseToSql].[ManagedCustomScripts]
	SET
		[Hash] = @Hash
	WHERE
		[ScriptName] = @ScriptName
END