-- Copyright (c) Microsoft Corporation.
-- Licensed under the MIT License.

CREATE FUNCTION [DataverseToSql].[ServerlessQuery]
(
	@OpenrowsetQueries nvarchar(max),
	@InnerQuery nvarchar(max),
	@LoadType int
)
RETURNS nvarchar(max)
BEGIN
	SET @InnerQuery = REPLACE(
		@InnerQuery,
		'<<<OPENROWSET_PLACEHOLDER>>>',
		@OpenrowsetQueries)

	IF @LoadType = 0 -- Full load
	BEGIN
		SET @InnerQuery = @InnerQuery + ' WHERE ISNULL(IsDelete, ''False'') <> ''True'''
	END

	DECLARE @FormattedInnerQuery nvarchar(max) =
		REPLACE(
			REPLACE (@InnerQuery, '''', ''''''),
			'<<<TOP_PLACEHOLDER>>>',
			''' + CAST(@rowcount AS nvarchar(max)) + N''')

	DECLARE @OuterQuery nvarchar(max) = '
        DECLARE @rowcount bigint

        SELECT  @rowcount=count(*)
        FROM    (' + @OpenrowsetQueries + ') a

        DECLARE @query nvarchar(max) = N''' + @FormattedInnerQuery + '''

        EXEC sp_executesql @query'

    RETURN @OuterQuery
END