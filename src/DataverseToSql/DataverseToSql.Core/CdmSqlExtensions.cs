// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using DataverseToSql.Core.CdmModel;
using System.Text;

namespace DataverseToSql.Core
{
    /// <summary>
    /// Provides extension methods to CDM objects for generating SQL code.
    /// </summary>
    internal static class CdmSqlExtensions
    {
        private const string SQL_CODE_INDENT = "    ";
        private const string DV_SCHEMA = "DataverseToSql";

        // Return the scripts of the SQL objects representing the CDM entity.
        // The scripts include:
        // - The CREATE TABLE statement of the target table.
        // - The CREATE TYPE statement for the creation of the table type required
        //   by the merge stored procedure.
        // - The CREATE PROCEDURE statement of the merge stored procedure invoked by
        //   the Copy activity.
        internal static string SqlScripts(this CdmEntity cdmEntity, string schema)
        {
            var sb = new StringBuilder();
            sb.AppendLine(cdmEntity.TableScript(schema));
            sb.AppendLine(cdmEntity.TableTypeScript());
            sb.AppendLine(cdmEntity.MergeProcedureScript(schema));
            return sb.ToString();
        }

        // Return the CREATE TABLE statement of the target table.
        internal static string TableScript(this CdmEntity entity, string schema)
        {
            var sb = new StringBuilder();

            sb.AppendLine(
                $"CREATE TABLE [{schema}].[{entity.Name}] (");

            sb.Append(SQL_CODE_INDENT);
            sb.AppendJoin(
                $",\n{SQL_CODE_INDENT}",
                entity.Attributes.Select(attr => attr.SqlColumnDef())
                ); ;


            if (entity.HasPrimaryKey)
            {
                var primaryKeyCols = string.Join(
                    ",",
                    entity.PrimaryKeyAttributes.Select(attr => attr.SqlColumnName()));

                sb.Append(
                    $",\n{SQL_CODE_INDENT}CONSTRAINT [PK_{entity.Name}] PRIMARY KEY({primaryKeyCols})");
            }

            sb.AppendLine();
            sb.AppendLine(");");
            sb.AppendLine("GO");

            return sb.ToString();
        }

        // Return he CREATE TYPE statement for the creation of the table type required
        // by the merge stored procedure.
        internal static string TableTypeScript(this CdmEntity entity)
        {
            var sb = new StringBuilder();

            sb.AppendLine($"CREATE TYPE [{DV_SCHEMA}].{entity.TableTypeName()} AS TABLE (");

            sb.AppendJoin($",\n", entity.Attributes
                .Select(attr => $"{attr.SqlColumnDef()} NULL"));

            sb.AppendLine(");");
            sb.AppendLine("GO");

            return sb.ToString();
        }

        // Return the CREATE PROCEDURE statement of the merge stored procedure invoked by
        // the Copy activity.
        internal static string MergeProcedureScript(this CdmEntity entity, string schema)
        {
            var sb = new StringBuilder();

            var primaryKeyCols = entity.PrimaryKeyAttributes.Select(a => a.SqlColumnName()).ToList();

            var updateExpressions = new List<string>();
            var insertTargetColumns = new List<string>();
            var insertSourceColumns = new List<string>();

            foreach (var attr in entity.Attributes)
            {
                if (!entity.PrimaryKeyAttributes.Contains(attr))
                {
                    updateExpressions.Add($"{attr.SqlColumnName()} = source.{attr.SqlColumnName()}");
                }
                insertTargetColumns.Add(attr.SqlColumnName());
                insertSourceColumns.Add($"source.{attr.SqlColumnName()}");
            }

            sb.AppendLine($@"
                CREATE PROCEDURE [{DV_SCHEMA}].{entity.MergeProcName()}
                    @entity [{DV_SCHEMA}].{entity.TableTypeName()} READONLY
                AS
                MERGE [{schema}].[{entity.Name}] AS target
                USING @entity AS source
                ON {string.Join(" AND ", primaryKeyCols.Select(c => $"source.{c} = target.{c}"))}
                WHEN MATCHED AND source.IsDelete = 1 THEN
	                DELETE
                WHEN MATCHED AND ISNULL(source.IsDelete, 0) = 0 AND source.[SinkModifiedOn] > target.[SinkModifiedOn] THEN
                    UPDATE SET {string.Join(",", updateExpressions)}
                WHEN NOT MATCHED THEN
                    INSERT ({string.Join(",", insertTargetColumns)})
                    VALUES ({string.Join(",", insertSourceColumns)});
                GO");

            return sb.ToString();
        }

        // Return the Serverless SQL Pool query to read the specified blob during 
        // full load (used in initial load of new entities)
        internal static string GetFullLoadServerlessQuery(
            this CdmEntity entity, 
            Uri blobUri, 
            IList<(string name, string datatype)> targetColumns) =>
            entity.IncrementalLoadServerlessQuery(blobUri, targetColumns) + "WHERE ISNULL(IsDelete, 'False') <> 'True'";

        // Return the Serverless SQL Pool query to read the specified blob during 
        // incremental load
        internal static string IncrementalLoadServerlessQuery(
            this CdmEntity entity, 
            Uri blobUri, 
            IList<(string name, string datatype)> targetColumns)
        {
            var sourceColumns = string.Join(",", entity.Attributes.Select(attr => attr.SqlColumnDef()));
            var primaryKeyCols = entity.PrimaryKeyAttributes.Select(a => a.SqlColumnName()).ToList();
            var primaryKeyString = string.Join(",", primaryKeyCols);
            var primaryKeyJoinPredicates = string.Join(" AND ", primaryKeyCols.Select(c => $"s.{c} = r.{c}"));
            var targetColumnList = string.Join(",", targetColumns.Select(c => $"[{c.name}]"));

            return $@"
                WITH cte_source AS (
                    SELECT  *
                    FROM    OPENROWSET(BULK ( N'{blobUri}' ),
                            FORMAT = 'csv', FIELDTERMINATOR  = ',', FIELDQUOTE = '""')
                            WITH ({sourceColumns}) AS T1
                ),
                cte_rownumber AS (
                    SELECT  ROW_NUMBER() OVER (PARTITION BY {primaryKeyString} ORDER BY [SinkModifiedOn] DESC, [versionnumber] DESC) [DvRowNumber],
                            {primaryKeyString},
                            [SinkModifiedOn],
                            [versionnumber]
                    FROM    cte_source
                ),
                cte_most_recent_records AS (
                    SELECT  s.*
                    FROM    cte_source s
                            INNER JOIN cte_rownumber r
                                ON {primaryKeyJoinPredicates}
                                AND s.[SinkModifiedOn] = r.[SinkModifiedOn]
                                AND s.[versionnumber] = r.[versionnumber]
                    WHERE   r.DvRowNumber = 1
                )
                SELECT
                    {targetColumnList}
                FROM
                    cte_most_recent_records
                ";
        }

        // Return the SQL column definition of the CDM attribute
        // in the format [<column name>] [<data type>]
        internal static string SqlColumnDef(this CdmAttribute attr) => $"{attr.SqlColumnName()} {attr.SqlDataType()}";

        // Return the formated SQL column name of the CDM attribute
        // in the format [<column name>]
        internal static string SqlColumnName(this CdmAttribute attr) => $"[{attr.Name}]";

        // Return the SQL data type of the CDM attribute
        internal static string SqlDataType(this CdmAttribute attr)
        {
            return attr.DataType.ToLower() switch
            {
                "binary" => "varbinary(max)",
                "boolean" => "bit",
                "byte" => "tinyint",
                "char" when attr.MaxLength == -1 => "nchar(100)",
                "char" when attr.MaxLength <= 4000 => $"nchar({attr.MaxLength})",
                "char" => "nvarchar(max)",
                "date" => "date",
                "datetime" => "datetime2",
                "datetimeoffset" => "datetimeoffset",
                "decimal" => $"decimal({attr.Precision},{attr.Scale})",
                "double" => "float",
                "float" => "float",
                "guid" => "uniqueidentifier",
                "int16" => "smallint",
                "int32" => "int",
                "int64" => "bigint",
                "integer" => "int",
                "json" => "nvarchar(max)",
                "long" => "bigint",
                "short" => "smallint",
                "string" when attr.MaxLength == -1 || attr.MaxLength > 4000 => "nvarchar(max)",
                "string" => $"nvarchar({attr.MaxLength})",
                "time" => "time",
                "timestamp" => "datetime2",
                _ => "nvarchar(max)"
            };
        }
    }
}
