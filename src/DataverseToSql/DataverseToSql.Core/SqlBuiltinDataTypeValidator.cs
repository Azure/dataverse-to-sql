using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace DataverseToSql.Core
{
    internal class SqlBuiltinDataTypeValidator
    {
        internal static void Validate(string datatype)
        {
            var dt = Parse(datatype);
            var dtName = dt.Name.BaseIdentifier.Value.ToLower();

            switch (dtName)
            {
                case "varbinary":
                case "varchar":
                case "nvarchar":
                    if (dt.Parameters.Count == 1
                        && dt.Parameters[0] is Literal lengthLiteral1
                        && lengthLiteral1.LiteralType == LiteralType.Max)
                        break;
                    goto case "binary";
                case "binary":
                case "char":
                case "nchar":
                    if (dt.Parameters.Count == 1 && dt.Parameters[0] is Literal lengthLiteral2)
                    {
                        switch (lengthLiteral2.LiteralType)
                        {
                            case LiteralType.Integer:
                                var len = int.Parse(lengthLiteral2.Value);
                                if (len < 1 || len > (dtName[0] == 'n' ? 4000 : 8000)) // dtName[0] == 'n' identifies Unicode data types
                                {
                                    throw new Exception($"Invalid length: {datatype}");
                                }
                                break;
                            default:
                                throw new Exception($"Invalid data type parameter: {datatype}");
                        }
                    }
                    else if (dt.Parameters.Count != 0)
                        throw new Exception($"Invalid data type: {datatype}");
                    break;
                case "datetime2":
                case "datetimeoffset":
                case "time":
                    if (dt.Parameters.Count == 1
                        && dt.Parameters[0] is Literal timeScaleLiteral
                        && timeScaleLiteral.LiteralType == LiteralType.Integer)
                    {
                        var timeScale = int.Parse(timeScaleLiteral.Value);
                        if (timeScale < 0 || timeScale > 7)
                            throw new Exception($"Invalid scale: {datatype}");
                    }
                    else if (dt.Parameters.Count != 0)
                        throw new Exception($"Unexpected data type parameters: {datatype}");
                    break;
                case "dec":
                case "decimal":
                case "numeric":
                    if (dt.Parameters.Count >= 1
                        && dt.Parameters.Count <= 2
                        && dt.Parameters[0] is Literal precisionLiteral1
                        && precisionLiteral1.LiteralType == LiteralType.Integer)
                    {
                        var precision = int.Parse(precisionLiteral1.Value);
                        if (precision < 1 || precision > 38)
                            throw new Exception($"Invalid precision: {datatype}");

                        if (dt.Parameters.Count == 2)
                        {
                            if (dt.Parameters[1] is Literal scaleLiteral
                                && scaleLiteral.LiteralType == LiteralType.Integer)
                            {
                                var scale = int.Parse(scaleLiteral.Value);

                                if (scale < 0)
                                    throw new Exception($"Invalid scale: {datatype}");

                                if (scale > precision)
                                    throw new Exception($"Scale cannot be larger than precision: {datatype}");
                            }
                            else
                                throw new Exception($"Invalid scale: {datatype}");
                        }
                    }
                    else if (dt.Parameters.Count != 0)
                        throw new Exception($"Unexpected data type parameters: {datatype}");
                    break;
                case "float":
                    if (dt.Parameters.Count == 1
                        && dt.Parameters[0] is Literal precisionLiteral2
                        && precisionLiteral2.LiteralType == LiteralType.Integer)
                    {
                        var precision = int.Parse(precisionLiteral2.Value);
                        if (precision < 1 || precision > 53)
                            throw new Exception($"Invalid precision: {datatype}");
                    }
                    else if (dt.Parameters.Count != 0)
                        throw new Exception($"Unexpected data type parameters: {datatype}");
                    break;
                case "int":
                case "bigint":
                case "bit":
                case "date":
                case "datetime":
                case "image":
                case "money":
                case "ntext":
                case "real":
                case "smalldatetime":
                case "smallint":
                case "smallmoney":
                case "sql_variant":
                case "sysname":
                case "text":
                case "timestamp":
                case "tinyint":
                case "uniqueidentifier":
                    if (dt.Parameters.Count != 0)
                        throw new Exception($"Unexpected data type parameters: {datatype}");
                    break;
                default:
                    throw new Exception($"Unexpected data type: {datatype}");
            }
        }

        private static ParameterizedDataTypeReference Parse(string datatype)
        {
            var parser = new TSql160Parser(false);
            var stringReader = new StringReader(datatype);

            var dt = parser.ParseScalarDataType(stringReader, out var errors);

            if (errors.Count > 0 || dt is null)
                throw new AggregateException(
                    $"Error parsing data type: {datatype}",
                    errors.Select(e => new Exception(e.Message)));

            if (dt is SqlDataTypeReference sqlDataTypeReference)
                return sqlDataTypeReference;
            else if (dt is UserDataTypeReference udt
                && udt.Name.Identifiers.Count == 1
                && udt.Name.BaseIdentifier.Value.ToLower() == "sysname")
                return udt;
            else if (dt is XmlDataTypeReference)
                throw new Exception($"Data type is XML: {datatype}");
            else
                throw new Exception($"Data type is not built-in: {datatype}");
        }
    }
}
