// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections;

namespace DataverseToSql.Core.CdmModel
{
    /// <summary>
    /// Represents a CDM entity attribute as deserialized from a manifest.
    /// The class is limited to the fields needed for the project.
    /// </summary>
    internal class CdmAttribute
    {
        internal readonly string Name;
        internal readonly string DataType;
        internal readonly int MaxLength;
        internal readonly int Precision = -1;
        internal readonly int Scale = -1;
        internal string? CustomSqlDatatype = null;

        internal CdmAttribute(dynamic attribute)
        {
            Name = attribute.name;
            DataType = attribute.dataType;
            MaxLength = attribute.maxLength;

            if (DataType == "decimal")
            {
                var arguments = ((IEnumerable)attribute["cdm:traits"])
                    .Cast<dynamic>()
                    .Where(a => a.traitReference == "is.dataFormat.numeric.shaped")
                    .First().arguments;

                var argdict = ((IEnumerable)arguments)
                    .Cast<dynamic>()
                    .ToDictionary(a => (string)a.name, a => (int)a.value);

                Precision = argdict["precision"];
                Scale = argdict["scale"];
            }
        }
    }
}
