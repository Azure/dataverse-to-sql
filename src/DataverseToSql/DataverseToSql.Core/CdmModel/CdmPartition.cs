// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DataverseToSql.Core.CdmModel
{
    /// <summary>
    /// Represents a CDM entity partition as deserialized from a manifest.
    /// The class is limited to the fields needed for the project.
    /// </summary>
    internal class CdmPartition
    {
        internal readonly string Name;
        internal readonly string Location;

        internal CdmPartition(dynamic partition)
        {
            Name = partition.name;
            Location = partition.location;
        }
    }
}
