// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections;

namespace DataverseToSql.Core.CdmModel
{
    /// <summary>
    /// Represents a CDM entity as deserialized from a manifest.
    /// The class is limited to the fields needed for the project.
    /// </summary>
    internal class CdmEntity
    {
        internal readonly string Name;
        internal readonly IList<CdmAttribute> Attributes;
        internal readonly IList<CdmPartition> Partitions;
        internal readonly string PartitionGranularity;
        internal readonly string InitialSyncState;

        private IList<CdmAttribute>? _primaryKeyAttributes = null;

        internal CdmEntity(dynamic entity)
        {
            Name = entity.name;
            Attributes = ((IEnumerable)entity.attributes).Cast<dynamic>().Select(a => new CdmAttribute(a)).ToList();
            Partitions = ((IEnumerable)entity.partitions).Cast<dynamic>().Select(a => new CdmPartition(a)).ToList();

            PartitionGranularity = ((IEnumerable)entity.annotations)
                .Cast<dynamic>()
                .Where(a => a.name == "Athena:PartitionGranularity")
                .Select(a => a.value).FirstOrDefault("Unknown");

            InitialSyncState = ((IEnumerable)entity.annotations)
                .Cast<dynamic>()
                .Where(a => a.name == "Athena:InitialSyncState")
                .Select(a => a.value).FirstOrDefault("Unknown");
        }

        // Return the set of fields composing the primary key of the entity.
        // At the moment it returns Id column only.
        internal IList<CdmAttribute> PrimaryKeyAttributes
        {
            get
            {
                if (_primaryKeyAttributes is null)
                {
                    CdmAttribute? idAttr = null;

                    foreach (var attr in Attributes)
                    {
                        if (attr.Name.ToLower() == "id")
                        {
                            idAttr = attr;
                        }
                    }

                    if (idAttr is not null)
                    {
                        _primaryKeyAttributes = new List<CdmAttribute>() { idAttr };
                    }
                    else
                    {
                        throw new Exception($"Entity {Name} contains no primary key or Id attribute.");
                    }
                }

                return _primaryKeyAttributes;
            }
        }

        // Return set of partitions grouped by name.
        // Partitions belonging to the same period are grouped together.
        // E.g. "2018" and "2018_001" are grouped together under 2018.
        internal IList<CdmPartitionGroup> PartitionGroups
        {
            get
            {
                return Partitions
                    .GroupBy(p => p.Name.Split('_')[0])
                    .Select(g => new CdmPartitionGroup(
                        name: g.Key,
                        partitions: g.ToList()))
                    .ToList();
            }
        }

        // Determines if the entity has a primary key.
        internal bool HasPrimaryKey { get => PrimaryKeyAttributes.Count > 0; }
    }
}
