// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DataverseToSql.Core.Model
{
    /// <summary>
    /// Represents a blob that is beign tracked for incremental ingestion.
    /// The main goal is to keep track of the offset the incremental ingestion has reached so far.
    /// Data are stored in the ManagedBlob table.
    /// </summary>
    internal class ManagedBlob
    {
        internal ManagedBlob(
            ManagedEntity entity,
            string name,
            long offset
            )
        {
            Entity = entity;
            Name = name;
            Offset = offset;
        }

        // Reference to the managed entity the blob belongs to
        public ManagedEntity Entity { get; }
        // Name of the blob, as a file basename (e.g. 2022.csv)
        public string Name { get; }
        // Offset inside the source blob that ingestion has reached so far
        public long Offset { get; set; }
    }
}
