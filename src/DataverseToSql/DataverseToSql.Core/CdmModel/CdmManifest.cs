// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections;

namespace DataverseToSql.Core.CdmModel
{
    /// <summary>
    /// Represents a CDM manifest as deserialized from a manifest JSON file.
    /// The class is limited to the fields needed for the project.
    /// </summary>
    internal class CdmManifest
    {
        internal readonly IList<CdmEntity> Entities;

        internal CdmManifest(dynamic model)
        {
            try
            {
                if (!(model.name == "cdm" && model.version == "1.0"))
                {
                    throw new Exception("Unsupported model format.");
                }

                Entities = ((IEnumerable)model.entities).Cast<dynamic>().Select(e => new CdmEntity(e)).ToList();
            }
            catch (Exception ex)
            {
                throw new Exception("Failed to load model.", ex);
            }
        }
    }
}
