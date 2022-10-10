// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Core;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Logging;

namespace DataverseToSql.Core
{
    /// <summary>
    /// Derived class of EnvironmentBase that works with configuration from an Azure Storage Account.
    /// </summary>
    public class CloudEnvironment : EnvironmentBase
    {
        public CloudEnvironment(
            ILogger log,
            TokenCredential credential,
            BlobContainerClient blobContainerClient)
            : base(log, credential, ReadConfig(log, blobContainerClient))
        { }

        private static EnvironmentConfiguration ReadConfig(
            ILogger log,
            BlobContainerClient blobContainerClient)
        {
            log.LogInformation(
                "Loading environment from {containerUri}",
                blobContainerClient.Uri
                );

            try
            {
                AzureStorageFileProvider.AzureStorageFileProvider afp = new(
                    blobContainerClient,
                    "");

                return new(CONFIG_FILE, afp);
            }
            catch (Exception ex)
            {
                throw new Exception(
                    $"Cannot load configuration: {ex.Message}", ex);
            }
        }
    }
}
