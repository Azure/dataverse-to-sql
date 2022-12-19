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

        internal override async Task<TextReader?> GetCustomDatatypeMapReader(CancellationToken cancellationToken)
        {
            var blobUriBuilder = new BlobUriBuilder(Config.ConfigurationStorage.ContainerUri())
            {
                BlobName = CUSTOM_DATATYPE_MAP
            };

            var blobClient = new BlobClient(blobUriBuilder.ToUri(), Credential);

            if (!await blobClient.ExistsAsync(cancellationToken))
                return null;

            log.LogInformation("Loading custom data type map from {path}.", blobUriBuilder.ToUri());

            return new StreamReader(await blobClient.OpenReadAsync(default, cancellationToken));
        }

        internal override async Task<IList<(string name, string script)>> InitCustomScriptsAsync(CancellationToken cancellationToken)
        {
            List<(string name, string script)> customScripts = new();

            var containerClient = new BlobContainerClient(
                Config.ConfigurationStorage.ContainerUri(), 
                Credential);

            await foreach (var item in containerClient.GetBlobsByHierarchyAsync(
                prefix: $"{CUSTOM_SQL_OBJECTS_FOLDER}/",
                cancellationToken: cancellationToken))
            {
                if (item.IsBlob && item.Blob.Properties.ContentLength > 0)
                {
                    var blobClient = containerClient.GetBlobClient(item.Blob.Name);
                    var stream = await blobClient.OpenReadAsync(cancellationToken: cancellationToken);
                    var streamReader = new StreamReader(stream);
                    var script = await streamReader.ReadToEndAsync();
                    customScripts.Add((item.Blob.Name, script));
                }
            }

            return customScripts;
        }
    }
}
