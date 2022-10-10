// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.Extensions.FileProviders;
using System.Collections;

namespace AzureStorageFileProvider
{
    public class AzureStorageDirectoryContents : IDirectoryContents
    {
        private readonly AzureStorageFileProvider fileProvider;
        private readonly string Delimiter;
        private readonly string Prefix;

        internal AzureStorageDirectoryContents(
            AzureStorageFileProvider fileProvider,
            string delimiter,
            string prefix)
        {
            this.fileProvider = fileProvider;
            Delimiter = delimiter;
            Prefix = prefix.TrimEnd('/') + "/";

            Exists = false;
            var prefixToSearch = Prefix.TrimEnd('/');
            foreach (var item in fileProvider.BlobContainerClient.GetBlobsByHierarchy(
                delimiter: Delimiter,
                prefix: prefixToSearch
                ))
            {
                if (item.IsPrefix && item.Prefix == prefixToSearch)
                {
                    Exists = true;
                }
            }
        }

        public bool Exists
        {
            get;
            internal set;
        }

        public IEnumerator<IFileInfo> GetEnumerator()
        {
            if (Exists)
            {
                foreach (var item in fileProvider.BlobContainerClient.GetBlobsByHierarchy(
                    delimiter: Delimiter,
                    prefix: Prefix
                    ))
                {
                    yield return new AzureStorageFileInfo(fileProvider, item);
                }
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            if (Exists)
            {
                foreach (var item in fileProvider.BlobContainerClient.GetBlobsByHierarchy(
                    delimiter: Delimiter,
                    prefix: Prefix
                    ))
                {
                    yield return new AzureStorageFileInfo(fileProvider, item);
                }
            }
        }
    }
}
