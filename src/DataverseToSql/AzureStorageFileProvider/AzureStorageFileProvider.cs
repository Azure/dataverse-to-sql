using Azure.Core;
using Azure.Storage.Blobs;
using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Primitives;
using System.Text;

namespace AzureStorageFileProvider
{
    public class AzureStorageFileProvider : IFileProvider
    {
        private readonly string RootPath;

        internal readonly Uri RootUri;

        internal BlobContainerClient BlobContainerClient;

        public AzureStorageFileProvider(
            BlobContainerClient blobContainerClient,
            string rootPath)
        {
            this.BlobContainerClient = blobContainerClient;

            RootPath = rootPath;
            RootUri = new Uri(
                rootPath,
                UriKind.Relative
                );
        }

        public IDirectoryContents GetDirectoryContents(string subpath)
        {
            return new AzureStorageDirectoryContents(
                this,
                delimiter: "/",
                prefix: JoinUri(RootPath, subpath)
                );
        }

        public IFileInfo GetFileInfo(string subpath)
        {
            var prefixToSearch = JoinUri(RootPath, subpath);

            foreach (var item in BlobContainerClient.GetBlobsByHierarchy(
                delimiter: "/",
                prefix: prefixToSearch
                ))
            {
                if (item.IsPrefix && item.Prefix == prefixToSearch)
                {
                    return new AzureStorageFileInfo(
                        this,
                        item
                        );
                }
                else if (item.IsBlob && item.Blob.Name == prefixToSearch)
                {
                    return new AzureStorageFileInfo(
                        this,
                        item
                        );
                }
            }

            return new AzureStorageFileInfo(
                this,
                null
                );
        }

        public IChangeToken Watch(string filter)
        {
            throw new NotImplementedException();
        }

        internal string JoinUri(string path1, string path2)
        {
            var sb = new StringBuilder();
            path1 = path1.Trim().TrimEnd('/');
            if (path1 != "")
            {
                sb.Append(path1.Trim().TrimEnd('/'));
                sb.Append("/");
            }
            sb.Append(path2.TrimStart('.').TrimStart('/'));
            return sb.ToString();
        }
    }
}