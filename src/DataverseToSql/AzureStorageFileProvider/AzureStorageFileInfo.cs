using Azure.Storage.Blobs.Models;
using Microsoft.Extensions.FileProviders;

namespace AzureStorageFileProvider
{
    public class AzureStorageFileInfo : IFileInfo
    {
        private readonly AzureStorageFileProvider fileProvider;
        private readonly BlobHierarchyItem? Item;

        internal AzureStorageFileInfo(
            AzureStorageFileProvider fileProvider,
            BlobHierarchyItem? item)
        {
            this.fileProvider = fileProvider;
            Item = item;
        }

        public bool Exists
        {
            get
            {
                return Item is not null;
            }
        }

        public long Length
        {
            get
            {
                if (Item?.IsBlob ?? false)
                {
                    return Item?.Blob?.Properties?.ContentLength ?? 0;
                }
                else
                {
                    return 0;
                }
            }
        }

        public string? PhysicalPath => null;

        public string Name
        {
            get
            {
                if (PhysicalPath is not null)
                    return PhysicalPath.Split("/").Last();
                else
                    throw new NullReferenceException();
            }
        }

        public DateTimeOffset LastModified
        {
            get
            {
                return Item?.Blob.Properties.LastAccessedOn ?? default;
            }
        }

        public bool IsDirectory
        {
            get
            {
                return Item?.IsPrefix ?? false;
            }
        }

        public Stream CreateReadStream()
        {
            var blobClient = fileProvider.BlobContainerClient.GetBlobClient(Item.Blob.Name);
            return blobClient.OpenRead();
        }
    }
}
