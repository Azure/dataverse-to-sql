// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;

namespace DataverseToSql.Core
{
    /// <summary>
    /// Provides a lock based on Azure Blob Storage.
    /// The lock is acquired and held via a timer.
    /// </summary>
    internal class BlobLock
    {
        private BlobLeaseClient blobLeaseClient;
        private TimeSpan leaseDuration;
        private System.Timers.Timer timer;

        internal BlobLock(
            BlobClient blobClient,
            TimeSpan? leaseDuration = null
            )
        {
            if (!blobClient.Exists())
            {
                blobClient.Upload(new BinaryData("lock"));
            }

            blobLeaseClient = blobClient.GetBlobLeaseClient();

            this.leaseDuration = leaseDuration ?? TimeSpan.FromSeconds(30);

            if (this.leaseDuration.TotalSeconds < 15.0
                || this.leaseDuration.TotalSeconds > 60.0)            
            {
                throw new ArgumentOutOfRangeException(
                    "leaseDuration",
                    "leaseDuration must be between 15 and 60 seconds."
                    );
            }

            timer = new();
            timer.Interval = this.leaseDuration.TotalMilliseconds / 2;
            timer.AutoReset = true;
            timer.Elapsed += (sender, args) =>
            {
                blobLeaseClient.Renew();
            };
        }

        internal bool TryAcquire()
        {
            try
            {
                blobLeaseClient.Acquire(leaseDuration);
                timer.Start();
                return true;
            }
            catch (Azure.RequestFailedException ex)
            {
                if (ex.ErrorCode == "LeaseAlreadyPresent")
                {
                    return false;
                }
                throw;
            }
        }

        internal void Release()
        {
            timer.Stop();
            blobLeaseClient.Release();
        }
    }
}
