// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure;
using Azure.Storage.Blobs.Specialized;
using System.Text;

namespace BlockBlobClientCopyRangeExtension
{
    public static class BlockBlobClientCopyRangeExtension
    {
        /// <summary>
        /// Performs the copy of a specified range of bytes from a source blob
        /// identified by its URI.
        /// Data replace any existing content in the target blob; they can optionally
        /// be appended using the append argument.
        /// If the target blob does not exist, it is created.
        /// The method relies on the StageBlockFromUriAsync and
        /// CommitBlockListAsync methods of the
        /// Azure.Storage.Blobs.Specialized.BlockBlobClient class.
        /// The copy is performed using one or more blocks, based on the size of the
        /// range to copy and the configured block size.
        /// New blocks are committed to the blob every maxUncommittedBlocks
        /// uncommitted blocks.
        /// </summary>
        /// <param name="targetBlockBlobClient">
        /// Specifies the BlockBlocClient representing the block blob data are
        /// being copied to.
        /// </param>
        /// <param name="sourceBlobUri">
        /// Specifies the URI of the blob data are copied from.
        /// </param>
        /// <param name="sourceAuthentication">
        /// Specifies the authentication details for the source blob.
        /// The HttpAuthorization class allows to specify a scheme and the authentication
        /// informantion.
        /// At the moment only AAD authentication with Bearer scheme is supported.
        /// Scheme must be set to "Bearer" and Parameter must be set to a valid AAD token
        /// issued for the "https://storage.azure.com/" scope.
        /// </param>
        /// <param name="offset">
        /// Specifies the zero-based offset within the source blob of the range
        /// of bytes to copy.
        /// Must be zero or greater.
        /// </param>
        /// <param name="length">
        /// Specifies the length of the range of bytes to copy.
        /// Must be greater than zero.
        /// </param>
        /// <param name="append">
        /// Optionally specifies if the data should be appended to the target
        /// block blob, or if it should replace the existing content.
        /// Default is false.
        /// </param>
        /// <param name="maxBlockSize">
        /// Optionally specifies the size of blocks used during the copy.
        /// Valid values range from 1 B to 4000 MiB.
        /// Default is 100 MiB.
        /// </param>
        /// <param name="maxUncommittedBlocks">
        /// Optionally specifies the maximum number of uncommitted blocks at
        /// any given time.
        /// Valid values range from 1 to 50000.
        /// Default is 50000.
        /// </param>
        /// <param name="cancellationToken"></param>
        /// Optional CancellationToken to propagate
        /// notifications that the operation should be cancelled.
        /// <remarks>
        /// An ArgumentOutOfRangeException will be thrown if
        /// arguments are out of bounds.
        /// </remarks>
        public static async Task CopyRangeFromUriAsync(
            this BlockBlobClient targetBlockBlobClient,
            Uri sourceBlobUri,
            HttpAuthorization sourceAuthentication,
            long offset,
            long length,
            bool append = false,
            long maxBlockSize = 100 * 1024 * 1024,
            int maxUncommittedBlocks = 50000,
            CancellationToken cancellationToken = default)
        {
            #region Arguments validation

            if (offset < 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(offset),
                    $"{nameof(offset)} cannot be negative."
                    );
            }

            if (length <= 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(length),
                    $"{nameof(length)} must be positive."
                    );
            }

            const long MIN_MAXBLOCKSIZE = 1;
            const long MAX_MAXBLOCKSIZE = 4000L * 1024 * 1024;

            if (maxBlockSize < MIN_MAXBLOCKSIZE
                || maxBlockSize > MAX_MAXBLOCKSIZE)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(maxBlockSize),
                    $"{nameof(maxBlockSize)} must be between {MIN_MAXBLOCKSIZE} and {MAX_MAXBLOCKSIZE}."
                    );
            }

            const int MIN_MAXUNCOMMITEDBLOCKS = 1;
            const int MAX_MAXUNCOMMITEDBLOCKS = 50000;

            if (maxUncommittedBlocks < MIN_MAXUNCOMMITEDBLOCKS
                || maxUncommittedBlocks > MAX_MAXUNCOMMITEDBLOCKS)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(maxUncommittedBlocks),
                    $"{nameof(maxUncommittedBlocks)} must be between {MIN_MAXUNCOMMITEDBLOCKS} and {MAX_MAXUNCOMMITEDBLOCKS}."
                    );
            }

            #endregion Arguments validation

            List<string> blockIds;

            // If the append argument is true, retrieve
            // the list of committed blocks IDs to preserve them when
            // staging the blocks (Put Block List).
            // If append is false (default) initialize an empty list
            // of block IDs; when the list will be later staged, any
            // existing committed block will be removed.

            if (append
                && (await targetBlockBlobClient.ExistsAsync(cancellationToken: cancellationToken)).Value)
            {
                blockIds = (await targetBlockBlobClient
                    .GetBlockListAsync(cancellationToken: cancellationToken))
                    .Value
                    .CommittedBlocks
                    .Select(block => block.Name)
                    .ToList();
            }
            else
            {
                blockIds = new();
            }

            long target = offset + length;
            int uncommittedBlocks = 0;

            // Copy the original blob content using one or more blocks, depending
            // on its size.
            // The size of each block is controlled by the maxBlockSize argument.
            // Blocks are assigned a sequential numeric ID starting with 0000000000.
            while (offset < target)
            {
                var blockLength = target - offset;
                blockLength = blockLength > maxBlockSize ? maxBlockSize : blockLength;

                var blockId = Convert.ToBase64String(
                    Encoding.UTF8.GetBytes($"{blockIds.Count:0000000000}"));

                await targetBlockBlobClient.StageBlockFromUriAsync(
                    sourceBlobUri,
                    blockId,
                    new()
                    {
                        SourceRange = new(offset, blockLength),
                        SourceAuthentication = sourceAuthentication
                    },
                    cancellationToken: cancellationToken);

                blockIds.Add(blockId);
                uncommittedBlocks++;

                if (uncommittedBlocks == maxUncommittedBlocks)
                {
                    await targetBlockBlobClient.CommitBlockListAsync(
                        blockIds,
                        cancellationToken: cancellationToken);
                    uncommittedBlocks = 0;
                }

                offset += blockLength;
            }

            if (uncommittedBlocks > 0)
            {
                await targetBlockBlobClient.CommitBlockListAsync(
                    blockIds,
                    cancellationToken: cancellationToken);
            }
        }

        /// <summary>
        /// Performs the copy of a specified range of bytes from a source blob
        /// identified by its URI.
        /// Data replace any existing content in the target blob; they can optionally
        /// be appended using the append argument.
        /// If the target blob does not exist, it is created.
        /// The method relies on the StageBlockFromUri and
        /// CommitBlockList methods of the
        /// Azure.Storage.Blobs.Specialized.BlockBlobClient class.
        /// The copy is performed using one or more blocks, based on the size of the
        /// range to copy and the configured block size.
        /// New blocks are committed to the blob every maxUncommittedBlocks
        /// uncommitted blocks.
        /// </summary>
        /// <param name="targetBlockBlobClient">
        /// Specifies the BlockBlocClient representing the block blob data are
        /// being copied to.
        /// </param>
        /// <param name="sourceBlobUri">
        /// Specifies the URI of the blob data are copied from.
        /// </param>
        /// <param name="sourceAuthentication">
        /// Specifies the authentication details for the source blob.
        /// The HttpAuthorization class allows to specify a scheme and the authentication
        /// informantion.
        /// At the moment only AAD authentication with Bearer scheme is supported.
        /// Scheme must be set to "Bearer" and Parameter must be set to a valid AAD token
        /// issued for the "https://storage.azure.com/" scope.
        /// </param>
        /// <param name="offset">
        /// Specifies the zero-based offset within the source blob of the range
        /// of bytes to copy.
        /// Must be zero or greater.
        /// </param>
        /// <param name="length">
        /// Specifies the length of the range of bytes to copy.
        /// Must be greater than zero.
        /// </param>
        /// <param name="append">
        /// Optionally specifies if the data should be appended to the target
        /// block blob, or if it should replace the existing content.
        /// Default is false.
        /// </param>
        /// <param name="maxBlockSize">
        /// Optionally specifies the size of blocks used during the copy.
        /// Valid values range from 1 B to 4000 MiB.
        /// Default is 100 MiB.
        /// </param>
        /// <param name="maxUncommittedBlocks">
        /// Optionally specifies the maximum number of uncommitted blocks at
        /// any given time.
        /// Valid values range from 1 to 50000.
        /// Default is 50000.
        /// </param>
        /// <param name="cancellationToken"></param>
        /// Optional CancellationToken to propagate
        /// notifications that the operation should be cancelled.
        /// <remarks>
        /// An ArgumentOutOfRangeException will be thrown if
        /// arguments are out of bounds.
        /// </remarks>
        public static void CopyRangeFromUri(
            this BlockBlobClient targetBlockBlobClient,
            Uri sourceBlobUri,
            HttpAuthorization sourceAuthentication,
            long offset,
            long length,
            bool append = false,
            long maxBlockSize = 100 * 1024 * 1024,
            int maxUncommittedBlocks = 50000,
            CancellationToken cancellationToken = default)
        {
            #region Arguments validation

            if (offset < 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(offset),
                    $"{nameof(offset)} cannot be negative."
                    );
            }

            if (length <= 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(length),
                    $"{nameof(length)} must be positive."
                    );
            }

            const long MIN_MAXBLOCKSIZE = 1;
            const long MAX_MAXBLOCKSIZE = 4000L * 1024 * 1024;

            if (maxBlockSize < MIN_MAXBLOCKSIZE
                || maxBlockSize > MAX_MAXBLOCKSIZE)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(maxBlockSize),
                    $"{nameof(maxBlockSize)} must be between {MIN_MAXBLOCKSIZE} and {MAX_MAXBLOCKSIZE}."
                    );
            }

            const int MIN_MAXUNCOMMITEDBLOCKS = 1;
            const int MAX_MAXUNCOMMITEDBLOCKS = 50000;

            if (maxUncommittedBlocks < MIN_MAXUNCOMMITEDBLOCKS
                || maxUncommittedBlocks > MAX_MAXUNCOMMITEDBLOCKS)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(maxUncommittedBlocks),
                    $"{nameof(maxUncommittedBlocks)} must be between {MIN_MAXUNCOMMITEDBLOCKS} and {MAX_MAXUNCOMMITEDBLOCKS}."
                    );
            }

            #endregion Arguments validation

            List<string> blockIds;

            // If the append argument is true, retrieve
            // the list of committed blocks IDs to preserve them when
            // staging the blocks (Put Block List).
            // If append is false (default) initialize an empty list
            // of block IDs; when the list will be later staged, any
            // existing committed block will be removed.

            if (append && targetBlockBlobClient.Exists(cancellationToken: cancellationToken).Value)
            {
                blockIds = targetBlockBlobClient
                    .GetBlockList(cancellationToken: cancellationToken).Value
                    .CommittedBlocks
                    .Select(block => block.Name)
                    .ToList();
            }
            else
            {
                blockIds = new();
            }

            long target = offset + length;
            int uncommittedBlocks = 0;

            // Copy the original blob content using one or more blocks, depending
            // on its size.
            // The size of each block is controlled by the maxBlockSize argument.
            // Blocks are assigned a sequential numeric ID starting with 0000000000.
            while (offset < target)
            {
                var blockLength = target - offset;
                blockLength = blockLength > maxBlockSize ? maxBlockSize : blockLength;

                var blockId = Convert.ToBase64String(
                    Encoding.UTF8.GetBytes($"{blockIds.Count:0000000000}"));

                targetBlockBlobClient.StageBlockFromUri(
                    sourceBlobUri,
                    blockId,
                    new()
                    {
                        SourceRange = new(offset, blockLength),
                        SourceAuthentication = sourceAuthentication
                    },
                    cancellationToken: cancellationToken);

                blockIds.Add(blockId);
                uncommittedBlocks++;

                if (uncommittedBlocks == maxUncommittedBlocks)
                {
                    targetBlockBlobClient.CommitBlockList(
                        blockIds,
                        cancellationToken: cancellationToken);
                    uncommittedBlocks = 0;
                }

                offset += blockLength;
            }

            if (uncommittedBlocks > 0)
            {
                targetBlockBlobClient.CommitBlockList(
                    blockIds,
                    cancellationToken: cancellationToken);
            }
        }
    }
}