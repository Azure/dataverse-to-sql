using Azure.Core;
using Azure.Identity;
using Azure.Storage.Blobs;
using DataverseToSql.Core;
using DataverseToSql.Core.Jobs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace DataverseToSql.Function
{
    public class Ingestion
    {
        [FunctionName("Ingestion")]
        public async Task Run([TimerTrigger("%TIMER_SCHEDULE%")] TimerInfo myTimer, ILogger log)
        {
            var cred = new DefaultAzureCredential();

            // Instantiate the BlobContainerClient to read configuration.
            var blobContainerClient = new BlobContainerClient(
                new Uri(Environment.GetEnvironmentVariable("CONFIGURATION_CONTAINER")),
                cred,
                default);

            // Instantiate the CloudEnvironment from the specified container.
            var env = new CloudEnvironment(log, cred, blobContainerClient);

            // Run the ingestion job
            var job = new IngestionJob(log, env);
            await job.RunAsync();
        }
    }
}
