// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.Extensions.Logging;

namespace DataverseToSql.Core.Jobs
{
    /// <summary>
    /// Adds all available CDM entities as managed entities.
    /// </summary>
    public class AddAllEntitiesJob
    {
        private readonly ILogger log;
        private readonly LocalEnvironment environment;

        public AddAllEntitiesJob(ILogger log, LocalEnvironment environment)
        {
            this.log = log;
            this.environment = environment;
        }

        public async Task RunAsync(CancellationToken cancellationToken=default)
        {
            var entityCount = 0;

            foreach (var cdmEntity in await environment.GetCdmEntitiesAsync(cancellationToken))
            {
                if (! await environment.IsManagedEntityAsync(cdmEntity, cancellationToken))
                {
                    entityCount++;
                    var job = new AddEntityJob(log, environment, cdmEntity.Name);
                    await job.RunAsync(cancellationToken);
                }
            }

            if (entityCount == 0)
            {
                log.LogInformation("No entities were added.");
            }
        }
    }
}
