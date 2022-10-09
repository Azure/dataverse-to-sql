using DataverseToSql.Core.Model;
using Microsoft.Extensions.Logging;
using System.Security.AccessControl;

namespace DataverseToSql.Core.Jobs
{
    /// <summary>
    /// Adds the specified CDM entity as a managed entity.
    /// </summary>
    public class AddEntityJob
    {
        private readonly ILogger log;
        private readonly LocalEnvironment environment;
        private readonly string name;

        public AddEntityJob(ILogger log, LocalEnvironment environment, string name)
        {
            this.log = log;
            this.environment = environment;
            this.name = name;
        }

        public async Task RunAsync(CancellationToken cancellationToken = default)
        {
            if (await environment.IsManagedEntityAsync(name, cancellationToken))
            {
                log.LogWarning("Entity {name} is already present in the environment.", name);
                return;
            }

            var (found, cdmEntity) = await environment.TryGetCdmEntityAsync(name, cancellationToken);
            if (!found || cdmEntity is null)
            {
                throw new Exception($"Entity {name} was not found.");
            }

            log.LogInformation("Adding entity {EntityName}.", cdmEntity.Name);

            await environment.database.UpsertAsync(new ManagedEntity
            {
                Name = cdmEntity.Name
            },
            cancellationToken);
        }
    }
}