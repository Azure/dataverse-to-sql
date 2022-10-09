using Azure.Identity;
using DataverseToSql.Core.Jobs;
using Microsoft.Extensions.Logging;
using System.CommandLine;

namespace DataverseToSql.Cli.Commands
{
    internal class Deploy
    {
        internal static Command GetCommandLineCommand(Option<string> pathOption)
        {
            var command = new Command(
                "deploy",
                "Deploy the environment."
                );

            command.SetHandler(
                Handler,
                pathOption
                );

            return command;
        }

        internal static void Handler(string path)
        {
            var logger = Program.loggerFactory.CreateLogger("Deploy");

            try
            {
                var environment = new Core.LocalEnvironment(
                    logger,
                    new DefaultAzureCredential(),
                    path
                    );

                new DeployJob(logger, environment).Run();
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "{message}", ex.Message);
            }
        }
    }
}
