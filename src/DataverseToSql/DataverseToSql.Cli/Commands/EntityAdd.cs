// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Identity;
using DataverseToSql.Core.Jobs;
using Microsoft.Extensions.Logging;
using System.CommandLine;

namespace DataverseToSql.Cli.Commands
{
    internal class EntityAdd
    {
        internal static Command GetCommandLineCommand(Option<string> pathOption)
        {
            var command = new Command(
                "add",
                "Add entities to the environment."
                );

            var nameOption = new Option<string>(
                name: "--name",
                description: "Name of the entity to add."
                );
            nameOption.AddAlias("-n");
            command.Add(nameOption);

            var allOption = new Option<bool>(
                name: "--all",
                description: "Add all available entities."
                );
            allOption.AddAlias("-a");
            command.Add(allOption);

            command.SetHandler(
                Handler,
                pathOption,
                nameOption,
                allOption
                );

            command.AddValidator(result =>
            {
                if (result.GetValueForOption(nameOption) is null
                    && !result.GetValueForOption(allOption))
                {
                    result.ErrorMessage = "Must specify either --name or --all.";
                }
            });

            return command;
        }

        internal static async void Handler(string path, string name, bool all)
        {
            var logger = Program.loggerFactory.CreateLogger("Add");

            try
            {
                var environment = new Core.LocalEnvironment(
                    logger,
                    new DefaultAzureCredential(),
                    path
                    );

                if (all)
                {
                    var job = new AddAllEntitiesJob(logger, environment);
                    job.RunAsync().Wait();
                }
                else
                {
                    var job = new AddEntityJob(logger, environment, name);
                    job.RunAsync().Wait();
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "{message}", ex.Message);
            }
        }
    }
}
