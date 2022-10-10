// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.Extensions.Logging;
using System.CommandLine;

namespace DataverseToSql.Cli.Commands
{
    internal class Init
    {
        internal static Command GetCommandLineCommand(Option<string> pathOption)
        {
            var command = new Command(
                "init",
                "Initialize an environment."
                );

            command.SetHandler(
                Handler,
                pathOption
                );

            return command;
        }

        internal static void Handler(string path)
        {
            var logger = Program.loggerFactory.CreateLogger("Init");

            try
            {
                Core.LocalEnvironment.Init(logger, path);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "{message}", ex.Message);
            }
        }
    }
}
