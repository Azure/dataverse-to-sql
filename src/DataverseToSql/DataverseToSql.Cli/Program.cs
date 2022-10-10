// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.Extensions.Logging;
using System.CommandLine;
using System.Reflection;

namespace DataverseToSql.Cli
{
    class Program
    {
        internal static ILoggerFactory loggerFactory;

        static Program()
        {
            loggerFactory = LoggerFactory.Create(builder =>
            {
                builder.AddSimpleConsole(options =>
                {
                    options.IncludeScopes = true;
                    options.SingleLine = true;
                    options.UseUtcTimestamp = false;
                    options.TimestampFormat = "yyyy-MM-dd HH:mm:ss.fffzz ";
                });
            });
        }

        static int Main(string[] args)
        {
            var logger = loggerFactory.CreateLogger("Main");
            logger.LogInformation(
                "Version: {version}",
                Assembly.GetExecutingAssembly().GetName().Version
                );

            try
            {
                RootCommand rootCommand = InitCommands();
                return rootCommand.Invoke(args);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "{Message}", ex.Message);
                return -1;
            }
        }

        private static RootCommand InitCommands()
        {
            var rootCommand = new RootCommand("DataverseToSql CLI");

            var pathOption = new Option<string>(
                name: "--path",
                description: "Path of the environment.",
                getDefaultValue: () => "."
                );
            pathOption.AddAlias("-p");
            pathOption.IsRequired = false;
            rootCommand.AddGlobalOption(pathOption);

            rootCommand.Add(Commands.Init.GetCommandLineCommand(pathOption));
            rootCommand.Add(Commands.Deploy.GetCommandLineCommand(pathOption));
            rootCommand.Add(Commands.EntityAdd.GetCommandLineCommand(pathOption));

            return rootCommand;
        }
    }
}