// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Core;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace DataverseToSql.Core
{
    /// <summary>
    /// Derived class of EnvironmentBase that works with configuration from local files.
    /// Provides the ability to initialize a local environment from scratch.
    /// </summary>
    public class LocalEnvironment : EnvironmentBase
    {
        public readonly string LocalPath;

        public LocalEnvironment(ILogger log, TokenCredential credential, string path)
            : base(log, credential, ReadConfig(log, path))
        {
            LocalPath = path;
        }

        // Loads configuration from the specified path.
        private static EnvironmentConfiguration ReadConfig(ILogger log, string path)
        {
            var environmentPath = Path.GetFullPath(path);
            var ConfigFilePath = Path.Join(environmentPath, CONFIG_FILE);

            log.LogInformation(
                "Loading environment from path {EnvironmentPath}",
                environmentPath
                );

            log.LogInformation(
                "Loading configuration file {ConfigFilePath}",
                ConfigFilePath
                );

            try
            {
                return new(ConfigFilePath);
            }
            catch (Exception ex)
            {
                throw new Exception(
                    $"Cannot load configuration from {ConfigFilePath}", ex);
            }
        }

        // Initialize an environment on the local file system, at the specified path.
        // The configuration file is initialized with placeholder values.
        public static void Init(ILogger log, string path)
        {
            // Create environment root folder, if it does not exist

            var environmentPath = Path.GetFullPath(path);
            log.LogInformation(
                "Initializing environment in path {path}",
                environmentPath
                );

            try
            {
                if (!Directory.Exists(environmentPath))
                {
                    Directory.CreateDirectory(environmentPath);
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Error creating environment directory.", ex);
            }

            // Create a the configuration file.

            var configFile = Path.Join(environmentPath, CONFIG_FILE);

            if (File.Exists(configFile))
            {
                throw new Exception($"Configuration file {configFile} already exists.");
            }

            try
            {
                var emptyConfig = new EnvironmentConfiguration();
                emptyConfig.FillTemplateValues();

                File.WriteAllText(
                    configFile,
                    JsonConvert.SerializeObject(emptyConfig, Formatting.Indented));

            }
            catch (Exception ex)
            {
                throw new Exception("Error creating configuration file.", ex);
            }

            // Create custom SQL objects folder

            var customSqlObjectsFolderName = Path.Join(environmentPath, CUSTOM_SQL_OBJECTS_FOLDER);

            if (!Directory.Exists(customSqlObjectsFolderName))
            {
                try
                {
                    log.LogInformation("Creating custom SQL objects folder {customSqlObjectsFolderName}.",
                        customSqlObjectsFolderName);
                    Directory.CreateDirectory(customSqlObjectsFolderName);
                }
                catch (Exception ex)
                {
                    throw new Exception("Error creating custom SQL objects folder.", ex);
                }
            }

            log.LogInformation("Successfully initialized the environment.");
        }

        internal override Task<IList<(string name, string script)>> InitCustomScriptsAsync(CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }
    }
}
