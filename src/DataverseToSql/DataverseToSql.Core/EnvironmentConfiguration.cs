// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Storage.Blobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.FileProviders;
using Newtonsoft.Json;

namespace DataverseToSql.Core
{
    /// <summary>
    /// Configuration class for EnvironmentBase.
    /// The class if used when deserializing the JSON configuration file.
    /// </summary>
    public class EnvironmentConfiguration
    {
        // This constrcutor initializes configuration with placeholder values.
        // Useful to generate a template configuration file.
        public EnvironmentConfiguration()
        {
            ConfigFilePath = "<ignore>";
        }

        // Loads the configuration from the specified filepath.
        public EnvironmentConfiguration(string filepath)
        {
            new ConfigurationBuilder()
                .AddJsonFile(filepath, optional: false)
                .Build()
                .Bind(
                    this,
                    options =>
                    {
                        options.ErrorOnUnknownConfiguration = true;
                    }
                );

            ConfigFilePath = filepath;
        }

        // Loads the configuration from the specified filepath, retrieving
        // the file using the specified IFileProvider implementation.
        // Currently used for ConfigurationBuilder to load the configuration
        // from a storage accoount.
        public EnvironmentConfiguration(string filepath, IFileProvider fileProvider)
        {
            new ConfigurationBuilder()
                .SetFileProvider(fileProvider)
                .AddJsonFile(filepath, optional: false)
                .Build()
                .Bind(
                    this,
                    options =>
                    {
                        options.ErrorOnUnknownConfiguration = true;
                    }
                );

            ConfigFilePath = filepath;
        }

        // Path of the file the configuration was read from
        [JsonIgnore]
        public readonly string ConfigFilePath;

        // Configuration of the storage container hosting
        // Dataverse data
        public StorageConfiguration DataverseStorage { get; set; } = new();

        // Configuration of the storage container where
        // Incremental data must be copied to
        public StorageConfiguration IncrementalStorage { get; set; } = new();
        
        // Configuration of the storage container where
        // configuration information is stored
        public StorageConfiguration ConfigurationStorage { get; set; } = new();

        // Configuration of the target Azure SQL Database
        public DatabaseConfiguration Database { get; set; } = new();

        // Configuration of the Synapse workspace
        public SynapseWorkspaceConfiguration SynapseWorkspace { get; set; } = new();

        // Configuration of the ingestion process
        public IngestionConfiguration Ingestion { get; set; } = new();

        public SchemaHandlingConfiguration SchemaHandling { get; set; } = new();

        // Fill the configuration with placeholder values.
        // Useful to generate a template.
        internal void FillTemplateValues()
        {
            DataverseStorage.FillTemplateValues();
            IncrementalStorage.FillTemplateValues();
            ConfigurationStorage.FillTemplateValues();
            Database.FillTemplateValues();
            SynapseWorkspace.FillTemplateValues();
            SchemaHandling.FillTemplateValues();
        }
    }

    public class StorageConfiguration
    {
        // Name of the storage account without FQDN
        public string StorageAccount { get; set; } = "";
        // Name of the container
        public string Container { get; set; } = "";

        // URI of the container
        public Uri ContainerUri()
        {
            return new BlobUriBuilder(new Uri(StorageAccount))
            {
                BlobContainerName = Container
            }.ToUri();
        }

        // Fill the configuration with placeholder values.
        internal void FillTemplateValues()
        {
            StorageAccount = "<Storage account Blob URI e.g https://accountname.blob.core.windows.net>";
            Container = "<Container name>";
        }
    }

    public class DatabaseConfiguration
    {
        // FQDN of the Azure SQL logical server
        public string Server { get; set; } = "";

        // Name of the database
        public string Database { get; set; } = "";

        // Default schema for the tables containing CDM entities
        public string Schema { get; set; } = "";

        // Fill the configuration with placeholder values.
        internal void FillTemplateValues()
        {
            Server = "<Azure SQL Server FQDN>";
            Database = "<Database name>";
            Schema = "<Default schema>";
        }
    }

    public class IngestionConfiguration
    {
        // Level of parallelism of the Foreach activity of the ingestion pipeline
        public int Parallelism { get; set; } = 1;
    }

    public class SynapseWorkspaceConfiguration
    {
        // Subscription ID of the Synapse Workspace
        public string SubscriptionId { get; set; } = "";

        // Resource group of the Synapse Workspace
        public string ResourceGroup { get; set; } = "";

        // Name of the Synapse Workspace
        public string Workspace { get; set; } = "";

        // Return the URI of the Dev endpoint of the Synapse workspace.
        public string DevEndpoint()
        {
            return $"https://{Workspace}.dev.azuresynapse.net";
        }

        // Return the FQDN of the Serverless endpoint of the Synapse workspace.
        public string ServerlessEndpoint()
        {
            return $"{Workspace}-ondemand.sql.azuresynapse.net";
        }

        // Fill the configuration with placeholder values.
        internal void FillTemplateValues()
        {
            SubscriptionId = "<Subscription ID of the Synapse workspace>";
            ResourceGroup = "<Resource Group of the Synapse workspace>";
            Workspace = "<Name of the Synapse workspace>";
        }
    }

    public class SchemaHandlingConfiguration
    {
        public bool EnableSchemaUpgradeForExistingTables { get; set; } = true;

        internal void FillTemplateValues()
        {
            EnableSchemaUpgradeForExistingTables = true;
        }
    }
}
