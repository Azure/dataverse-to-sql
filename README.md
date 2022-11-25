# DataverseToSql

DataverseToSql is a tool for the incremental transfer of data between [Azure Synapse Link for Dataverse](https://learn.microsoft.com/en-us/power-apps/maker/data-platform/azure-synapse-link-synapse) and [Azure SQL Database](https://learn.microsoft.com/en-us/azure/azure-sql/database/sql-database-paas-overview?view=azuresql).

- Data is transferred incrementally between the container populated by Synapse Link for Dataverse and an Azure SQL Database; this minimizes the amount of data to process while updating the target database.
- DataverseToSql reads from Azure Synapse Link [near real-time data](https://learn.microsoft.com/en-us/power-apps/maker/data-platform/azure-synapse-link-synapse#access-near-real-time-data-and-read-only-snapshot-data) to minimize  latency.
- Schema changes are propagated automatically.

## Alternatives

- [Copy Dataverse data into Azure SQL](https://learn.microsoft.com/en-us/power-apps/maker/data-platform/azure-synapse-link-pipelines?tabs=synapse-analytics)
- [Sync Dataverse data from Synapse Serverless to Azure SQL DB](https://github.com/slavatrofimov/Sync-Dataverse-data-from-Synapse-Serverless-to-Azure-SQL-DB)

## Why?

DataverseToSql goals are:

- Reducing the replication latency between the Dataverse container and Azure SQL Database. compared to other solutions.
- Automating data ingestion pipeline and schema evolution.

## How it works

The core functionality of DataverseToSql is implemented by an Azure Function that extracts data incrementally from the tail of [append-only tables](https://learn.microsoft.com/en-us/power-apps/maker/data-platform/azure-synapse-link-advanced-configuration) using the [Azure Blob Storage API](https://learn.microsoft.com/en-us/rest/api/storageservices/put-block-from-url); the copy is lightweight and is handled entirely by the storage account. DataverseToSql keeps track of the reading position within each blob to identify newly appended data to be copied.

DataverseToSql relies on the [near real-time data](https://learn.microsoft.com/en-us/power-apps/maker/data-platform/azure-synapse-link-synapse#access-near-real-time-data-and-read-only-snapshot-data) produced by Synapse Link for Dataverse instead of the hourly snapshot, adopted by other solutions, in order to reduce the replication latency.

The mechanism implemented by DataverseToSql is an alternative to the native [incremental update feature](https://learn.microsoft.com/en-us/power-apps/maker/data-platform/azure-synapse-incremental-updates) that is currently in preview. Once that feature becomes generally available, it should be considered the preferred solution to extract data incrementally.

The Azure Function copies new data to blobs that can then be consumed by any tool capable of reading CSV files.
DataverseToSql provides an Azure Synapse pipeline with a Copy activity that performs an upsert to Azure SQL Database.
The pipeline relies on [Serverless SQL pool](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql/on-demand-workspace-overview) to read and deduplicate data from the incremental blobs.

To better support the core functionality, DataverseToSql automates the deployment of the required Synapse artifacts (pipeline, datasets, linked services) and updates the schema of the target database when the source schema in Dataverse changes (e.g., when a new column is added).

![Architecture](./images/Architecture.svg)

## Definitions

- **DataverseToSql environment** - (or simply **environment**) the collection of services and metadata that support the incremental copy of data between the Dataverse container and Azure SQL Database.
- **Dataverse container** - the Azure Blob Storage container, created and maintained by Synapse Link for Dataverse, where Dataverse data and metadata first land. It is the source of data for DataverseToSql.
- **Incremental blobs** - the blobs created by DataverseToSql that contain the data extracted incrementally from the blobs in the Dataverse container.

## Prerequisites

- A source Dataverse environment.
- An Azure subscription in the same Azure AD tenant of the Dataverse environment.
- An Azure AD user ("dv2sql user" from now on) in the same Azure AD tenant, with the following permissions:
  - Create and manage Synapse Link for Dataverse in the Dataverse environment.
  - Deploy and own resources in the Azure subscription.
  - Create artifacts in Azure Synapse.
- An environment that [supports .NET 6](https://github.com/dotnet/core/blob/main/release-notes/6.0/supported-os.md) to:
  - Build and deploy the code.
  - Configure and deploy the DataverseToSql environment.

## Setup

### Azure Region

All the Azure resources must be deployed to the same region of the Dataverse environment.

### Azure storage account

Provision an [Azure storage account with Azure Data Lake Storage Gen2](https://learn.microsoft.com/en-us/azure/storage/blobs/create-data-lake-storage-account).

You can opt to create a storage account as part of the creation of the Azure Synapse workspace.

### Assign the Storage Blob Data Contributor role to the dv2sql user

Assign the Storage Blob Data Contributor role on the storage account to the dv2sql user.

### Assign storage permissions to the dv2sql user

Assign the "Storage Blob Data Contributor" on the storage account to the dv2sql user. See [Assign an Azure role for access to blob data](https://learn.microsoft.com/en-us/azure/storage/blobs/assign-azure-role-data-access).

### Configure the storage firewall to allow network access from all networks

To enable access to the storage account by Synapse Link for Dataverse and the Azure Function App, configure the firewall of the storage account to allow network access from all networks. See [Configure Azure Storage firewalls and virtual networks](https://learn.microsoft.com/en-us/azure/storage/common/storage-network-security?tabs=azure-portal).

### Storage containers

Create two containers inside the storage account; the naming is not important.

The access level of the containers must be private.

The two containers are respectively for storing:

- Incremental blobs.
- DataverseToSql configuration files.

### Azure Synapse Analytics workspace

[Provision an Azure Synapse workspace.](https://learn.microsoft.com/en-us/azure/synapse-analytics/quickstart-create-workspace)

### Assign storage permissions to the Synapse workspace

Assign the "Storage Blob Data Contributor" on the storage account to the Synapse workspace. See [Assign an Azure role for access to blob data](https://learn.microsoft.com/en-us/azure/storage/blobs/assign-azure-role-data-access).

### Synapse Link for Dataverse

Setup Synapse Link for Dataverse to replicate tables to the Azure storage account and optionally connect to the Azure Synapse workspace; while an Azure Synapse workspace is not strictly necessary for DataverseToSql, it is anyway recommended because it enables [new analytics capabilities](https://learn.microsoft.com/en-us/power-apps/maker/data-platform/azure-synapse-link-serverless) that you are encouraged to explore.

**Note** The Azure Synapse Workspace is required, whether you decide to connect it to Synapse Link for Dataverse or not.

**Important:** The tables must be configured in append-only mode. See [Advanced Configuration Options in Azure Synapse Link](https://learn.microsoft.com/en-us/power-apps/maker/data-platform/azure-synapse-link-advanced-configuration) for more details.

**NOTE:** The storage account must be the same of the container for incremental data..

To setup Synapse Link for Dataverse connected to Azure Synapse, follow [Create an Azure Synapse Link for Dataverse with your Azure Synapse Workspace](https://learn.microsoft.com/en-us/power-apps/maker/data-platform/azure-synapse-link-synapse).

To setup Synapse Link for Dataverse connected to the storage account only, follow [Create an Azure Synapse Link for Dataverse with Azure Data Lake](https://learn.microsoft.com/en-us/power-apps/maker/data-platform/azure-synapse-link-data-lake).

### Azure SQL Database

Deploy an [Azure SQL Database (single database)](https://learn.microsoft.com/en-us/azure/azure-sql/database/single-database-create-quickstart?view=azuresql&tabs=azure-portal).

### Configure Azure SQL for Azure AD authentication

Configure the Azure SQL logical server for Azure AD authentication. See [Configure and manage Azure AD authentication with Azure SQL](https://learn.microsoft.com/en-us/azure/azure-sql/database/authentication-aad-configure).

### Configure the Azure SQL firewall

Configure the Azure SQL firewall to allow access by

- Azure services
- The environment where the DataverseToSql CLI (dv2sql) is executed

See [Azure SQL Database and Azure Synapse IP firewall rules](https://learn.microsoft.com/en-us/azure/azure-sql/database/firewall-configure?view=azuresql).

### Assign db_owner role to the dv2sql user inside the Azure SQL Database

Create an user as described in [Create contained users mapped to Azure AD identities](https://learn.microsoft.com/en-us/azure/azure-sql/database/authentication-aad-configure?view=azuresql&tabs=azure-powershell#create-contained-users-mapped-to-azure-ad-identities).

The `CREATE USER` command must be executed by the Azure AD administrator.

```sql
CREATE USER [<name_of_the_managed_identity>] FROM EXTERNAL PROVIDER;
```

Assign the db_owner role using the [ALTER ROLE](https://learn.microsoft.com/en-us/sql/t-sql/statements/alter-role-transact-sql) statement.

```sql
ALTER ROLE db_owner ADD MEMBER [<name_of_the_managed_identity>];
```

### Assign db_owner role to the Synapse Workspace identity inside the Azure SQL Database

Create an user as described in [Create contained users mapped to Azure AD identities](https://learn.microsoft.com/en-us/azure/azure-sql/database/authentication-aad-configure?view=azuresql&tabs=azure-powershell#create-contained-users-mapped-to-azure-ad-identities).

The `CREATE USER` command must be executed by the Azure AD administrator.

```sql
CREATE USER [<synapse_workspace_name>] FROM EXTERNAL PROVIDER;
```

Assign the db_owner role using the [ALTER ROLE](https://learn.microsoft.com/en-us/sql/t-sql/statements/alter-role-transact-sql) statement.

```sql
ALTER ROLE db_owner ADD MEMBER [<synapse_workspace_name>];
```

### Azure Function App

Deploy an [Azure Function App](https://learn.microsoft.com/en-us/azure/azure-functions/functions-create-scheduled-function#create-a-function-app).

The function must be configure to use the .NET 6 runtime.

Configure the function to use Application Insights for monitoring.

### Azure Function App managed identity

Assign a managed identity (either system-assigned or a user-assigned) to the Azure Function App. See [How to use managed identities for App Service and Azure Functions](https://learn.microsoft.com/en-us/azure/app-service/overview-managed-identity).

### Assign storage permissions to the Azure Function App managed identity

Assign the "Storage Blob Data Contributor" on the storage account to the Azure Function App managed identity. See [Assign an Azure role for access to blob data](https://learn.microsoft.com/en-us/azure/storage/blobs/assign-azure-role-data-access).

### Assign db_owner role to the Azure Function App managed identity inside the Azure SQL Database

Create an user as described in [Create contained users mapped to Azure AD identities](https://learn.microsoft.com/en-us/azure/azure-sql/database/authentication-aad-configure?view=azuresql&tabs=azure-powershell#create-contained-users-mapped-to-azure-ad-identities).

The `CREATE USER` command must be executed by the Azure AD administrator.

```sql
CREATE USER [<name_of_the_managed_identity>] FROM EXTERNAL PROVIDER;
```

Assign the db_owner role using the [ALTER ROLE](https://learn.microsoft.com/en-us/sql/t-sql/statements/alter-role-transact-sql) statement.

```sql
ALTER ROLE db_owner ADD MEMBER [<name_of_the_managed_identity>];
```

### Assign Synapse Administrator permissions to the Azure Function App managed identity

Assign the [Synapse Administrator RBAC role](https://learn.microsoft.com/en-us/azure/synapse-analytics/security/synapse-workspace-synapse-rbac-roles) to the Azure Function App managed identity.

### Configure the Azure Function App settings

You must configure the following [Function App settings](https://learn.microsoft.com/en-us/azure/azure-functions/functions-how-to-use-azure-function-app-settings).

| Setting                 | Description                                                                                                                                                                                                    | Example                                                    |
|-------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------|
| TIMER_SCHEDULE          | Schedule of the function timer in [NCRONTAB format](https://learn.microsoft.com/en-us/azure/azure-functions/functions-bindings-timer?tabs=in-process&pivots=programming-language-csharp#ncrontab-expressions). | `0 */5 * * * *`                                            |
| CONFIGURATION_CONTAINER | URI of the container with DataverseToSql configuration (as specified in the ConfigurationStorage section of `DataverseToSql.json` under [Configure the environment](#configure-the-environment) below).        | `https://myaccount.blob.core.windows.net/dataverse-to-sql` |

### Install the required software on the local environment

Install the following components where you plan to build the code and use the DataverseToSql CLI (dv2sql).

- [.NET SDK 6.*](https://dotnet.microsoft.com/en-us/download/dotnet/6.0)
- [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli)

For building deploying the Azure Function you can install any of the following:

- Visual Studio 2022.
- Visual Studio Code.

Additional tools are mentioned in [Deployment technologies in Azure Functions](https://learn.microsoft.com/en-us/azure/azure-functions/functions-deployment-technologies).

### Login to Azure CLI

Login to Azure CLI with the dv2sql user (see [Prerequisites](#prerequisites)).

dv2sql uses [DefaultAzureCredential](https://learn.microsoft.com/en-us/dotnet/api/azure.identity.defaultazurecredential) to authenticate to Azure SQL Database, the storage account and Azure Synapse. DefaultAzureCredential tries different authentication methods, including the current Azure CLI credentials.
You can override the authentication process to specify other credentials using the environment variables documented in [EnvironmentCredential](https://learn.microsoft.com/en-us/dotnet/api/azure.identity.environmentcredential).

## Setup checklist

- You have identified an Azure subscription in the same Azure AD tenant of the Dataverse environment.
- The user that performs the setup has administrative permissions in Dataverse and in the Azure subscription.
- All Azure resources are deployed to the same region of the Dataverse environment.
- An Azure storage account with Azure Data Lake Storage Gen2 has been deployed.
- The dv2sql user has been assigned Storage Blob Data Contributor role on the storage account.
- The storage account firewall is configured to enable network access from all networks.
- Two containers have been created inside the storage account to store incremental blobs and DataverseToSql configuration files, respectively.
- An Azure Synapse workspace has been deployed.
- The Synapse workspace has been assigned Storage Blob Data Contributor role on the storage account.
- A link is established between Dataverse and the Azure storage account using Synapse Link for Dataverse. **Note** The storage account must be the same of the container used to store incremental blobs.
- _(Optionally)_ Synapse link for Dataverse is configured to connect to the Azure Synapse workspace.
- Dataverse tables are replicated to Azure Storage in append-only mode.
- An Azure SQL Database (single database) has been deployed.
- Azure SQL is configured to use Azure AD authentication.
- Azure SQL firewall is configured to allow access by Azure services.
- Azure SQL firewall is configured to allow access by the environment where the DataverseToSql CLI is executed.
- The dv2sql user has been assigned the db_owner role inside the database.
- The Synapse Workspace has been assigned the db_owner role inside the database.
- An Azure Function App has been deployed.
- A managed identity has been assigned to the Azure Function App.
- The managed identity of the Azure Function App has been assigned the "Storage Blob Data Contributor" role inside the storage account.
- The managed identity of the Azure Function App has been assigned the db_owner role inside the database.
- The managed identity of the Azure Function App has been assigned the Synapse Administrator role.
- Azure Function App setting TIMER_SCHEDULE has been configured.
- Azure Function App setting CONFIGURATION_CONTAINER has been configured.
- The required software has been installed on the local environment.
- dv2sql user logged on locally to Azure CLI.

## Build and deployment

### Build DataverseToSql CLI (dv2sql)

To build dv2sql run the following command (customize `<output_folder>`).

```shell
dotnet publish --use-current-runtime src/DataverseToSql/DataverseToSql.Cli/DataverseToSql.Cli.csproj -c Release -o <output_folder>
```

Example:

```shell
dotnet publish --use-current-runtime src/DataverseToSql/DataverseToSql.Cli/DataverseToSql.Cli.csproj -c Release -o bin
```

### Build and deploy the Azure Function

For build and deployments steps, specific to the tool of your choice, refer to [Deployment technologies in Azure Functions](https://learn.microsoft.com/en-us/azure/azure-functions/functions-deployment-technologies).

## Usage

### Initialize the environment

DataverseToSql requires a local folder for its configuration files.

Run the following command to create the folder and the necessary configuration files.

```shell
dv2sql init -p <path_to_environment>
```

Example:

```shell
dv2sql init -p ./dataverse-to-sql
```

### Configure the environment

Open the `DataverseToSql.json` file from the folder created above and replace the placeholder values with the details of the environment.

Default template file:

```json
{
  "DataverseStorage": {
    "StorageAccount": "<Storage account Blob URI e.g https://accountname.blob.core.windows.net>",
    "Container": "<Container name>"
  },
  "IncrementalStorage": {
    "StorageAccount": "<Storage account Blob URI e.g https://accountname.blob.core.windows.net>",
    "Container": "<Container name>"
  },
  "ConfigurationStorage": {
    "StorageAccount": "<Storage account Blob URI e.g https://accountname.blob.core.windows.net>",
    "Container": "<Container name>"
  },
  "Database": {
    "Server": "<Azure SQL Server FQDN>",
    "Database": "<Database name>",
    "Schema": "<Default schema>"
  },
  "SynapseWorkspace": {
    "SubscriptionId": "<Subscription ID of the Synapse workspace>",
    "ResourceGroup": "<Resource Group of the Synapse workspace>",
    "Workspace": "<Name of the Synapse workspace>"
  },
  "Ingestion": {
    "Parallelism": 1
  },
  "SchemaHandling": {
    "EnableSchemaUpgradeForExistingTables": true
  }

}
```

| Section              | Setting                              | Default | Description                                                                          |
|----------------------|--------------------------------------|---------|--------------------------------------------------------------------------------------|
| DataverseStorage     | StorageAccount                       |         | FQDN of the storage account containing Dataverse data.                               |
| DataverseStorage     | Container                            |         | Storage container containing Dataverse data (created by Synapse Link for Dataverse). |
| IncrementalStorage   | StorageAccount                       |         | FQDN of the storage account containing incremental data.                             |
| IncrementalStorage   | Container                            |         | Storage container containing incremental data.                                       |
| ConfigurationStorage | StorageAccount                       |         | FQDN of the storage account containing DataverseToSql configuration data.            |
| ConfigurationStorage | Container                            |         | Storage container containing DataverseToSql configuration data.                      |
| Database             | Server                               |         | FQDN of the Azure SQL Database.                                                      |
| Database             | Database                             |         | Name of the Azure SQL Database.                                                      |
| Database             | Schema                               |         | SQL Schema to be used for Dataverse tables.                                          |
| SynapseWorkspace     | SubscriptionId                       |         | Subscription ID of the Synapse workspace.                                            |
| SynapseWorkspace     | ResourceGroup                        |         | Resource group of the Synapse workspace.                                             |
| SynapseWorkspace     | Workspace                            |         | Name of the Synapse workspace.                                                       |
| Ingestion            | Parallelism                          | 1       | Number of concurrent copy activities performed by the ingestion pipeline.            |
| SchemaHandling       | EnableSchemaUpgradeForExistingTables | true    | Enable the propagation of schema changes for existing tables.                        |

### Provide scripts for custom SQL objects

If you plan to introduce custom SQL objects into the database, the preferred way to do so is creating files under the `CustomSqlObjects` folder of the environment.

Using this method is mandatory when the custom SQL objects refer to entity tables or other objects generated by DataverseToSql. Per example, if you plan to create a custom index on an entity table, the index must be placed in a file in `CustomSqlObjects`. Doing otherwise, by creating objects directly on the database, may cause the failure of schema deployment and, as a consequence, block the ingestion process.

Any file placed under `CustomSqlObjects` is treated as a SQL script file and included in the database schema.
Files can be placed directly under `CustomSqlObjects` or organized in folders.

Each file must contain the definition of one or more objects, separated by a GO command.
Only top level DDL commands are considered.

Example:

```sql
CREATE TABLE mytable(
  field1 int,
  field2 int
)
GO

CREATE INDEX ix_mytable ON mytable(field1)
GO
```

When the ingestion process runs, it detects any change in the content of the `CustomSqlObjects` folder in the container and applies the necessary changes to the database.
The ingestion process applies the files in random order and handles dependencies automatically, per example, when a file depends on objects from another file.

The ingestion process applies custom scripts in best effort; if any script contains syntax errors or introduces inconsistencies (e.g., it refers to non-existent objects) it is skipped and a warning is produced by the Azure function.

### Deploy the environment

Environment deployment creates the following:

- The database schema (DataverseToSql metadata tables and OptionsetMetadata tables).
- Synapse linked services for Azure SQL Database and Synapse Serverless SQL pool.
- Synapse datasets.
- Synapse ingestion pipeline.
- DataverseToSql configuration files in the container specified under the ConfigurationStorage section of `DataverseToSql.json`.
- Custom SQL objects script under the `CustomSqlObjects` folder of the container specified under the ConfigurationStorage section of `DataverseToSql.json`.

To perform the deployment, run the command

```shell
dv2sql deploy -p <path_to_environment>
```

Example:

```shell
dv2sql deploy -p ./dataverse-to-sql
```

### Add entities

To add entities (tables) to the environment, for ingestion, use one of the following commands.

To add one entity, run the command

```shell
dv2sql -p <path_to_environment> add --name <entity_name>
```

Example:

```shell
dv2sql -p ./dataverse-to-sql add --name account
```

To add all entities, run the command

```shell
dv2sql -p <path_to_environment> add --all
```

Example:

```shell
dv2sql -p ./dataverse-to-sql add --all
```

## Upgrade

To upgrade the tool to a new version do the following.

1. Build the CLI and function.
2. Disable the function.
3. Run `dv2sql deploy`. See [Deploy the environment](#deploy-the-environment).
4. Deploy the function.
5. Enable the function.

## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit <https://cla.opensource.microsoft.com>.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft
trademarks or logos is subject to and must follow
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
