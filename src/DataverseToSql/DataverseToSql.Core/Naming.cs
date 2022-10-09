using DataverseToSql.Core.CdmModel;

namespace DataverseToSql.Core
{
    /// <summary>
    /// Provides naming for Synapse artifacts and some SQL objects.
    /// </summary>
    internal static class Naming
    {
        // Pipelines
        internal static string PipelineFolderName() => "DataverseToSql";
        internal static string IngestionPipelineName() => $"DataverseToSql_Ingestion"; 

        // Datasets
        internal static string DatasetFolder() => "DataverseToSql";
        internal static string MetadataDatasetName() => "DataverseToSql_Metadata";
        internal static string ServerlessDatasetName() => "DataverseToSql_Severless";
        internal static string AzureSqlDatasetName() => "DataverseToSql_AzureSQL";

        // Linked services
        internal static string AzureSqlLinkedServiceName() => "DataverseToSql_AzureSQL";
        internal static string ServerlessPoolLinkedServiceName() => "DataverseToSql_Serverless";

        // SQL objects
        // Note: changing the names below requires changing the Copy activity code in DeployJob
        // to reflect the new naming in the expressions referencing stored procedure and table
        // type
        internal static string MergeProcName(this CdmEntity entity) => $"[Merge_{entity.Name}]";
        internal static string TableTypeName(this CdmEntity entity) => $"[{entity.Name}_TableType]";
    }
}
