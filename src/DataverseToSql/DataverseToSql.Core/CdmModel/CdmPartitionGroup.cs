namespace DataverseToSql.Core.CdmModel
{
    internal class CdmPartitionGroup
    {
        internal readonly string Name;
        internal readonly IList<CdmPartition> Partitions;

        internal CdmPartitionGroup(string name, IList<CdmPartition> partitions)
        {
            Name = name;
            Partitions = partitions;
        }
    }
}
