namespace DataverseToSql.Core.Model
{
    internal class ManagedCustomScript
    {
        internal ManagedCustomScript(
            string name,
            string hash)
        {
            Name = name;
            Hash = hash;
        }

        public string Name { get; }
        public string Hash { get; }
    }
}
