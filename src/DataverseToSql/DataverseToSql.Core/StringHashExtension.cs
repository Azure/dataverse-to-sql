using System.Security.Cryptography;
using System.Text;

namespace DataverseToSql.Core
{
    /// <summary>
    /// Provides an extension method for strings to compute SHA1.
    /// </summary>
    internal static class StringHashExtension
    {
        internal static string Sha1(this string sourceString)
        {
            var sha1 = SHA1.Create();
            var hash = sha1.ComputeHash(Encoding.UTF8.GetBytes(sourceString));
            return Convert.ToBase64String(hash);
        }
    }
}
