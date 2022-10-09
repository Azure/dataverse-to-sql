using Azure.Core;
using Microsoft.SqlServer.Dac;

namespace DataverseToSql.Core.Auth
{
    /// <summary>
    /// Provides access tokens for DacFx to connect to Azure SQL Database.
    /// </summary>
    internal class DacAuthProvider : IUniversalAuthProvider
    {
        private readonly TokenCredential _credential;
        public DacAuthProvider(TokenCredential credential)
        {
            _credential = credential;
        }

        public string GetValidAccessToken()
        {
            return _credential.GetToken(
                new(new[] { "https://database.windows.net/.default" })
                , default
                ).Token;
        }
    }
}
