// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Core;

namespace DataverseToSql.Core
{
    /// <summary>
    /// Provide an extension method to TokenCredential to retrieve a token for 
    /// some spefied scopes.
    /// </summary>
    internal static class TokenCredentialExtensions
    {
        internal static string GetToken(this TokenCredential credential, string[] scopes)
        {
            return credential.GetToken(
                new(scopes),
                default
                ).Token;
        }
    }
}
