using System;
using System.Collections.Generic;
using CluedIn.Core.Net.Mail;
using CluedIn.Core.Providers;

namespace CluedIn.Connector.Snowflake
{
    public class SnowflakeConstants
    {
        public struct KeyName
        {
            public const string Host = "host";
            public const string DatabaseName = "databaseName";
            public const string Username = "username";
            public const string Password = "password";
            public const string PortNumber = "portNumber";
            public const string Role = "role";
            public const string Warehouse = "warehouse";
            public const string Schema = "schema";
            public const string Account = "account";
        }

        public const string ConnectorName = "SnowflakeConnector";
        public const string ConnectorComponentName = "SnowflakeConnector";
        public const string ConnectorDescription = "Supports publishing of data to external Snowflake databases.";
        public const string Uri = "https://www.snowflake.com";

        public static readonly Guid ProviderId = Guid.Parse("DA0CAC46-A121-47B0-95D3-5FBDC70A36B7");
        public const string ProviderName = "Snowflake Connector";
        public const bool SupportsConfiguration = false;
        public const bool SupportsWebHooks = false;
        public const bool SupportsAutomaticWebhookCreation = false;
        public const bool RequiresAppInstall = false;
        public const string AppInstallUrl = null;
        public const string ReAuthEndpoint = null;

        public static IList<string> ServiceType = new List<string> { "Connector" };
        public static IList<string> Aliases = new List<string> { "SnowflakeConnector" };
        public const string IconResourceName = "Resources.snowflake.png";
        public const string Instructions = "Provide authentication instructions here, if applicable";
        public const IntegrationType Type = IntegrationType.Connector;
        public const string Category = "Connectivity";
        public const string Details = "Provides connectivity to a Snowflake database";

        public static AuthMethods AuthMethods = new AuthMethods
        {
            token = new Control[]
            {
                new Control
                {
                    name = KeyName.Host,
                    displayName = "Host",
                    type = "input",
                    isRequired = true
                },
                new Control
                {
                    name = KeyName.DatabaseName,
                    displayName = "DatabaseName",
                    type = "input",
                    isRequired = true
                },
                new Control
                {
                    name = KeyName.Username,
                    displayName = "Username",
                    type = "input",
                    isRequired = true
                },
                new Control
                {
                    name = KeyName.Password,
                    displayName = "Password",
                    type = "password",
                    isRequired = true
                },
                new Control
                {
                    name = KeyName.PortNumber,
                    displayName = "Port Number",
                    type = "input",
                    isRequired = false
                },
                new Control
                {
                    name = KeyName.Account,
                    displayName = "Account",
                    type = "input",
                    isRequired = false
                },
                new Control
                {
                    name = KeyName.Schema,
                    displayName = "Schema",
                    type = "input",
                    isRequired = false
                },
                new Control
                {
                    name = KeyName.Warehouse,
                    displayName = "Warehouse",
                    type = "input",
                    isRequired = false
                },
                new Control
                {
                    name = KeyName.Role,
                    displayName = "Role",
                    type = "input",
                    isRequired = false
                }
            }
        };

        public static IEnumerable<Control> Properties = new List<Control>
        {

        };

        public static readonly ComponentEmailDetails ComponentEmailDetails = new ComponentEmailDetails {
            Features = new Dictionary<string, string>
            {
                                       { "Connectivity",        "Expenses and Invoices against customers" }
                                   },
            Icon = ProviderIconFactory.CreateConnectorUri(ProviderId),
            ProviderName = ProviderName,
            ProviderId = ProviderId,
            Webhooks = SupportsWebHooks
        };

        public static IProviderMetadata CreateProviderMetadata()
        {
            return new ProviderMetadata {
                Id = ProviderId,
                ComponentName = ConnectorName,
                Name = ProviderName,
                Type = "Connector",
                SupportsConfiguration = SupportsConfiguration,
                SupportsWebHooks = SupportsWebHooks,
                SupportsAutomaticWebhookCreation = SupportsAutomaticWebhookCreation,
                RequiresAppInstall = RequiresAppInstall,
                AppInstallUrl = AppInstallUrl,
                ReAuthEndpoint = ReAuthEndpoint,
                ComponentEmailDetails = ComponentEmailDetails
            };
        }
    }
}
