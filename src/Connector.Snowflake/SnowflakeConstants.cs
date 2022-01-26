using CluedIn.Connector.Common.Configurations;
using CluedIn.Core;
using CluedIn.Core.Providers;
using System;

// ReSharper disable ArgumentsStyleStringLiteral

namespace CluedIn.Connector.Snowflake
{
    public class SnowflakeConstants : ConfigurationConstantsBase, ISnowflakeConstants
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

        public SnowflakeConstants() : base(providerId: Guid.Parse("DA0CAC46-A121-47B0-95D3-5FBDC70A36B7"),
            providerName: "Snowflake Connector",
            componentName: "SnowflakeConnector",
            icon: "Resources.snowflake.png",
            domain: "https://www.snowflake.com",
            about: "Supports publishing of data to external Snowflake databases.",
            authMethods: SnowflakeAuthMethods,
            guideDetails: "Provides connectivity to a Snowflake database")
        {
        }

        private static AuthMethods SnowflakeAuthMethods => new AuthMethods
        {
            token = new Control[]
            {
                new Control
                {
                    name = KeyName.Host,
                    displayName = CommonConfigurationNames.Host.ToDisplayName(),
                    type = "input",
                    isRequired = true
                },
                new Control
                {
                    name = KeyName.DatabaseName,
                    displayName = CommonConfigurationNames.DatabaseName.ToDisplayName(),
                    type = "input",
                    isRequired = true
                },
                new Control
                {
                    name = KeyName.Username,
                    displayName = CommonConfigurationNames.Username.ToDisplayName(),
                    type = "input",
                    isRequired = true
                },
                new Control
                {
                    name = KeyName.Password,
                    displayName = CommonConfigurationNames.Password.ToDisplayName(),
                    type = "password",
                    isRequired = true
                },
                new Control
                {
                    name = KeyName.PortNumber,
                    displayName = CommonConfigurationNames.PortNumber.ToDisplayName(),
                    type = "input",
                    isRequired = false
                },
                new Control
                {
                    name = KeyName.Account,
                    displayName = nameof(KeyName.Account),
                    type = "input",
                    isRequired = false
                },
                new Control
                {
                    name = KeyName.Schema,
                    displayName = nameof(KeyName.Schema),
                    type = "input",
                    isRequired = false
                },
                new Control
                {
                    name = KeyName.Warehouse,
                    displayName = nameof(KeyName.Warehouse),
                    type = "input",
                    isRequired = false
                },
                new Control
                {
                    name = KeyName.Role,
                    displayName = nameof(KeyName.Role),
                    type = "input",
                    isRequired = false
                }
            }
        };
    }
}
