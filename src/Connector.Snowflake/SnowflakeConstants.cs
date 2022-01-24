using CluedIn.Connector.Common.Configurations;
using CluedIn.Core;
using CluedIn.Core.Providers;
using System;

// ReSharper disable ArgumentsStyleStringLiteral

namespace CluedIn.Connector.Snowflake
{
    public class SnowflakeConstants : ConfigurationConstantsBase, ISnowflakeConstants
    {
        public const string Account = nameof(Account);
        public const string Role = nameof(Role);
        public const string Warehouse = nameof(Warehouse);

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
                    name = CommonConfigurationNames.Host.ToCamelCase(),
                    displayName = CommonConfigurationNames.Host.ToDisplayName(),
                    type = "input",
                    isRequired = true
                },
                new Control
                {
                    name = CommonConfigurationNames.DatabaseName.ToCamelCase(),
                    displayName = CommonConfigurationNames.DatabaseName.ToDisplayName(),
                    type = "input",
                    isRequired = true
                },
                new Control
                {
                    name = CommonConfigurationNames.Username.ToCamelCase(),
                    displayName = CommonConfigurationNames.Username.ToDisplayName(),
                    type = "input",
                    isRequired = true
                },
                new Control
                {
                    name = CommonConfigurationNames.Password.ToCamelCase(),
                    displayName = CommonConfigurationNames.Password.ToDisplayName(),
                    type = "password",
                    isRequired = true
                },
                new Control
                {
                    name = CommonConfigurationNames.PortNumber.ToCamelCase(),
                    displayName = CommonConfigurationNames.PortNumber.ToDisplayName(),
                    type = "input",
                    isRequired = false
                },
                new Control
                {
                    name = Account.ToCamelCase(),
                    displayName = Account,
                    type = "input",
                    isRequired = false
                },
                new Control
                {
                    name = CommonConfigurationNames.Schema.ToCamelCase(),
                    displayName = CommonConfigurationNames.Schema.ToDisplayName(),
                    type = "input",
                    isRequired = false
                },
                new Control
                {
                    name = Warehouse.ToCamelCase(),
                    displayName = Warehouse,
                    type = "input",
                    isRequired = false
                },
                new Control
                {
                    name = Role.ToCamelCase(),
                    displayName = Role,
                    type = "input",
                    isRequired = false
                }
            }
        };
    }
}
