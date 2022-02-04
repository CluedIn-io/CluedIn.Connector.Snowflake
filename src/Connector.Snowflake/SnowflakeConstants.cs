using CluedIn.Connector.Common.Configurations;
using CluedIn.Core.Providers;
using System;

// ReSharper disable ArgumentsStyleStringLiteral

namespace CluedIn.Connector.Snowflake
{
    public class SnowflakeConstants : ConfigurationConstantsBase, ISnowflakeConstants
    {
        public const string Role = nameof(Role);
        public const string Warehouse = nameof(Warehouse);
        public const string Account = nameof(Account);

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

        /// <summary>
        /// Environment key name for cache sync interval
        /// </summary>
        public string CacheSyncIntervalKeyName => "Streams.SnowflakeConnector.CacheSyncInterval";

        /// <summary>
        /// Default value for Cache sync interval in milliseconds
        /// </summary>
        public int CacheSyncIntervalDefaultValue => 60_000;

        /// <summary>
        /// Environment key name for cache records threshold
        /// </summary>
        public string CacheRecordsThresholdKeyName => "Streams.SnowflakeConnector.CacheRecordsThreshold";

        /// <summary>
        /// Default value for Cache records threshold
        /// </summary>
        public int CacheRecordsThresholdDefaultValue => 50;

        private static AuthMethods SnowflakeAuthMethods => new AuthMethods
        {
            token = new Control[]
            {
                new Control
                {
                    name = CommonConfigurationNames.Host,
                    displayName = CommonConfigurationNames.Host.ToDisplayName(),
                    type = "input",
                    isRequired = true
                },
                new Control
                {
                    name = CommonConfigurationNames.DatabaseName,
                    displayName = CommonConfigurationNames.DatabaseName.ToDisplayName(),
                    type = "input",
                    isRequired = true
                },
                new Control
                {
                    name = CommonConfigurationNames.Username,
                    displayName = CommonConfigurationNames.Username.ToDisplayName(),
                    type = "input",
                    isRequired = true
                },
                new Control
                {
                    name = CommonConfigurationNames.Password,
                    displayName = CommonConfigurationNames.Password.ToDisplayName(),
                    type = "password",
                    isRequired = true
                },
                new Control
                {
                    name = CommonConfigurationNames.PortNumber,
                    displayName = CommonConfigurationNames.PortNumber.ToDisplayName(),
                    type = "input",
                    isRequired = false
                },
                new Control
                {
                    name = Account,
                    displayName = Account,
                    type = "input",
                    isRequired = false
                },
                new Control
                {
                    name = CommonConfigurationNames.Schema,
                    displayName = CommonConfigurationNames.Schema,
                    type = "input",
                    isRequired = false
                },
                new Control
                {
                    name = Warehouse,
                    displayName = Warehouse,
                    type = "input",
                    isRequired = false
                },
                new Control
                {
                    name = Role,
                    displayName = Role,
                    type = "input",
                    isRequired = false
                }
            }
        };
    }
}
