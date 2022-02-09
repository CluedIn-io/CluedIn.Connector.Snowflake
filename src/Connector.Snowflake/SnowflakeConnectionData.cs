using CluedIn.Connector.Common.Configurations;
using CluedIn.Core.Connectors;
using System;
using System.Collections.Generic;

namespace CluedIn.Connector.Snowflake
{
    public class SnowflakeConnectionData : IConnectorConnection
    {
        public SnowflakeConnectionData(IDictionary<string, object> configurations, string containerName = null)
        {
            Configurations = configurations;
            ContainerName = containerName;
        }

        public string Account => Configurations[SnowflakeConstants.Account] as string;
        public string Host => Configurations[CommonConfigurationNames.Host] as string;
        public string PortNumber => Configurations[CommonConfigurationNames.PortNumber] as string;
        public string Role => Configurations[SnowflakeConstants.Role] as string;
        public string Warehouse => Configurations[SnowflakeConstants.Warehouse] as string;
        public string Username => Configurations[CommonConfigurationNames.Username] as string;
        public string Password => Configurations[CommonConfigurationNames.Password] as string;
        public string DatabaseName => Configurations[CommonConfigurationNames.DatabaseName] as string;
        public string Schema => Configurations[CommonConfigurationNames.Schema] as string;

        public IDictionary<string, object> Configurations { get; }
        public string ContainerName { get; }
        IDictionary<string, object> IConnectorConnection.Authentication { get => Configurations; set { } }

        public override int GetHashCode()
        {
            return HashCode.Combine(HashCode.Combine(Account, Host, PortNumber, Role, Warehouse, Username, Password, DatabaseName), Schema);
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as SnowflakeConnectionData);
        }

        public bool Equals(SnowflakeConnectionData other)
        {
            return other != null &&
                Account == other.Account &&
                Host == other.Host &&
                PortNumber == other.PortNumber &&
                Role == other.Role &&
                Warehouse == other.Warehouse &&
                Username == other.Username &&
                Password == other.Password &&
                DatabaseName == other.DatabaseName &&
                Schema == other.Schema;
        }
    }
}
