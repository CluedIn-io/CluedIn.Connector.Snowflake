using CluedIn.Core.Connectors;
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

        public IDictionary<string, object> Configurations { get; }
        public string ContainerName { get; }
        IDictionary<string, object> IConnectorConnection.Authentication { get => Configurations; set { } }
    }
}
