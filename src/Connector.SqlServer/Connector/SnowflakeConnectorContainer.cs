using CluedIn.Core.Connectors;

namespace CluedIn.Connector.Snowflake.Connector
{
    public class SnowflakeConnectorContainer : IConnectorContainer
    {
        public string Name { get; set; }
        public string Id { get; set; }
        public string FullyQualifiedName { get; set; }
    }
}
