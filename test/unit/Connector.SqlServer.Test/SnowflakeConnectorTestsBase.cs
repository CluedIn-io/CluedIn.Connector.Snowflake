using CluedIn.Connector.Snowflake.Connector;
using CluedIn.Core.DataStore;
using Microsoft.Extensions.Logging;
using Moq;

namespace CluedIn.Connector.Snowflake.Unit.Tests
{
    public class SnowflakeConnectorTestsBase
    {
        public readonly SnowflakeConnector Sut;
        public readonly Mock<IConfigurationRepository> Repo = new Mock<IConfigurationRepository>();
        public readonly Mock<ILogger<SnowflakeConnector>> Logger = new Mock<ILogger<SnowflakeConnector>>();
        public readonly Mock<ISnowflakeClient> Client = new Mock<ISnowflakeClient>();
        public readonly Mock<ISnowflakeConstants> Constants = new Mock<ISnowflakeConstants>();
        public readonly TestContext Context = new TestContext();

        public SnowflakeConnectorTestsBase()
        {
            Sut = new SnowflakeConnector(Repo.Object, Logger.Object, Client.Object, Constants.Object);
        }
    }
}
