using CluedIn.Connector.Snowflake.Connector;
using CluedIn.Core.DataStore;
using Microsoft.Extensions.Logging;
using Moq;

namespace CluedIn.Connector.Snowflake.Unit.Tests
{
    public class SnowflakeConnectorTestsBase
    {
        protected readonly SnowflakeConnector Sut;
        protected readonly Mock<IConfigurationRepository> Repo = new Mock<IConfigurationRepository>();
        protected readonly Mock<ILogger<SnowflakeConnector>> Logger = new Mock<ILogger<SnowflakeConnector>>();
        protected readonly Mock<ISqlClient> Client = new Mock<ISqlClient>();
        protected readonly TestContext Context = new TestContext();

        public SnowflakeConnectorTestsBase()
        {
            Sut = new SnowflakeConnector(Repo.Object, Logger.Object, Client.Object);
        }
    }
}
