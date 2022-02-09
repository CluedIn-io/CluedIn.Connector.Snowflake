using CluedIn.Connector.Common.Caching;
using CluedIn.Connector.Snowflake.Connector;
using CluedIn.Core.DataStore;
using Microsoft.Extensions.Logging;
using Moq;
using System.Collections.Generic;

namespace CluedIn.Connector.Snowflake.Unit.Tests
{
    public class SnowflakeConnectorTestsBase
    {
        protected readonly SnowflakeConnector Sut;
        protected readonly Mock<IConfigurationRepository> Repo = new Mock<IConfigurationRepository>();
        protected readonly Mock<ILogger<SnowflakeConnector>> Logger = new Mock<ILogger<SnowflakeConnector>>();
        protected readonly Mock<ISnowflakeClient> Client = new Mock<ISnowflakeClient>();
        protected readonly Mock<ISnowflakeConstants> Constants = new Mock<ISnowflakeConstants>();
        protected readonly TestContext Context = new TestContext();
        protected readonly ICachingService<IDictionary<string, object>, SnowflakeConnectionData> CachingService = new InMemoryCachingService<IDictionary<string, object>, SnowflakeConnectionData>();

        public SnowflakeConnectorTestsBase()
        {
            Sut = new SnowflakeConnector(Repo.Object, Logger.Object, Client.Object, Constants.Object, CachingService);
        }
    }
}
