using CluedIn.Connector.Common;
using CluedIn.Connector.Common.Configurations;
using CluedIn.Core;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;

namespace CluedIn.Connector.Snowflake
{
    public class SnowflakeConnectorProvider : ConnectorProviderBase<SnowflakeConnectorProvider>
    {
        public SnowflakeConnectorProvider([NotNull] ApplicationContext appContext, ISnowflakeConstants configuration, ILogger<SnowflakeConnectorProvider> logger)
            : base(appContext, configuration, logger)
        {
        }

        protected override IEnumerable<string> ProviderNameParts => new[] { CommonConfigurationNames.Host, CommonConfigurationNames.DatabaseName };
    }
}
