using CluedIn.Connector.Common;
using CluedIn.Connector.Common.Configurations;
using CluedIn.Core;
using CluedIn.Core.Crawling;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace CluedIn.Connector.Snowflake
{
    public class SnowflakeConnectorProvider : ConnectorProviderBase<SnowflakeConnectorProvider>
    {
        public SnowflakeConnectorProvider([NotNull] ApplicationContext appContext, ISnowflakeConstants configuration, ILogger<SnowflakeConnectorProvider> logger)
            : base(appContext, configuration, logger)
        {
        }

        protected override IEnumerable<string> ProviderNameParts => new[] { CommonConfigurationNames.Host, CommonConfigurationNames.DatabaseName };

        public override string Schedule(DateTimeOffset relativeDateTime, bool webHooksEnabled)
            => $"{relativeDateTime.Minute} 0/23 * * *";

        public override Task<CrawlLimit> GetRemainingApiAllowance(ExecutionContext context, CrawlJobData jobData, Guid organizationId, Guid userId, Guid providerDefinitionId)
            => Task.FromResult(new CrawlLimit(-1, TimeSpan.Zero));
    }
}
