using Castle.MicroKernel.Registration;
using CluedIn.Connector.Snowflake.Connector;
using CluedIn.Core;
using CluedIn.Core.Configuration;
using ComponentHost;
using Connector.Common;
using System.Timers;

namespace CluedIn.Connector.Snowflake
{
    [Component(nameof(SnowflakeConnectorComponent), "Providers", ComponentType.Service, ServerComponents.ProviderWebApi, Components.Server, Components.DataStores, Isolation = ComponentIsolation.NotIsolated)]
    public sealed class SnowflakeConnectorComponent : ComponentBase<InstallComponents>
    {
        public SnowflakeConnectorComponent(ComponentInfo componentInfo) : base(componentInfo)
        {
        }

        public override void Start()
        {
            base.Start();

            var configurations = Container.Resolve<ISnowflakeConstants>();
            var cacheIntervalValue = ConfigurationManagerEx.AppSettings.GetValue(configurations.CacheSyncIntervalKeyName, configurations.CacheSyncIntervalDefaultValue);
            var syncService = Container.Resolve<IScheduledSyncs>();
            var backgroundCacheSyncTimer = new Timer
            {
                Interval = cacheIntervalValue,
                AutoReset = true
            };
            backgroundCacheSyncTimer.Elapsed += (_, __) => { syncService.Sync().GetAwaiter().GetResult(); };
            backgroundCacheSyncTimer.Start();
        }
    }
}
