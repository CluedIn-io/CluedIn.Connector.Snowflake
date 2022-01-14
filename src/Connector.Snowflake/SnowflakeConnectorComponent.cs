using Castle.MicroKernel.Registration;
using CluedIn.Core;
using ComponentHost;
using Connector.Common;

namespace CluedIn.Connector.Snowflake
{
    [Component(nameof(SnowflakeConnectorComponent), "Providers", ComponentType.Service, ServerComponents.ProviderWebApi, Components.Server, Components.DataStores, Isolation = ComponentIsolation.NotIsolated)]
    public sealed class SnowflakeConnectorComponent : ComponentBase<InstallComponents>
    {
        public SnowflakeConnectorComponent(ComponentInfo componentInfo) : base(componentInfo)
        {
        }
    }
}
