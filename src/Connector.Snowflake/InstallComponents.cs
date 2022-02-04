using Castle.MicroKernel.Registration;
using Castle.MicroKernel.SubSystems.Configuration;
using Castle.Windsor;
using CluedIn.Connector.Common.Caching;
using CluedIn.Connector.Snowflake.Connector;
using System.Collections.Generic;

namespace CluedIn.Connector.Snowflake
{
    public class InstallComponents : IWindsorInstaller
    {
        public void Install(IWindsorContainer container, IConfigurationStore store)
        {
            container.Register(Component.For<ISnowflakeClient>().ImplementedBy<SnowflakeClient>());
            container.Register(Component.For<ISnowflakeConstants>().ImplementedBy<SnowflakeConstants>().LifestyleSingleton());
            container.Register(Component.For<ICachingService<IDictionary<string, object>, SnowflakeConnectionData>>()
                .UsingFactoryMethod(x => SqlServerCachingService<IDictionary<string, object>, SnowflakeConnectionData>.CreateCachingService().GetAwaiter().GetResult())
                .LifestyleSingleton());

            container.Register(Component.For<IScheduledSyncs>().ImplementedBy<SnowflakeConnector>().Named($"{nameof(IScheduledSyncs)}.{nameof(SnowflakeConnector)}").LifestyleSingleton());
        }
    }
}
