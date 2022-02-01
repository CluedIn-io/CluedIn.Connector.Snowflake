using Castle.MicroKernel.Registration;
using Castle.MicroKernel.SubSystems.Configuration;
using Castle.Windsor;
using CluedIn.Connector.Common.Caching;
using CluedIn.Connector.Snowflake.Connector;

namespace CluedIn.Connector.Snowflake
{
    public class InstallComponents : IWindsorInstaller
    {
        public void Install(IWindsorContainer container, IConfigurationStore store)
        {
            container.Register(Component.For<ISnowflakeClient>().ImplementedBy<SnowflakeClient>().OnlyNewServices());
            container.Register(Component.For<ISnowflakeConstants>().ImplementedBy<SnowflakeConstants>().LifestyleSingleton());
            container.Register(Component.For<ICachingService<IDictionary<string, object>, AzureDataLakeConnectorJobData>>()
                .UsingFactoryMethod(x => SqlServerCachingService<IDictionary<string, object>, AzureDataLakeConnectorJobData>.CreateCachingService().GetAwaiter().GetResult())
                .LifestyleSingleton());

            var connector = container.ResolveAll<IConnector>().Single(c => c.GetType() == typeof(AzureDataLakeConnector)) as AzureDataLakeConnector;
            container.Register(Component.For<IScheduledSyncs>().Instance(connector).Named($"{nameof(IScheduledSyncs)}.{nameof(AzureDataLakeConnector)}"));
        }
    }
}
