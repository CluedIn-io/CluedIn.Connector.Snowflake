using Castle.MicroKernel.Registration;
using Castle.MicroKernel.SubSystems.Configuration;
using Castle.Windsor;
using CluedIn.Connector.Snowflake.Connector;

namespace CluedIn.Connector.Snowflake
{
    public class InstallComponents : IWindsorInstaller
    {
        public void Install(IWindsorContainer container, IConfigurationStore store)
        {
            container.Register(Component.For<ISnowflakeClient>().ImplementedBy<SnowflakeClient>().OnlyNewServices());
            container.Register(Component.For<ISnowflakeConstants>().ImplementedBy<SnowflakeConstants>().LifestyleSingleton());
        }
    }
}
