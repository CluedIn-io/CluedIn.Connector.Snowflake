using System.Threading.Tasks;

namespace CluedIn.Connector.Snowflake.Connector
{
    public interface IScheduledSyncs
    {
        Task Sync();
    }
}
