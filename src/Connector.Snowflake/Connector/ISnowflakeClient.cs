using CluedIn.Connector.Common.Clients;
using Microsoft.Data.SqlClient;
using Snowflake.Data.Client;
using System.Threading.Tasks;

namespace CluedIn.Connector.Snowflake.Connector
{
    public interface ISnowflakeClient : IClientBase<SnowflakeDbConnection, SqlParameter>
    {
        Task SaveData(SnowflakeConnectionData configuration, string content);

        Task RemoveContainer(SnowflakeConnectionData configuration, string containerName);
    }
}
