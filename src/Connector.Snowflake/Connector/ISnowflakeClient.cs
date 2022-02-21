using CluedIn.Connector.Common.Clients;
using Microsoft.Data.SqlClient;
using Snowflake.Data.Client;

namespace CluedIn.Connector.Snowflake.Connector
{
    public interface ISnowflakeClient : IClientBase<SnowflakeDbConnection, SqlParameter>
    {
    }
}
