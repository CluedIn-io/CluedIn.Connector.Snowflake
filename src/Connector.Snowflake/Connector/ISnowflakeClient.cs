using CluedIn.Connector.Common.Clients;
using CluedIn.Core.Connectors;
using Microsoft.Data.SqlClient;
using Snowflake.Data.Client;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace CluedIn.Connector.Snowflake.Connector
{
    public interface ISnowflakeClient : IClientBase<SnowflakeDbConnection, SqlParameter>
    {
        Task CreateContainer(SnowflakeConnectionData configuration, CreateContainerModel model);
        Task EmptyContainer(SnowflakeConnectionData configuration, string containerName);
        Task RenameContainer(SnowflakeConnectionData configuration, string oldName, string newName);
        Task RemoveContainer(SnowflakeConnectionData configuration, string containerName);
        Task SaveData(SnowflakeConnectionData configuration, IEnumerable<KeyValuePair<string, object>> content);
    }
}
