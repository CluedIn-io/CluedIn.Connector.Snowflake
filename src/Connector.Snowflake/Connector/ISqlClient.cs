using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using CluedIn.Core.Connectors;
using Microsoft.Data.SqlClient;
using Snowflake.Data.Client;

namespace CluedIn.Connector.Snowflake.Connector
{
    public interface ISqlClient
    {
        Task ExecuteCommandAsync(IConnectorConnection config, string commandText, IList<SqlParameter> param = null);
        Task<SnowflakeDbConnection> GetConnection(IDictionary<string, object> config);
        Task<DataTable> GetTables(IDictionary<string, object> config, string name = null);
        Task<DataTable> GetTableColumns(IDictionary<string, object> config, string tableName);
    }
}
