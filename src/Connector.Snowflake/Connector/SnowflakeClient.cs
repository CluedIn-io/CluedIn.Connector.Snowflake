using CluedIn.Connector.Common.Clients;
using CluedIn.Connector.Common.Configurations;
using Microsoft.Data.SqlClient;
using Snowflake.Data.Client;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

namespace CluedIn.Connector.Snowflake.Connector
{
    public class SnowflakeClient : ClientBase<SnowflakeDbConnection, SqlParameter>, ISnowflakeClient
    {
        public override async Task<SnowflakeDbConnection> GetConnection(IDictionary<string, object> config)
        {
            var connectionString = BuildConnectionString(config);
            var connection = new SnowflakeDbConnection
            {
                ConnectionString = connectionString
            };

            await connection.OpenAsync();
            return connection;
        }

        public override async Task<DataTable> GetTables(IDictionary<string, object> config, string name = null)
        {
            using var connection = await GetConnection(config);
            var cmd = connection.CreateCommand();
            cmd.CommandText = "select * from " + name;
            var reader = await cmd.ExecuteReaderAsync();
            var dataTable = new DataTable();
            dataTable.Load(reader);

            return dataTable;
        }

        public override async Task<DataTable> GetTableColumns(IDictionary<string, object> config, string tableName)
        {
            using var connection = await GetConnection(config);
            var cmd = connection.CreateCommand();
            cmd.CommandText = "select * from " + tableName;
            var reader = await cmd.ExecuteReaderAsync();
            var dataTable = new DataTable();
            dataTable.Load(reader);

            return dataTable;
        }

        public override string BuildConnectionString(IDictionary<string, object> config)
        {
            var connectionString = string.Format("scheme=https;ACCOUNT={0};HOST={1};port={2};ROLE={3};WAREHOUSE={4};USER={5};PASSWORD={6};DB={7};SCHEMA={8}",
                (string)config[SnowflakeConstants.KeyName.Account],
                (string)config[SnowflakeConstants.KeyName.Host],
                (string)config[SnowflakeConstants.KeyName.PortNumber],
                (string)config[SnowflakeConstants.KeyName.Role],
                (string)config[SnowflakeConstants.KeyName.Warehouse],
                (string)config[SnowflakeConstants.KeyName.Username],
                (string)config[SnowflakeConstants.KeyName.Password],
                (string)config[SnowflakeConstants.KeyName.DatabaseName],
                (string)config[SnowflakeConstants.KeyName.Schema]);

            return connectionString;
        }
    }
}
