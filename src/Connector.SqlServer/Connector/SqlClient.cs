using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using CluedIn.Core.Connectors;
using Microsoft.Data.SqlClient;
using Snowflake.Data.Client;

namespace CluedIn.Connector.Snowflake.Connector
{
    public class SqlClient : ISqlClient
    {
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Security", "CA2100:Review SQL queries for security vulnerabilities", Justification = "<Pending>")]
        public async Task ExecuteCommandAsync(IConnectorConnection config, string commandText, IList<SqlParameter> param = null)
        {
            using (var connection = await GetConnection(config.Authentication))
            {
                var cmd = connection.CreateCommand();

                cmd.CommandText = commandText;

                if (param != null)
                    cmd.Parameters.AddRange(param.ToArray());

                await cmd.ExecuteNonQueryAsync();
            }
        }

        public async Task<SnowflakeDbConnection> GetConnection(IDictionary<string, object> config)
        {
            string connectionString = string.Format("scheme=https;ACCOUNT={0};HOST={1};port={2};ROLE={3};WAREHOUSE={4};USER={5};PASSWORD={6};DB={7};SCHEMA={8}", (string)config[SnowflakeConstants.KeyName.Account], (string)config[SnowflakeConstants.KeyName.Host], (string)config[SnowflakeConstants.KeyName.PortNumber], (string)config[SnowflakeConstants.KeyName.Role], (string)config[SnowflakeConstants.KeyName.Warehouse], (string)config[SnowflakeConstants.KeyName.Username], (string)config[SnowflakeConstants.KeyName.Password], (string)config[SnowflakeConstants.KeyName.DatabaseName], (string)config[SnowflakeConstants.KeyName.Schema]);

            var result = new SnowflakeDbConnection();

            result.ConnectionString = connectionString;

            await result.OpenAsync();

            return result;
        }

        public async Task<DataTable> GetTables(IDictionary<string, object> config, string name = null)
        {
            using (var connection = await GetConnection(config))
            {
                DataTable result;
                if (!string.IsNullOrEmpty(name))
                {
                    var restrictions = new string[4];
                    restrictions[2] = name;

                    result = connection.GetSchema("Tables", restrictions);
                }
                else
                {
                    result = connection.GetSchema("Tables");
                }

                return result;
            }
        }

        public async Task<DataTable> GetTableColumns(IDictionary<string, object> config, string tableName)
        {
            using (var connection = await GetConnection(config))
            {

                var restrictions = new string[4];
                restrictions[2] = tableName;

                var result = connection.GetSchema("Columns", restrictions);

                return result;
            }
        }
    }
}
