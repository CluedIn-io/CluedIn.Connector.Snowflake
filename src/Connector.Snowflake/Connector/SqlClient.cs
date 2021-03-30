using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
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
            using (var conn = new SnowflakeDbConnection())
            {
                string connectionString = string.Format("scheme=https;ACCOUNT={0};HOST={1};port={2};ROLE={3};WAREHOUSE={4};USER={5};PASSWORD={6};DB={7};SCHEMA={8}", (string)config.Authentication[SnowflakeConstants.KeyName.Account], (string)config.Authentication[SnowflakeConstants.KeyName.Host], (string)config.Authentication[SnowflakeConstants.KeyName.PortNumber], (string)config.Authentication[SnowflakeConstants.KeyName.Role], (string)config.Authentication[SnowflakeConstants.KeyName.Warehouse], (string)config.Authentication[SnowflakeConstants.KeyName.Username], (string)config.Authentication[SnowflakeConstants.KeyName.Password], (string)config.Authentication[SnowflakeConstants.KeyName.DatabaseName], (string)config.Authentication[SnowflakeConstants.KeyName.Schema]);

                conn.ConnectionString = connectionString;
                await conn.OpenAsync();
                var cmd = conn.CreateCommand();

                cmd.CommandText = commandText;

                if (param != null)
                    cmd.Parameters.AddRange(param.ToArray());

                await cmd.ExecuteNonQueryAsync();
                await conn.CloseAsync();
            }
        }

        public async Task<SnowflakeDbConnection> GetConnection(IDictionary<string, object> config)
        {
            try
            {
                string connectionString = string.Format("scheme=https;ACCOUNT={0};HOST={1};port={2};ROLE={3};WAREHOUSE={4};USER={5};PASSWORD={6};DB={7};SCHEMA={8}", (string)config[SnowflakeConstants.KeyName.Account], (string)config[SnowflakeConstants.KeyName.Host], (string)config[SnowflakeConstants.KeyName.PortNumber], (string)config[SnowflakeConstants.KeyName.Role], (string)config[SnowflakeConstants.KeyName.Warehouse], (string)config[SnowflakeConstants.KeyName.Username], (string)config[SnowflakeConstants.KeyName.Password], (string)config[SnowflakeConstants.KeyName.DatabaseName], (string)config[SnowflakeConstants.KeyName.Schema]);

                var result = new SnowflakeDbConnection();

                result.ConnectionString = connectionString;

                result.Open();

                return await Task.FromResult(result);
            }
            catch (Exception e)
            {
                throw e;
            }
        }

        public async Task<DataTable> GetTables(IDictionary<string, object> config, string name = null)
        {
            try
            {
                using (var conn = new SnowflakeDbConnection())
                {

                    string connectionString = string.Format("scheme=https;ACCOUNT={0};HOST={1};port={2};ROLE={3};WAREHOUSE={4};USER={5};PASSWORD={6};DB={7};SCHEMA={8}", (string)config[SnowflakeConstants.KeyName.Account], (string)config[SnowflakeConstants.KeyName.Host], (string)config[SnowflakeConstants.KeyName.PortNumber], (string)config[SnowflakeConstants.KeyName.Role], (string)config[SnowflakeConstants.KeyName.Warehouse], (string)config[SnowflakeConstants.KeyName.Username], (string)config[SnowflakeConstants.KeyName.Password], (string)config[SnowflakeConstants.KeyName.DatabaseName], (string)config[SnowflakeConstants.KeyName.Schema]);
                    conn.ConnectionString = connectionString;
                   
                    await conn.OpenAsync();
                    var cmd = conn.CreateCommand();
                    cmd.CommandText = "select * from " + name;
                    var reader = await cmd.ExecuteReaderAsync();
                    var dataTable = new DataTable();
                    dataTable.Load(reader);
                    await conn.CloseAsync();
                    return dataTable;
                }
            }
            catch (Exception)
            {
                
                return new DataTable();
            }
        }

        public async Task<DataTable> GetTableColumns(IDictionary<string, object> config, string tableName)
        {
            try
            {
                using (var conn = new SnowflakeDbConnection())
                {
                    string connectionString = string.Format("scheme=https;ACCOUNT={0};HOST={1};port={2};ROLE={3};WAREHOUSE={4};USER={5};PASSWORD={6};DB={7};SCHEMA={8}", (string)config[SnowflakeConstants.KeyName.Account], (string)config[SnowflakeConstants.KeyName.Host], (string)config[SnowflakeConstants.KeyName.PortNumber], (string)config[SnowflakeConstants.KeyName.Role], (string)config[SnowflakeConstants.KeyName.Warehouse], (string)config[SnowflakeConstants.KeyName.Username], (string)config[SnowflakeConstants.KeyName.Password], (string)config[SnowflakeConstants.KeyName.DatabaseName], (string)config[SnowflakeConstants.KeyName.Schema]);
                    conn.ConnectionString = connectionString;

                    conn.Open();
                    var cmd = conn.CreateCommand();
                    cmd.CommandText = "select * from " + tableName;
                    var reader = cmd.ExecuteReader();
                    var dataTable = new DataTable();
                    dataTable.Load(reader);
                    conn.Close();
                    return await Task.FromResult(dataTable);
                }
            }
            catch (Exception)
            {
             
                return new DataTable();
            }
        }
    }
}
