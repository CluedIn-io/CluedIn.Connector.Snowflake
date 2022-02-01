using CluedIn.Connector.Common.Clients;
using CluedIn.Connector.Common.Configurations;
using CluedIn.Connector.Common.Helpers;
using Microsoft.Data.SqlClient;
using Newtonsoft.Json;
using Snowflake.Data.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
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
                (string)config[SnowflakeConstants.Account],
                (string)config[CommonConfigurationNames.Host],
                (string)config[CommonConfigurationNames.PortNumber],
                (string)config[SnowflakeConstants.Role],
                (string)config[SnowflakeConstants.Warehouse],
                (string)config[CommonConfigurationNames.Username],
                (string)config[CommonConfigurationNames.Password],
                (string)config[CommonConfigurationNames.DatabaseName],
                (string)config[CommonConfigurationNames.Schema]);

            return connectionString;
        }

        public async Task SaveData(SnowflakeConnectionData configuration, string content)
        {
            var data = JsonConvert.DeserializeObject<IDictionary<string, object>>(content);
            var sql = BuildStoreDataSql(configuration.ContainerName, data, out var param);

            await ExecuteCommandAsync(configuration, sql, param);
        }

        private string BuildStoreDataSql(string containerName, IDictionary<string, object> data, out List<SqlParameter> parameters)
        {
            var builder = new StringBuilder();
            var nameList = data.Select(n => n.Key).ToList();
            var valueList = data.Select(n => n.Value).ToList();

            var fieldList = string.Join(", ", nameList.Select(n => $"{n}"));
            var paramList = string.Join(", ", valueList.Select(n => $"'{n}'"));
            var insertList = string.Join(", ", nameList.Select(n => $"source.{n}"));
            var updateList = string.Join(", ", nameList.Select(n => $"target.{n} = source.{n}"));

            builder.AppendLine($"MERGE INTO {SqlStringSanitizer.Sanitize(containerName)} AS target");
            builder.AppendLine($"USING (SELECT {paramList}) AS source ({fieldList})");
            builder.AppendLine("  ON (target.OriginEntityCode = source.OriginEntityCode)");
            builder.AppendLine("WHEN MATCHED THEN");
            builder.AppendLine($"  UPDATE SET {updateList}");
            builder.AppendLine("WHEN NOT MATCHED THEN");
            builder.AppendLine($"  INSERT ({fieldList})");
            builder.AppendLine($"  VALUES ({insertList});");

            parameters = new List<SqlParameter>();
            foreach (var entry in data)
            {
                var name = SqlStringSanitizer.Sanitize(entry.Key);
                var param = new SqlParameter($"@{name}", entry.Value);
                try
                {
                    var dbType = param.DbType;
                }
                catch (Exception)
                {                    
                    param.Value = JsonConvert.SerializeObject(entry.Value);
                }

                parameters.Add(param);
            }

            return builder.ToString();
        }        
    }
}
