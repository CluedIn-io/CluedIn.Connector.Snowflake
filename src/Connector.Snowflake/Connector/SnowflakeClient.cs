using CluedIn.Connector.Common.Clients;
using CluedIn.Connector.Common.Helpers;
using CluedIn.Core.Connectors;
using Newtonsoft.Json;
using Serilog;
using Snowflake.Data.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using SFDataType = Snowflake.Data.Core.SFDataType;

[assembly: InternalsVisibleTo("CluedIn.Connector.Snowflake.Unit.Tests")]
namespace CluedIn.Connector.Snowflake.Connector
{
    public class SnowflakeClient : ClientBase<SnowflakeDbConnection, SnowflakeDbParameter>, ISnowflakeClient
    {
        public override async Task<SnowflakeDbConnection> GetConnection(IDictionary<string, object> config)
        {
            var connectionData = new SnowflakeConnectionData(config);
            return await GetConnection(connectionData);
        }

        public async Task<SnowflakeDbConnection> GetConnection(SnowflakeConnectionData configuration)
        {
            var connectionString = BuildConnectionString(configuration);
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
            var connectionData = new SnowflakeConnectionData(config);
            return BuildConnectionString(connectionData);
        }

        public string BuildConnectionString(SnowflakeConnectionData cfg)
        {
            return $"scheme=https;ACCOUNT={cfg.Account};HOST={cfg.Host};port={cfg.PortNumber};ROLE={cfg.Role};WAREHOUSE={cfg.Warehouse};USER={cfg.Username};PASSWORD={cfg.Password};DB={cfg.DatabaseName};SCHEMA={cfg.Schema}";
        }

        public async Task CreateContainer(SnowflakeConnectionData configuration, CreateContainerModel model)
        {
            var sql = BuildCreateContainerSql(model);
            await ExecuteCommandAsync(configuration, sql);
        }

        public async Task EmptyContainer(SnowflakeConnectionData configuration, string containerName)
        {
            var sql = "TRUNCATE TABLE [" + SqlStringSanitizer.Sanitize(containerName) + "]";
            await ExecuteCommandAsync(configuration, sql);
        }

        public async Task RenameContainer(SnowflakeConnectionData configuration, string oldName, string newName)
        {
            var sql = $"ALTER TABLE IF EXISTS {SqlStringSanitizer.Sanitize(oldName)} RENAME TO {SqlStringSanitizer.Sanitize(newName)}";
            await ExecuteCommandAsync(configuration, sql);
        }

        public async Task RemoveContainer(SnowflakeConnectionData configuration, string containerName)
        {
            var sql = $"DROP TABLE {SqlStringSanitizer.Sanitize(containerName)}";
            await ExecuteCommandAsync(configuration, sql);
        }

        public async Task SaveData(SnowflakeConnectionData configuration, IList<IDictionary<string, object>> content)
        {
            var sql = BuildStoreDataSql(configuration.ContainerName, content, out var param);
            await AlternateExecuteCommandAsync(configuration, sql, param);
        }

        private async Task AlternateExecuteCommandAsync(SnowflakeConnectionData configuration, string sql, IList<SnowflakeDbParameter> parameters)
        {
            Log.Information("SnowflakeClient.AlternateExecuteCommandAsync: entry");
            using var connection = await GetConnection(configuration);
            using var command = connection.CreateCommand();
            try
            {
                command.CommandText = sql;
                command.Parameters.AddRange(parameters.ToArray());
                await command.ExecuteNonQueryAsync();
                Log.Information("SnowflakeClient.AlternateExecuteCommandAsync: exit");
            }
            catch (Exception ex)
            {
                Log.Error(ex, $"SnowflakeClient.AlternateExecuteCommandAsync failed. Sql:\n {sql} \n with parameters:\n {JsonConvert.SerializeObject(parameters.Select(p => new KeyValuePair<string, object>(p.ParameterName, p.Value)))}\n with configuration: {JsonConvert.SerializeObject(configuration)}");
                Log.Error($"SnowflakeClient.AlternateExecuteCommandAsync failed. Exception: {JsonConvert.SerializeObject(ex)}");
            }
        }

        internal string BuildCreateContainerSql(CreateContainerModel model)
        {
            var sqlBuilder = new StringBuilder();
            sqlBuilder.AppendLine($"CREATE TABLE IF NOT EXISTS {SqlStringSanitizer.Sanitize(model.Name)} (");
            var columnsCount = model.DataTypes.Count;
            for (var index = 0; index < columnsCount; index++)
            {
                sqlBuilder.AppendLine($"{SqlStringSanitizer.Sanitize(model.DataTypes[index].Name)} varchar NULL{(index < columnsCount - 1 ? "," : "")}");
            }

            sqlBuilder.AppendLine(");");

            return sqlBuilder.ToString();
        }

        internal string BuildStoreDataSql(string containerName, IList<IDictionary<string, object>> data, out IList<SnowflakeDbParameter> parameters)
        {
            var builder = new StringBuilder();
            var sampleData = data.First();
            var rawColumnNames = sampleData.Keys;
            var columnNames = rawColumnNames.Select(n => SqlStringSanitizer.Sanitize(n));
            var fieldList = string.Join(", ", columnNames.Select(n => $"{n}"));
            var insertList = string.Join(", ", columnNames.Select(n => $"source.{n}"));
            var updateList = string.Join(", ", columnNames.Select(n => $"target.{n} = source.{n}"));

            parameters = new List<SnowflakeDbParameter>();
            var paramNamesBuilder = new StringBuilder();
            for (var index = 0; index < data.Count(); index++)
            {
                if (index != 0)
                    paramNamesBuilder.Append(",");

                paramNamesBuilder
                    .Append("(")
                    .Append(string.Join(", ", columnNames.Select(columnName => $":{columnName}{index}")))
                    .Append(")");

                var entry = data[index];
                foreach (var columnName in rawColumnNames)
                {
                    var valueExist = entry.TryGetValue(columnName, out var value);
                    var snowflakeParameter = new SnowflakeDbParameter($"{SqlStringSanitizer.Sanitize(columnName)}{index}", SFDataType.TEXT)
                    {
                        Value = valueExist ? JsonConvert.SerializeObject(value) : null
                    };
                    parameters.Add(snowflakeParameter);
                }
            }

            builder
                .AppendLine($"MERGE INTO {SqlStringSanitizer.Sanitize(containerName)} AS target")
                .AppendLine($"USING (SELECT * FROM VALUES ({paramNamesBuilder})) AS source ({fieldList})")
                .AppendLine("  ON (target.OriginEntityCode = source.OriginEntityCode)")
                .AppendLine("WHEN MATCHED THEN")
                .AppendLine($"  UPDATE SET {updateList}")
                .AppendLine("WHEN NOT MATCHED THEN")
                .AppendLine($"  INSERT ({fieldList})")
                .AppendLine($"  VALUES ({insertList});");

            return builder.ToString();
        }
    }
}
