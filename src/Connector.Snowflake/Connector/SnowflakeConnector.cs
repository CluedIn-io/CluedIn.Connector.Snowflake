using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using CluedIn.Core;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data.Vocabularies;
using CluedIn.Core.DataStore;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Snowflake.Data.Client;

namespace CluedIn.Connector.Snowflake.Connector
{
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Security",
        "CA2100:Review SQL queries for security vulnerabilities", Justification = "<Pending>")]
    public class SnowflakeConnector : ConnectorBase
    {
        private readonly ILogger<SnowflakeConnector> _logger;
        private readonly ISqlClient _client;

        public SnowflakeConnector(IConfigurationRepository repo, ILogger<SnowflakeConnector> logger,
            ISqlClient client) : base(repo)
        {
            ProviderId = SnowflakeConstants.ProviderId;
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _client = client ?? throw new ArgumentNullException(nameof(client));
        }

        public override async Task CreateContainer(ExecutionContext executionContext, Guid providerDefinitionId,
            CreateContainerModel model)
        {
            try
            {
                var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
                string databaseName = (string)config.Authentication[SnowflakeConstants.KeyName.DatabaseName];
                var sql = BuildCreateContainerSql(model, databaseName);

                _logger.LogDebug($"Snowflake Connector - Create Container - Generated query: {sql}");

                await _client.ExecuteCommandAsync(config, sql);
            }
            catch (Exception e)
            {
                var message = $"Could not create Container {model.Name} for Connector {providerDefinitionId}";
                _logger.LogError(e, message);
                //throw new CreateContainerException(message);
            }
        }

        public string BuildCreateContainerSql(CreateContainerModel model, string databaseName)
        {
            var builder = new StringBuilder();
            builder.AppendLine($"CREATE TABLE {Sanitize(model.Name)}Codes (");
            builder.AppendLine("OriginEntityCode varchar,");
            builder.AppendLine("Code varchar");

            builder.AppendLine(");");

            var sql = builder.ToString();
            return sql;
        }

        public override async Task EmptyContainer(ExecutionContext executionContext, Guid providerDefinitionId,
            string id)
        {
            try
            {
                var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);

                var sql = BuildEmptyContainerSql(id);

                _logger.LogDebug($"Snowflake Connector - Empty Container - Generated query: {sql}");

                await _client.ExecuteCommandAsync(config, sql);
            }
            catch (Exception e)
            {
                var message = $"Could not empty Container {id}";
                _logger.LogError(e, message);

                // throw new EmptyContainerException(message);
            }
        }

        public string BuildEmptyContainerSql(string id)
        {
            var builder = new StringBuilder();
            builder.AppendLine($"TRUNCATE TABLE {Sanitize(id)}Codes");
            var sql = builder.ToString();
            return sql;
        }

        private string Sanitize(string str)
        {
            return
                str.Replace("--", "").Replace(";", "")
                    .Replace("'",
                        ""); // Bare-bones sanitization to prevent Sql Injection. Extra info here http://sommarskog.se/dynamic_sql.html
        }

        public override Task<string> GetValidDataTypeName(ExecutionContext executionContext, Guid providerDefinitionId,
            string name)
        {
            // Strip non-alpha numeric characters
            var result = Regex.Replace(name, @"[^A-Za-z0-9]+", "");

            return Task.FromResult(result);
        }

        public override async Task<string> GetValidContainerName(ExecutionContext executionContext,
            Guid providerDefinitionId, string name)
        {
            // Strip non-alpha numeric characters
            var result = Regex.Replace(name, @"[^A-Za-z0-9]+", "");

            // Check if exists
            if (await CheckTableExists(executionContext, providerDefinitionId, result))
            {
                // If exists, append count like in windows explorer
                var count = 0;
                string newName;
                do
                {
                    count++;
                    newName = $"{result}{count}";
                } while (await CheckTableExists(executionContext, providerDefinitionId, newName));

                result = newName;
            }

            // return new name
            return result;
        }

        private async Task<bool> CheckTableExists(ExecutionContext executionContext, Guid providerDefinitionId,
            string name)
        {
            try
            {
                var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
                var tables = await _client.GetTables(config.Authentication, $"{name}Codes");

                return tables.Rows.Count > 0;
            }
            catch (Exception e)
            {
                var message = $"Error checking Container '{name}' exists for Connector {providerDefinitionId}";
                _logger.LogError(e, message);
                //throw new ConnectionException(message);
                return await Task.FromResult(false);
            }
        }

        public override async Task<IEnumerable<IConnectorContainer>> GetContainers(ExecutionContext executionContext,
            Guid providerDefinitionId)
        {
            try
            {
                var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
                var tables = await _client.GetTables(config.Authentication);

                var result = from DataRow row in tables.Rows
                    select row["TABLE_NAME"] as string
                    into tableName
                    select new SnowflakeConnectorContainer {Id = tableName, Name = tableName};

                return result.ToList();
            }
            catch (Exception e)
            {
                var message = $"Could not get Containers for Connector {providerDefinitionId}";
                _logger.LogError(e, message);
                //throw new GetContainersException(message);
                return await Task.FromResult(new List<IConnectorContainer>());
            }
        }

        public override async Task<IEnumerable<IConnectionDataType>> GetDataTypes(ExecutionContext executionContext,
            Guid providerDefinitionId, string containerId)
        {
            try
            {
                var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
                var tables = await _client.GetTableColumns(config.Authentication, containerId);

                var result = from DataRow row in tables.Rows
                    let name = row["COLUMN_NAME"] as string
                    let rawType = row["DATA_TYPE"] as string
                    let type = GetVocabType(rawType)
                    select new SnowflakeConnectorDataType {Name = name, RawDataType = rawType, Type = type};

                return result.ToList();
            }
            catch (Exception e)
            {
                var message =
                    $"Could not get Data types for Container '{containerId}' for Connector {providerDefinitionId}";
                _logger.LogError(e, message);
                // throw new GetDataTypesException(message);
                return await Task.FromResult(new List<IConnectionDataType>());
            }
        }

        private VocabularyKeyDataType GetVocabType(string rawType)
        {
            //return rawType.ToLower() switch
            //{
            //    "bigint" => VocabularyKeyDataType.Integer,
            //    "int" => VocabularyKeyDataType.Integer,
            //    "smallint" => VocabularyKeyDataType.Integer,
            //    "boolean" => VocabularyKeyDataType.Boolean,
            //    "decimal" => VocabularyKeyDataType.Number,
            //    "numeric" => VocabularyKeyDataType.Number,
            //    "float" => VocabularyKeyDataType.Number,
            //    "real" => VocabularyKeyDataType.Number,
            //    "datetime" => VocabularyKeyDataType.DateTime,
            //    "date" => VocabularyKeyDataType.DateTime,
            //    "time" => VocabularyKeyDataType.Time,
            //    "char" => VocabularyKeyDataType.Text,
            //    "varchar" => VocabularyKeyDataType.Text,
            //    "text" => VocabularyKeyDataType.Text,
            //    "binary" => VocabularyKeyDataType.Text,
            //    "varbinary" => VocabularyKeyDataType.Text,
            //    "timestamp" => VocabularyKeyDataType.Text,
            //    "geography" => VocabularyKeyDataType.GeographyLocation, _ => VocabularyKeyDataType.Text
            //};

            return VocabularyKeyDataType.Text;
        }

        private string GetDbType(VocabularyKeyDataType type)
        {
            //return type switch
            //{
            //    VocabularyKeyDataType.Integer => "bigint",
            //    VocabularyKeyDataType.Number => "decimal(18,4)",
            //    VocabularyKeyDataType.Money => "money",
            //    VocabularyKeyDataType.DateTime => "datetime",
            //    VocabularyKeyDataType.Time => "time",
            //    VocabularyKeyDataType.Xml => "XML",
            //    VocabularyKeyDataType.Guid => "varchar",
            //    VocabularyKeyDataType.GeographyLocation => "geography", _ => "varchar"
            //};

            return "varchar";
        }

        public override async Task<bool> VerifyConnection(ExecutionContext executionContext, Guid providerDefinitionId)
        {
            var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
            return await VerifyConnection(executionContext, config.Authentication);
        }

        public override async Task<bool> VerifyConnection(ExecutionContext executionContext,
            IDictionary<string, object> config)
        {
            try
            {
                using (var conn = new SnowflakeDbConnection())
                {
                    string connectionString = string.Format(
                        "scheme=https;ACCOUNT={0};HOST={1};port={2};ROLE={3};WAREHOUSE={4};USER={5};PASSWORD={6};DB={7};SCHEMA={8}",
                        (string)config[SnowflakeConstants.KeyName.Account],
                        (string)config[SnowflakeConstants.KeyName.Host],
                        (string)config[SnowflakeConstants.KeyName.PortNumber],
                        (string)config[SnowflakeConstants.KeyName.Role],
                        (string)config[SnowflakeConstants.KeyName.Warehouse],
                        (string)config[SnowflakeConstants.KeyName.Username],
                        (string)config[SnowflakeConstants.KeyName.Password],
                        (string)config[SnowflakeConstants.KeyName.DatabaseName],
                        (string)config[SnowflakeConstants.KeyName.Schema]);
                    conn.ConnectionString = connectionString;
                    conn.Open();
                    var cmd = conn.CreateCommand();
                    var result = await Task.FromResult(conn.State == ConnectionState.Open);
                    conn.Close();
                    return result;
                }
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error verifying connection");
                // throw new ConnectionException();
                return await Task.FromResult(false);
            }
        }

        public override async Task StoreData(ExecutionContext executionContext, Guid providerDefinitionId,
            string containerName, IDictionary<string, object> data)
        {
            await BulkTableUpdate(
                executionContext,
                providerDefinitionId,
                containerName,
                data,
                16000,
                _client,
                () => base.GetAuthenticationDetails(executionContext, providerDefinitionId),
                _logger);

        }

        public override async Task StoreEdgeData(ExecutionContext executionContext, Guid providerDefinitionId,
            string containerName, string originEntityCode, IEnumerable<string> edges)
        {

            await BulkEdgeTableUpdate(
                executionContext,
                providerDefinitionId,
                containerName,
                edges,
                16000,
                _client,
                () => base.GetAuthenticationDetails(executionContext, providerDefinitionId),
                _logger);

            //try
            //{
            //    var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
            //    string databaseName = (string)config.Authentication[SnowflakeConstants.KeyName.DatabaseName];
            //    var sql = BuildEdgeStoreData(containerName, originEntityCode, edges, out var param);

            //    _logger.LogDebug($"Snowflake Connector - Store Edge Data - Generated query: {sql}");

            //    await _client.ExecuteCommandAsync(config, sql, param);
            //}
            //catch (Exception e)
            //{
            //    var message =
            //        $"Could not store data into Container '{containerName}' for Connector {providerDefinitionId}";
            //    _logger.LogError(e, message);
            //}
        }

        public SnowflakeConnectorCommand ComposeInsert(string tableName, DataTable dataTable)
        {
            var columns = new List<string>();
            var parameters = new List<SqlParameter>();
            List<List<string>> fields = new List<List<string>>();


            foreach (DataColumn dataColumn in dataTable.Columns)
            {
                columns.Add($"{dataColumn.ColumnName}");
            }

            foreach (DataRow entry in dataTable.Rows)
            {
                List<string> listOfValues = new List<string>();
                listOfValues.Add($"'{entry["OriginEntityCode"]}'");
                listOfValues.Add($"'{entry["Code"]}'");
                fields.Add(listOfValues);
            }

            List<string> insertValues = new List<string>();
            int loopedCount = 0;
            foreach (var valuepair in fields)
            {
                var totalCount = fields.Count;
                loopedCount++;

                string jointValues = string.Join(',', valuepair);
                string insertValue = "";

                if (loopedCount == totalCount)
                {
                    insertValue = $"({jointValues});";
                }
                else
                {
                    insertValue = $"({jointValues}),";
                }

                insertValues.Add(insertValue);
            }

            var sqlBuilder = new StringBuilder($"INSERT INTO {tableName}Codes VALUES");
            foreach (var insertValue in insertValues)
            {
                sqlBuilder.Append(insertValue);
            }

            return new SnowflakeConnectorCommand()
            {
                Text = sqlBuilder.ToString(),
                Parameters = parameters
            };
        }

        public SnowflakeConnectorCommand ComposeEdgeInsert(string tableName, DataTable dataTable)
        {
            var columns = new List<string>();
            var parameters = new List<SqlParameter>();
            List<List<string>> fields = new List<List<string>>();


            foreach (DataColumn dataColumn in dataTable.Columns)
            {
                columns.Add($"{dataColumn.ColumnName}");
            }

            foreach (DataRow entry in dataTable.Rows)
            {
                List<string> listOfValues = new List<string>();
                listOfValues.Add($"'{entry["Edge"]}'");
                listOfValues.Add($"'{entry["Code"]}'");
                fields.Add(listOfValues);
            }

            List<string> insertValues = new List<string>();
            int loopedCount = 0;
            foreach (var valuepair in fields)
            {
                var totalCount = fields.Count;
                loopedCount++;

                string jointValues = string.Join(',', valuepair);
                string insertValue = "";

                if (loopedCount == totalCount)
                {
                    insertValue = $"({jointValues});";
                }
                else
                {
                    insertValue = $"({jointValues}),";
                }

                insertValues.Add(insertValue);
            }

            var sqlBuilder = new StringBuilder($"INSERT INTO {tableName}Edges VALUES");
            foreach (var insertValue in insertValues)
            {
                sqlBuilder.Append(insertValue);
            }

            return new SnowflakeConnectorCommand()
            {
                Text = sqlBuilder.ToString(),
                Parameters = parameters
            };
        }

        public string BuildEdgeStoreData(string containerName, string originEntityCode, IEnumerable<string> edges,
            out List<SqlParameter> param)
        {
            var originParam = new SqlParameter {ParameterName = "@OriginEntityCode", Value = originEntityCode};
            param = new List<SqlParameter> {originParam};

            var builder = new StringBuilder();
            var edgeValues = new List<string>();
            foreach (var edge in edges)
            {
                var edgeParam = new SqlParameter {ParameterName = $"@{edgeValues.Count}", Value = edge};
                param.Add(edgeParam);
                edgeValues.Add($"(@OriginEntityCode, {edgeParam.ParameterName})");
            }

            if (edgeValues.Count > 0)
            {
                builder.AppendLine($"INSERT INTO {Sanitize(containerName)}Edges (OriginEntityCode, Edge) values");
                builder.AppendJoin(", ", edgeValues);
            }

            return builder.ToString();
        }

        private object GetDbCompatibleValue(object o)
        {
            try
            {
                var t = new SqlParameter() {ParameterName = "dummy", Value = o}.DbType;
                return o;
            }
            catch
            {
                return JsonUtility.Serialize(o);
            }
        }

        public override async Task ArchiveContainer(ExecutionContext executionContext, Guid providerDefinitionId,
            string id)
        {
            try
            {
                var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);

                var newName = await GetValidContainerName(executionContext, providerDefinitionId,
                    $"{id}{DateTime.Now:yyyyMMddHHmmss}");
                string databaseName = (string)config.Authentication[SnowflakeConstants.KeyName.DatabaseName];
                var sql = BuildRenameContainerSql(id, newName, databaseName, out var param);

                _logger.LogDebug($"Snowflake Connector - Archive Container - Generated query: {sql}");

                await _client.ExecuteCommandAsync(config, sql, param);
            }
            catch (Exception e)
            {
                var message = $"Could not archive Container {id}";
                _logger.LogError(e, message);

                // throw new EmptyContainerException(message);
            }
        }

        private string BuildRenameContainerSql(string id, string newName, string databaseName,
            out List<SqlParameter> param)
        {
            var result = $"ALTER TABLE IF EXISTS {Sanitize(id)}Edges RENAME TO {Sanitize(newName)}Edges";

            param = new List<SqlParameter>
            {
                new SqlParameter("@tableName", SqlDbType.NVarChar) {Value = Sanitize(id)},
                new SqlParameter("@newName", SqlDbType.NVarChar) {Value = Sanitize(newName)}
            };

            return result;
        }

        private string BuildRemoveContainerSql(string id, string databaseName)
        {
            var result = $"DROP TABLE {Sanitize(id)}Edges";

            return result;
        }

        public override async Task RenameContainer(ExecutionContext executionContext, Guid providerDefinitionId,
            string id, string newName)
        {
            try
            {
                var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);

                var tempName = Sanitize(newName);

                string databaseName = (string)config.Authentication[SnowflakeConstants.KeyName.DatabaseName];

                var sql = BuildRenameContainerSql(id, tempName, databaseName, out var param);

                _logger.LogDebug($"Snowflake Connector - Rename Container - Generated query: {sql}");

                await _client.ExecuteCommandAsync(config, sql, param);
            }
            catch (Exception e)
            {
                var message = $"Could not rename Container {id}";
                _logger.LogError(e, message);

                //throw new EmptyContainerException(message);
            }
        }

        public override async Task RemoveContainer(ExecutionContext executionContext, Guid providerDefinitionId,
            string id)
        {
            try
            {
                var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
                string databaseName = (string)config.Authentication[SnowflakeConstants.KeyName.DatabaseName];
                var sql = BuildRemoveContainerSql(id, databaseName);

                _logger.LogDebug($"Snowflake Connector - Remove Container - Generated query: {sql}");

                await _client.ExecuteCommandAsync(config, sql);
            }
            catch (Exception e)
            {
                var message = $"Could not remove Container {id}";
                _logger.LogError(e, message);

                // throw new EmptyContainerException(message);
            }
        }

        public class SnowflakeConnectorCommand
        {
            public string Text { get; set; }

            public IList<SqlParameter> Parameters { get; set; }
        }

        private DataTable GetDataTable(ExecutionContext executionContext, string containerName, IDictionary<string, object> data)
        {
            var dataTableCacheName = GetDataTableCacheName(containerName);

            return executionContext.ApplicationContext.System.Cache.GetItem(dataTableCacheName, () =>
            {
                var table = new DataTable(containerName);
                table.Columns.Add("OriginEntityCode");
                table.Columns.Add("Code");
                return table;
            });
        }

        private DataTable GetEdgeDataTable(ExecutionContext executionContext, string containerName, IEnumerable<string> edges)
        {
            var dataTableCacheName = GetDataTableCacheName(containerName);

            return executionContext.ApplicationContext.System.Cache.GetItem(dataTableCacheName, () =>
            {
                var table = new DataTable(containerName);
                table.Columns.Add("Edge");
                table.Columns.Add("Code");
                return table;
            });
        }

        private static string GetDataTableCacheName(string containerName)
        {
            return $"Stream_cache_{containerName}";
        }

        public virtual async Task BulkTableUpdate(
            ExecutionContext executionContext,
            Guid providerDefinitionId,
            string containerName,
            IDictionary<string, object> data,
            int threshold,
            ISqlClient client,
            Func<Task<IConnectorConnection>> connectionFactory,
            ILogger logger)
        {
            var table = GetDataTable(executionContext, containerName, data);
            CacheDataTableRow(data, table);

            if(table.Rows.Count >= threshold)
            {
                await FlushTable(executionContext, connectionFactory, client, containerName, table, logger);
            }
        }

        public virtual async Task BulkEdgeTableUpdate(
            ExecutionContext executionContext,
            Guid providerDefinitionId,
            string containerName,
            IEnumerable<string> edges,
            int threshold,
            ISqlClient client,
            Func<Task<IConnectorConnection>> connectionFactory,
            ILogger logger)
        {
            var table = GetEdgeDataTable(executionContext, containerName, edges);
            CacheEdgeTableRow(edges, table);

            if (table.Rows.Count >= threshold)
            {
                await FlushTable(executionContext, connectionFactory, client, containerName, table, logger);
            }
        }

        private static void CacheDataTableRow(IDictionary<string, object> data, DataTable table)
        {
            var row = table.NewRow();
            if (data.TryGetValue("Codes", out var codes) && codes is IEnumerable codesEnumerable)
            {
                var enumerator = codesEnumerable.GetEnumerator();
                while (enumerator.MoveNext())
                {
                    row["OriginEntityCode"] = data["OriginEntityCode"];
                    row["Code"] = enumerator.Current;
                }
            }

            table.Rows.Add(row);
        }

        private static void CacheEdgeTableRow(IEnumerable<string> edges, DataTable table)
        {
            var row = table.NewRow();

            var enumerator = edges.GetEnumerator();
            while (enumerator.MoveNext())
            {
                row["Edge"] = edges;
                row["Code"] = enumerator.Current;
            }

            table.Rows.Add(row);
        }

        private async Task FlushTable(
            ExecutionContext executionContext,
            Func<Task<IConnectorConnection>> connectionFactory,
            ISqlClient client,
            string containerName,
            DataTable table,
            ILogger logger)
        {
            var dataTableCacheName = GetDataTableCacheName(containerName);
            executionContext.ApplicationContext.System.Cache.RemoveItem(dataTableCacheName);

            var connection = await connectionFactory();

            var sw = new Stopwatch();
            sw.Start();

            var command = ComposeInsert(containerName, table);

            await client.ExecuteCommandAsync(connection, command.Text);
            logger.LogDebug($"Stream StoreData BulkInsert {table.Rows.Count} rows - {sw.ElapsedMilliseconds}ms");
        }
    }
}
