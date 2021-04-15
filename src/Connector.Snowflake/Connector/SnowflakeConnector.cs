using System;
using System.Collections.Generic;
using System.Data;
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
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Security", "CA2100:Review SQL queries for security vulnerabilities", Justification = "<Pending>")]
    public class SnowflakeConnector : ConnectorBase
    {
        private readonly ILogger<SnowflakeConnector> _logger;
        private readonly ISqlClient _client;

        public SnowflakeConnector(IConfigurationRepository repo, ILogger<SnowflakeConnector> logger, ISqlClient client) : base(repo)
        {
            ProviderId = SnowflakeConstants.ProviderId;
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _client = client ?? throw new ArgumentNullException(nameof(client));
        }

        public override async Task CreateContainer(ExecutionContext executionContext, Guid providerDefinitionId, CreateContainerModel model)
        {
            try
            {
                var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);

                var sql = BuildCreateContainerSql(model);

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

        public string BuildCreateContainerSql(CreateContainerModel model)
        {
            var builder = new StringBuilder();
            builder.AppendLine($"CREATE TABLE {Sanitize(model.Name)} (");

            var index = 0;
            var count = model.DataTypes.Count;
            foreach (var type in model.DataTypes)
            {
                builder.AppendLine($"{Sanitize(type.Name)} {GetDbType(type.Type)} NULL{(index < count - 1 ? "," : "")}");

                index++;
            }

            builder.AppendLine(");");

            var sql = builder.ToString();
            return sql;
        }

        public override async Task EmptyContainer(ExecutionContext executionContext, Guid providerDefinitionId, string id)
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
            builder.AppendLine($"TRUNCATE TABLE {Sanitize(id)}");
            var sql = builder.ToString();
            return sql;
        }

        private string Sanitize(string str)
        {
            return str.Replace("--", "").Replace(";", "").Replace("'", "");       // Bare-bones sanitization to prevent Sql Injection. Extra info here http://sommarskog.se/dynamic_sql.html
        }

        public override Task<string> GetValidDataTypeName(ExecutionContext executionContext, Guid providerDefinitionId, string name)
        {
            // Strip non-alpha numeric characters
            var result = Regex.Replace(name, @"[^A-Za-z0-9]+", "");

            return Task.FromResult(result);
        }

        public override async Task<string> GetValidContainerName(ExecutionContext executionContext, Guid providerDefinitionId, string name)
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

        private async Task<bool> CheckTableExists(ExecutionContext executionContext, Guid providerDefinitionId, string name)
        {
            try
            {
                var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
                var tables = await _client.GetTables(config.Authentication, name);

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

        public override async Task<IEnumerable<IConnectorContainer>> GetContainers(ExecutionContext executionContext, Guid providerDefinitionId)
        {
            try
            {
                var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
                var tables = await _client.GetTables(config.Authentication);

                var result = from DataRow row in tables.Rows
                             select row["TABLE_NAME"] as string into tableName
                             select new SnowflakeConnectorContainer { Id = tableName, Name = tableName };

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

        public override async Task<IEnumerable<IConnectionDataType>> GetDataTypes(ExecutionContext executionContext, Guid providerDefinitionId, string containerId)
        {
            try
            {
                var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
                var tables = await _client.GetTableColumns(config.Authentication, containerId);

                var result = from DataRow row in tables.Rows
                             let name = row["COLUMN_NAME"] as string
                             let rawType = row["DATA_TYPE"] as string
                             let type = GetVocabType(rawType)
                             select new SnowflakeConnectorDataType
                             {
                                 Name = name,
                                 RawDataType = rawType,
                                 Type = type
                             };

                return result.ToList();
            }
            catch (Exception e)
            {
                var message = $"Could not get Data types for Container '{containerId}' for Connector {providerDefinitionId}";
                _logger.LogError(e, message);
                // throw new GetDataTypesException(message);
                return await Task.FromResult(new List<IConnectionDataType>());
            }
        }

        private VocabularyKeyDataType GetVocabType(string rawType)
        {
            return rawType.ToLower() switch
            {
                "bigint" => VocabularyKeyDataType.Integer,
                "int" => VocabularyKeyDataType.Integer,
                "smallint" => VocabularyKeyDataType.Integer,
                "boolean" => VocabularyKeyDataType.Boolean,
                "decimal" => VocabularyKeyDataType.Number,
                "numeric" => VocabularyKeyDataType.Number,
                "float" => VocabularyKeyDataType.Number,
                "real" => VocabularyKeyDataType.Number,
                "datetime" => VocabularyKeyDataType.DateTime,
                "date" => VocabularyKeyDataType.DateTime,
                "time" => VocabularyKeyDataType.Time,
                "char" => VocabularyKeyDataType.Text,
                "varchar" => VocabularyKeyDataType.Text,
                "text" => VocabularyKeyDataType.Text,
                "binary" => VocabularyKeyDataType.Text,
                "varbinary" => VocabularyKeyDataType.Text,
                "timestamp" => VocabularyKeyDataType.Text,
                "geography" => VocabularyKeyDataType.GeographyLocation, _ => VocabularyKeyDataType.Text
            };
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

        public override async Task<bool> VerifyConnection(ExecutionContext executionContext, IDictionary<string, object> config)
        {
            try
            {

                using (var conn = new SnowflakeDbConnection())
                {
                    string connectionString = string.Format("scheme=https;ACCOUNT={0};HOST={1};port={2};ROLE={3};WAREHOUSE={4};USER={5};PASSWORD={6};DB={7};SCHEMA={8}", (string)config[SnowflakeConstants.KeyName.Account], (string)config[SnowflakeConstants.KeyName.Host], (string)config[SnowflakeConstants.KeyName.PortNumber], (string)config[SnowflakeConstants.KeyName.Role], (string)config[SnowflakeConstants.KeyName.Warehouse], (string)config[SnowflakeConstants.KeyName.Username], (string)config[SnowflakeConstants.KeyName.Password], (string)config[SnowflakeConstants.KeyName.DatabaseName], (string)config[SnowflakeConstants.KeyName.Schema]);
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

        public override async Task StoreData(ExecutionContext executionContext, Guid providerDefinitionId, string containerName, IDictionary<string, object> data)
        {
            try
            {
                var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);

                var sql = BuildStoreDataSql(containerName, data, out var param);

                _logger.LogDebug($"Snowflake Connector - Store Data - Generated query: {sql}");

                await _client.ExecuteCommandAsync(config, sql, param);
            }
            catch (Exception e)
            {
                var message = $"Could not store data into Container '{containerName}' for Connector {providerDefinitionId}";
                _logger.LogError(e, message);
                //throw new StoreDataException(message);
            }
        }

        public override async Task StoreEdgeData(ExecutionContext executionContext, Guid providerDefinitionId, string containerName, string originEntityCode, IEnumerable<string> edges)
        {
            await Task.CompletedTask;
        }

        public string BuildStoreDataSql(string containerName, IDictionary<string, object> data, out List<SqlParameter> param)
        {
            var builder = new StringBuilder();

            var nameList = data.Select(n => Sanitize(n.Key)).ToList();
            var fieldList = string.Join(", ", nameList.Select(n => $"{n}"));
            var paramList = string.Join(", ", nameList.Select(n => $"@{n}"));
            var insertList = string.Join(", ", nameList.Select(n => $"source.{n}"));
            var updateList = string.Join(", ", nameList.Select(n => $"target.{n} = source.{n}"));

            builder.AppendLine($"MERGE {Sanitize(containerName)}] AS target");
            builder.AppendLine($"USING (SELECT {paramList}) AS source ({fieldList})");
            builder.AppendLine("  ON (target.OriginEntityCode = source.OriginEntityCode)");
            builder.AppendLine("WHEN MATCHED THEN");
            builder.AppendLine($"  UPDATE SET {updateList}");
            builder.AppendLine("WHEN NOT MATCHED THEN");
            builder.AppendLine($"  INSERT ({fieldList})");
            builder.AppendLine($"  VALUES ({insertList});");



            param = (from dataType in data let name = Sanitize(dataType.Key) select new SqlParameter {ParameterName = $"@{name}", Value = dataType.Value ?? ""}).ToList();

            return builder.ToString();
        }

        public override async Task ArchiveContainer(ExecutionContext executionContext, Guid providerDefinitionId, string id)
        {
            try
            {
                var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);

                var newName = await GetValidContainerName(executionContext, providerDefinitionId, $"{id}{DateTime.Now:yyyyMMddHHmmss}");
                var sql = BuildRenameContainerSql(id, newName, out var param);

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

        private string BuildRenameContainerSql(string id, string newName, out List<SqlParameter> param)
        {
            var result = $"ALTER TABLE IF EXISTS @tableName RENAME TO @newName";

            param = new List<SqlParameter>
            {
                new SqlParameter("@tableName", SqlDbType.NVarChar)
                {
                    Value = Sanitize(id)
                },
                new SqlParameter("@newName", SqlDbType.NVarChar)
                {
                    Value = Sanitize(newName)
                }
            };

            return result;
        }

        private string BuildRemoveContainerSql(string id)
        {
            var result = $"DROP TABLE {Sanitize(id)}";

            return result;
        }

        public override async Task RenameContainer(ExecutionContext executionContext, Guid providerDefinitionId, string id, string newName)
        {
            try
            {
                var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);

                var tempName = Sanitize(newName);

                var sql = BuildRenameContainerSql(id, tempName, out var param);

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

        public override async Task RemoveContainer(ExecutionContext executionContext, Guid providerDefinitionId, string id)
        {
            try
            {
                var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);

                var sql = BuildRemoveContainerSql(id);

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
    }
}
