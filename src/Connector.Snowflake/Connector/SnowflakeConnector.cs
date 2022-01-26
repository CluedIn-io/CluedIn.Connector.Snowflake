using CluedIn.Connector.Common.Configurations;
using CluedIn.Connector.Common.Connectors;
using CluedIn.Connector.Common.Helpers;
using CluedIn.Core;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data.Vocabularies;
using CluedIn.Core.DataStore;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Snowflake.Data.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CluedIn.Connector.Snowflake.Connector
{
    public class SnowflakeConnector : SqlConnectorBase<SnowflakeConnector, SnowflakeDbConnection, SqlParameter>
    {
        public SnowflakeConnector(IConfigurationRepository repository, ILogger<SnowflakeConnector> logger, ISnowflakeClient client,
            ISnowflakeConstants constants) : base(repository, logger, client, constants.ProviderId)
        {
        }

        public override async Task CreateContainer(ExecutionContext executionContext, Guid providerDefinitionId, CreateContainerModel model)
        {
            try
            {
                var config = await GetAuthenticationDetails(executionContext, providerDefinitionId);
                //databaseName is not used
                var databaseName = (string)config.Authentication[SnowflakeConstants.KeyName.DatabaseName];
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

        public string BuildCreateContainerSql(CreateContainerModel model, string tableName)
        {
            var builder = new StringBuilder();
            builder.AppendLine($"CREATE TABLE IF NOT EXISTS {SqlStringSanitizer.Sanitize(model.Name)} (");

            var index = 0;
            var count = model.DataTypes.Count;
            foreach (var type in model.DataTypes)
            {
                builder.AppendLine($"{SqlStringSanitizer.Sanitize(type.Name)} {GetDbType(type.Type)} NULL{(index < count - 1 ? "," : "")}");

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
                var config = await GetAuthenticationDetails(executionContext, providerDefinitionId);

                var sql = BuildEmptyContainerSql(id);

                _logger.LogDebug($"Snowflake Connector - Empty Container - Generated query: {sql}");

                await _client.ExecuteCommandAsync(config, sql);
            }
            catch (Exception e)
            {
                var message = $"Could not empty Container {id}";
                _logger.LogError(e, message);
            }
        }

        public override async Task<IEnumerable<IConnectorContainer>> GetContainers(ExecutionContext executionContext, Guid providerDefinitionId)
        {
            try
            {
                var config = await GetAuthenticationDetails(executionContext, providerDefinitionId);
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
                return new List<IConnectorContainer>();
            }
        }

        public override async Task<IEnumerable<IConnectionDataType>> GetDataTypes(ExecutionContext executionContext, Guid providerDefinitionId, string containerId)
        {
            try
            {
                var config = await GetAuthenticationDetails(executionContext, providerDefinitionId);
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
                return new List<IConnectionDataType>();
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

        public override async Task StoreData(ExecutionContext executionContext, Guid providerDefinitionId, string containerName, IDictionary<string, object> data)
        {
            try
            {
                var config = await GetAuthenticationDetails(executionContext, providerDefinitionId);
                var databaseName = (string)config.Authentication[SnowflakeConstants.KeyName.DatabaseName];
                var sql = BuildStoreDataSql(containerName, data, databaseName, out var param);

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

        public override Task StoreEdgeData(ExecutionContext executionContext, Guid providerDefinitionId, string containerName, string originEntityCode, IEnumerable<string> edges)
        {
            return Task.CompletedTask;
        }

        public string BuildStoreDataSql(string containerName, IDictionary<string, object> data, string databaseName, out List<SqlParameter> param)
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



            param = (from dataType in data let name = SqlStringSanitizer.Sanitize(dataType.Key) select new SqlParameter { ParameterName = $"@{name}", Value = GetDbCompatibleValue(dataType.Value ?? "") }).ToList();

            return builder.ToString();
        }

        private object GetDbCompatibleValue(object o)
        {
            try
            {
                var t = new SqlParameter() { ParameterName = "dummy", Value = o }.DbType;
                return o;
            }
            catch
            {
                return JsonUtility.Serialize(o);
            }
        }

        public override async Task ArchiveContainer(ExecutionContext executionContext, Guid providerDefinitionId, string id)
        {
            try
            {
                var config = await GetAuthenticationDetails(executionContext, providerDefinitionId);

                var newName = await GetValidContainerName(executionContext, providerDefinitionId, $"{id}{DateTime.Now:yyyyMMddHHmmss}");
                var databaseName = (string)config.Authentication[SnowflakeConstants.KeyName.DatabaseName];
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

        private string BuildRenameContainerSql(string oldTableName, string newTableName, out List<SqlParameter> param)
        {
            var result = $"ALTER TABLE IF EXISTS {SqlStringSanitizer.Sanitize(oldTableName)} RENAME TO {SqlStringSanitizer.Sanitize(newTableName)}";

            param = new List<SqlParameter>
            {
                new SqlParameter("@tableName", SqlDbType.NVarChar)
                {
                    Value = SqlStringSanitizer.Sanitize(oldTableName)
                },
                new SqlParameter("@newName", SqlDbType.NVarChar)
                {
                    Value = SqlStringSanitizer.Sanitize(newTableName)
                }
            };

            return result;
        }

        private string BuildRemoveContainerSql(string tableName)
        {
            var result = $"DROP TABLE {SqlStringSanitizer.Sanitize(tableName)}";

            return result;
        }

        public override async Task RenameContainer(ExecutionContext executionContext, Guid providerDefinitionId, string oldTableName, string newTableName)
        {
            try
            {
                var config = await GetAuthenticationDetails(executionContext, providerDefinitionId);

                var tempName = SqlStringSanitizer.Sanitize(newTableName);

                var databaseName = (string)config.Authentication[SnowflakeConstants.KeyName.DatabaseName];

                var sql = BuildRenameContainerSql(oldTableName, tempName, out var param);

                _logger.LogDebug($"Snowflake Connector - Rename Container - Generated query: {sql}");

                await _client.ExecuteCommandAsync(config, sql, param);
            }
            catch (Exception e)
            {
                var message = $"Could not rename Container {oldTableName}";
                _logger.LogError(e, message);

                //throw new EmptyContainerException(message);
            }
        }

        public override async Task RemoveContainer(ExecutionContext executionContext, Guid providerDefinitionId, string id)
        {
            try
            {
                var config = await GetAuthenticationDetails(executionContext, providerDefinitionId);
                var databaseName = (string)config.Authentication[SnowflakeConstants.KeyName.DatabaseName];
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
