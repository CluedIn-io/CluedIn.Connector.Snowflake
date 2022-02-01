using CluedIn.Connector.Common.Caching;
using CluedIn.Connector.Common.Configurations;
using CluedIn.Connector.Common.Connectors;
using CluedIn.Connector.Common.Helpers;
using CluedIn.Core;
using CluedIn.Core.Configuration;
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
    public class SnowflakeConnector : SqlConnectorBase<SnowflakeConnector, SnowflakeDbConnection, SqlParameter>, IScheduledSyncs
    {
        private readonly ICachingService<IDictionary<string, object>, SnowflakeConnectionData> _cachingService;
        private readonly ISnowflakeClient _snowflakeClient;
        private readonly object _cacheLock = new object();
        private readonly int _cacheRecordsThreshold;

        public SnowflakeConnector(IConfigurationRepository repository,
            ILogger<SnowflakeConnector> logger,
            ISnowflakeClient client,
            ISnowflakeConstants constants,
            ICachingService<IDictionary<string, object>, SnowflakeConnectionData> cachingService)
            : base(repository, logger, client, constants.ProviderId)
        {
            _cachingService = cachingService;
            _snowflakeClient = client;
            _cacheRecordsThreshold = ConfigurationManagerEx.AppSettings.GetValue(constants.CacheRecordsThresholdKeyName, constants.CacheRecordsThresholdDefaultValue);
        }

        public override async Task CreateContainer(ExecutionContext executionContext, Guid providerDefinitionId, CreateContainerModel model)
        {
            try
            {
                var config = await GetAuthenticationDetails(executionContext, providerDefinitionId);
                //databaseName is not used
                var databaseName = (string)config.Authentication[CommonConfigurationNames.DatabaseName];
                var sql = BuildCreateContainerSql(model, databaseName);

                _logger.LogDebug($"Snowflake Connector - Create Container - Generated query: {sql}");

                await _client.ExecuteCommandAsync(config, sql);
            }
            catch (Exception e)
            {
                var message = $"Could not create Container {model.Name} for Connector {providerDefinitionId}";
                _logger.LogError(e, message);
                throw;
            }
        }

        private string BuildCreateContainerSql(CreateContainerModel model, string tableName)
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
            return VocabularyKeyDataType.Text;
        }

        private string GetDbType(VocabularyKeyDataType type)
        {
            return "varchar";
        }

        public override async Task StoreData(ExecutionContext executionContext, Guid providerDefinitionId, string containerName, IDictionary<string, object> data)
        {
            try
            {
                var connection = await GetAuthenticationDetails(executionContext, providerDefinitionId);
                var configurations = new SnowflakeConnectionData(connection.Authentication, containerName);

                lock (_cacheLock)
                {
                    _cachingService.AddItem(data, configurations).GetAwaiter().GetResult();
                }

                if (await _cachingService.Count() >= _cacheRecordsThreshold)
                {
                    Flush();
                }
            }
            catch (Exception e)
            {
                var message = $"Could not store data into Container '{containerName}' for Connector {providerDefinitionId}";
                _logger.LogError(e, message);
                throw;
            }
        }

        public override Task StoreEdgeData(ExecutionContext executionContext, Guid providerDefinitionId, string containerName, string originEntityCode, IEnumerable<string> edges)
        {
            return Task.CompletedTask;
        }

        public override async Task ArchiveContainer(ExecutionContext executionContext, Guid providerDefinitionId, string id)
        {
            try
            {
                var config = await GetAuthenticationDetails(executionContext, providerDefinitionId);

                var newName = await GetValidContainerName(executionContext, providerDefinitionId, $"{id}{DateTime.Now:yyyyMMddHHmmss}");
                var databaseName = (string)config.Authentication[CommonConfigurationNames.DatabaseName];
                var sql = BuildRenameContainerSql(id, newName, out var param);

                _logger.LogDebug($"Snowflake Connector - Archive Container - Generated query: {sql}");

                await _client.ExecuteCommandAsync(config, sql, param);
            }
            catch (Exception e)
            {
                var message = $"Could not archive Container {id}";
                _logger.LogError(e, message);
                throw;
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



        public override async Task RenameContainer(ExecutionContext executionContext, Guid providerDefinitionId, string oldTableName, string newTableName)
        {
            try
            {
                var config = await GetAuthenticationDetails(executionContext, providerDefinitionId);

                var tempName = SqlStringSanitizer.Sanitize(newTableName);

                var databaseName = (string)config.Authentication[CommonConfigurationNames.DatabaseName];

                var sql = BuildRenameContainerSql(oldTableName, tempName, out var param);

                _logger.LogDebug($"Snowflake Connector - Rename Container - Generated query: {sql}");

                await _client.ExecuteCommandAsync(config, sql, param);
            }
            catch (Exception e)
            {
                var message = $"Could not rename Container {oldTableName}";
                _logger.LogError(e, message);
                throw;
            }
        }

        public override async Task RemoveContainer(ExecutionContext executionContext, Guid providerDefinitionId, string id)
        {
            var connection = await GetAuthenticationDetails(executionContext, providerDefinitionId);
            await _snowflakeClient.RemoveContainer(new SnowflakeConnectionData(connection.Authentication), id);
        }

        public async Task Sync()
        {
            if (await _cachingService.Count() == 0)
            {
                return;
            }

            Flush();
        }

        private void Flush()
        {
            lock (_cacheLock)
            {
                var itemsCount = _cachingService.Count().GetAwaiter().GetResult();
                if (itemsCount == 0)
                {
                    return;
                }

                var cachedItems = _cachingService.GetItems().GetAwaiter().GetResult();
                var cachedItemsByConfigurations = cachedItems.GroupBy(pair => pair.Value).ToList();

                foreach (var group in cachedItemsByConfigurations)
                {
                    var configuration = group.Key;
                    var content = JsonUtility.SerializeIndented(group.Select(g => g.Key));

                    _snowflakeClient.SaveData(configuration, content).GetAwaiter().GetResult();
                    _cachingService.Clear(configuration).GetAwaiter().GetResult();
                }
            }
        }
    }
}
