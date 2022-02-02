using CluedIn.Connector.Common.Caching;
using CluedIn.Connector.Common.Connectors;
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
                var connection = await GetAuthenticationDetails(executionContext, providerDefinitionId);
                await _snowflakeClient.CreateContainer(new SnowflakeConnectionData(connection.Authentication), model);
            }
            catch (Exception e)
            {
                var message = $"Could not create Container {model.Name} for Connector {providerDefinitionId}";
                _logger.LogError(e, message);
                throw;
            }
        }

        public override async Task EmptyContainer(ExecutionContext executionContext, Guid providerDefinitionId, string id)
        {
            try
            {
                var connection = await GetAuthenticationDetails(executionContext, providerDefinitionId);
                await _snowflakeClient.EmptyContainer(new SnowflakeConnectionData(connection.Authentication, id), id);
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
                             let type = VocabularyKeyDataType.Text
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
                var newName = await GetValidContainerName(executionContext, providerDefinitionId, $"{id}{DateTime.Now:yyyyMMddHHmmss}");
                await RenameContainer(executionContext, providerDefinitionId, id, newName);

            }
            catch (Exception e)
            {
                var message = $"Could not archive Container {id}";
                _logger.LogError(e, message);
                throw;
            }
        }

        public override async Task RenameContainer(ExecutionContext executionContext, Guid providerDefinitionId, string oldName, string newName)
        {
            try
            {
                var connection = await GetAuthenticationDetails(executionContext, providerDefinitionId);
                await _snowflakeClient.RenameContainer(new SnowflakeConnectionData(connection.Authentication, oldName), oldName, newName);
            }
            catch (Exception e)
            {
                var message = $"Could not rename Container {oldName}";
                _logger.LogError(e, message);
                throw;
            }
        }

        public override async Task RemoveContainer(ExecutionContext executionContext, Guid providerDefinitionId, string id)
        {
            try
            {
                var connection = await GetAuthenticationDetails(executionContext, providerDefinitionId);
                await _snowflakeClient.RemoveContainer(new SnowflakeConnectionData(connection.Authentication, id), id);
            }
            catch (Exception e)
            {
                var message = $"Could not remove Container {id}";
                _logger.LogError(e, message);
                throw;
            }
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
                    var content = group.SelectMany(g => g.Key);

                    _snowflakeClient.SaveData(configuration, content).GetAwaiter().GetResult();
                    _cachingService.Clear(configuration).GetAwaiter().GetResult();
                }
            }
        }
    }
}
