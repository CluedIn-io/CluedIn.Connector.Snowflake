using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using AutoFixture;
using AutoFixture.Idioms;
using AutoFixture.Xunit2;
using CluedIn.Core.Crawling;
using CluedIn.Core.Webhooks;
using FluentAssertions;
using Moq;
using Xunit;

namespace CluedIn.Connector.Snowflake.Unit.Tests
{
    public class SnowflakeConnectorProviderTests
    {
        private readonly TestContext _testContext;

        public SnowflakeConnectorProviderTests()
        {
            _testContext = new TestContext();
        }

        [Fact]
        public void Ctor_NullContext_Throws()
        {
            Action action = () => new SnowflakeConnectorProvider(null);

            action.Should().Throw<ArgumentNullException>()
                .And.ParamName.Should().Be("appContext");
        }

        [Theory, AutoData]
        public async void GetCrawlJobData_NullContext_ReturnsDefaults(Guid orgId, Guid userId, Guid providerDefId)
        {
            var sut = new SnowflakeConnectorProvider(_testContext.AppContext.Object);

            var result = await sut.GetCrawlJobData(null, new Dictionary<string, object>(), orgId, userId, providerDefId);

            result.Should().BeOfType<SnowflakeConnectorJobData>();
            var typedResult = result as SnowflakeConnectorJobData;
            typedResult.ToDictionary().Should().Equal(
                new Dictionary<string, object>
                {
                    { SnowflakeConstants.KeyName.Username, null },
                    { SnowflakeConstants.KeyName.DatabaseName, null },
                    { SnowflakeConstants.KeyName.Host, null },
                    { SnowflakeConstants.KeyName.Password, null },
                    { SnowflakeConstants.KeyName.PortNumber, null },
                    { SnowflakeConstants.KeyName.Role, null },
                    { SnowflakeConstants.KeyName.Warehouse, null },
                    { SnowflakeConstants.KeyName.Schema, null },
                    { SnowflakeConstants.KeyName.Account, null }
                });

            typedResult.Username.Should().BeNull();
            typedResult.DatabaseName.Should().BeNull();
            typedResult.Host.Should().BeNull();
            typedResult.Password.Should().BeNull();
            typedResult.PortNumber.Should().BeNull();
            typedResult.Role.Should().BeNull();
            typedResult.Warehouse.Should().BeNull();
            typedResult.Schema.Should().BeNull();
            typedResult.Account.Should().BeNull();
        }

        [Theory, AutoData]
        public void GetCrawlJobData_NullConfiguration_Throws(Guid orgId, Guid userId, Guid providerDefId)
        {
            var sut = new SnowflakeConnectorProvider(_testContext.AppContext.Object);

            Func<Task> action = () => sut.GetCrawlJobData(_testContext.ProviderUpdateContext, null, orgId, userId, providerDefId);

            action.Should().Throw<ArgumentNullException>()
                .And.ParamName.Should().Be("configuration");
        }

        [Theory, AutoData]
        public async void GetCrawlJobData_CamelCaseKeys_MatchesConstantsAndReturnsValues(Guid orgId, Guid userId, Guid providerDefId)
        {
            var sut = new SnowflakeConnectorProvider(_testContext.AppContext.Object);
            var values = new Dictionary<string, object>
            {
                { "username", "user" },
                { "databaseName", "database" },
                { "host", "host" },
                { "password", "password" },
                { "portNumber", "port" },
                { "role", "role" },
                { "warehouse", "warehouse" },
                { "schema", "schema" },
                { "account", "account" }
            };


            var result = await sut.GetCrawlJobData(_testContext.ProviderUpdateContext,
               values, orgId, userId, providerDefId);

            result.Should().BeOfType<SnowflakeConnectorJobData>();
            var typedResult = result as SnowflakeConnectorJobData;
            typedResult.ToDictionary().Should().Equal(
                new Dictionary<string, object>
                {
                    { SnowflakeConstants.KeyName.Username, "user" },
                    { SnowflakeConstants.KeyName.DatabaseName, "database" },
                    { SnowflakeConstants.KeyName.Host, "host" },
                    { SnowflakeConstants.KeyName.Password, "password" },
                    { SnowflakeConstants.KeyName.PortNumber, "port" },
                    { SnowflakeConstants.KeyName.Role, "role" },
                    { SnowflakeConstants.KeyName.Warehouse, "warehouse" },
                    { SnowflakeConstants.KeyName.Schema, "schema" },
                    { SnowflakeConstants.KeyName.Account, "account" }
                });

            typedResult.Username.Should().Be("user");
            typedResult.DatabaseName.Should().Be("database");
            typedResult.Host.Should().Be("host");
            typedResult.Password.Should().Be("password");
            typedResult.PortNumber.Should().Be("port");
            typedResult.Role.Should().Be("role");
            typedResult.Warehouse.Should().Be("warehouse");
            typedResult.Schema.Should().Be("schema");
            typedResult.Account.Should().Be("account");
        }

        [Theory, AutoData]
        public void TestAuthentication_Throws(Guid orgId, Guid userId, Guid providerDefId)
        {
            var sut = new SnowflakeConnectorProvider(_testContext.AppContext.Object);

            Func<Task> action = () => sut.TestAuthentication(_testContext.ProviderUpdateContext, null, orgId, userId, providerDefId);

            action.Should().Throw<NotImplementedException>();
        }

        [Theory, AutoData]
        public void FetchUnSyncedEntityStatistics_Throws(Guid orgId, Guid userId, Guid providerDefId)
        {
            var sut = new SnowflakeConnectorProvider(_testContext.AppContext.Object);

            Func<Task> action = () => sut.FetchUnSyncedEntityStatistics(_testContext.ProviderUpdateContext, null, orgId, userId, providerDefId);

            action.Should().Throw<NotImplementedException>();
        }

        [Theory, AutoData]
        public async void GetHelperConfiguration_NullContext_ReturnsDefaults(Guid orgId, Guid userId, Guid providerDefId)
        {
            var sut = new SnowflakeConnectorProvider(_testContext.AppContext.Object);
            var data = new SnowflakeConnectorJobData(new Dictionary<string, object>());


            var result = await sut.GetHelperConfiguration(null, data, orgId, userId, providerDefId);

            result.Should().Equal(
                new Dictionary<string, object>
                {
                    { SnowflakeConstants.KeyName.Username, null },
                    { SnowflakeConstants.KeyName.DatabaseName, null },
                    { SnowflakeConstants.KeyName.Host, null },
                    { SnowflakeConstants.KeyName.Password, null },
                    { SnowflakeConstants.KeyName.PortNumber, null },
                    { SnowflakeConstants.KeyName.Role, null },
                    { SnowflakeConstants.KeyName.Warehouse, null },
                    { SnowflakeConstants.KeyName.Schema, null },
                    { SnowflakeConstants.KeyName.Account, null }
                });
        }

        [Theory, AutoData]
        public void GetHelperConfiguration_NullData_Throws(Guid orgId, Guid userId, Guid providerDefId)
        {
            var sut = new SnowflakeConnectorProvider(_testContext.AppContext.Object);

            Func<Task> action = () => sut.GetHelperConfiguration(_testContext.ProviderUpdateContext, null, orgId, userId, providerDefId);

            action.Should().Throw<ArgumentNullException>()
                .And.ParamName.Should().Be("jobData");
        }

        [Theory, AutoData]
        public async void GetHelperConfiguration_CamelCaseKeys_MatchesConstantsAndReturnsValues(Guid orgId, Guid userId, Guid providerDefId)
        {
            var sut = new SnowflakeConnectorProvider(_testContext.AppContext.Object);
            var data = new SnowflakeConnectorJobData(new Dictionary<string, object> {
                { "username", "user" },
                { "databaseName", "database" },
                { "host", "host" },
                { "password", "password" },
                { "portNumber", "port" },
                { "role", "role" },
                { "warehouse", "warehouse" },
                { "schema", "schema" },
                { "account", "account" }
            });



            var result = await sut.GetHelperConfiguration(_testContext.ProviderUpdateContext, data, orgId, userId, providerDefId);

            result.Should().Equal(
                new Dictionary<string, object>
                {
                    { SnowflakeConstants.KeyName.Username, "user" },
                    { SnowflakeConstants.KeyName.DatabaseName, "database" },
                    { SnowflakeConstants.KeyName.Host, "host" },
                    { SnowflakeConstants.KeyName.Password, "password" },
                    { SnowflakeConstants.KeyName.PortNumber, "port" },
                    { SnowflakeConstants.KeyName.Role, "role" },
                    { SnowflakeConstants.KeyName.Warehouse, "warehouse" },
                    { SnowflakeConstants.KeyName.Schema, "schema" },
                    { SnowflakeConstants.KeyName.Account, "account" }
                });
        }

        [Theory, AutoData]
        public async void GetHelperConfigurationWithFolder_NullContext_ReturnsDefaults(Guid orgId, Guid userId, Guid providerDefId, string folderId)
        {
            var sut = new SnowflakeConnectorProvider(_testContext.AppContext.Object);
            var data = new SnowflakeConnectorJobData(new Dictionary<string, object>());


            var result = await sut.GetHelperConfiguration(null, data, orgId, userId, providerDefId, folderId);

            result.Should().Equal(
                new Dictionary<string, object>
                {
                    { SnowflakeConstants.KeyName.Username, null },
                    { SnowflakeConstants.KeyName.DatabaseName, null },
                    { SnowflakeConstants.KeyName.Host, null },
                    { SnowflakeConstants.KeyName.Password, null },
                    { SnowflakeConstants.KeyName.PortNumber, null },
                    { SnowflakeConstants.KeyName.Role, null },
                    { SnowflakeConstants.KeyName.Warehouse, null },
                    { SnowflakeConstants.KeyName.Schema, null },
                    { SnowflakeConstants.KeyName.Account, null }
                });
        }

        [Theory, AutoData]
        public void GetHelperConfigurationWithFolder_NullData_Throws(Guid orgId, Guid userId, Guid providerDefId, string folderId)
        {
            var sut = new SnowflakeConnectorProvider(_testContext.AppContext.Object);

            Func<Task> action = () => sut.GetHelperConfiguration(_testContext.ProviderUpdateContext, null, orgId, userId, providerDefId, folderId);

            action.Should().Throw<ArgumentNullException>()
                .And.ParamName.Should().Be("jobData");
        }

        [Theory, AutoData]
        public async void GetHelperConfigurationWithFolder_CamelCaseKeys_MatchesConstantsAndReturnsValues(Guid orgId, Guid userId, Guid providerDefId, string folderId)
        {
            var sut = new SnowflakeConnectorProvider(_testContext.AppContext.Object);
            var data = new SnowflakeConnectorJobData(new Dictionary<string, object> {
                { "username", "user" },
                { "databaseName", "database" },
                { "host", "host" },
                { "password", "password" },
                { "portNumber", "port" },
                { "role", "role" },
                { "warehouse", "warehouse" },
                { "schema", "schema" },
                { "account", "account" }
            });



            var result = await sut.GetHelperConfiguration(_testContext.ProviderUpdateContext, data, orgId, userId, providerDefId, folderId);

            result.Should().Equal(
                new Dictionary<string, object>
                {
                    { SnowflakeConstants.KeyName.Username, "user" },
                    { SnowflakeConstants.KeyName.DatabaseName, "database" },
                    { SnowflakeConstants.KeyName.Host, "host" },
                    { SnowflakeConstants.KeyName.Password, "password" },
                    { SnowflakeConstants.KeyName.PortNumber, "port" },
                    { SnowflakeConstants.KeyName.Role, "role" },
                    { SnowflakeConstants.KeyName.Warehouse, "warehouse" },
                    { SnowflakeConstants.KeyName.Schema, "schema" },
                    { SnowflakeConstants.KeyName.Account, "account" }
                });
        }

        [Theory, AutoData]
        public void GetAccountInformation_NullData_Throws(Guid orgId, Guid userId, Guid providerDefId)
        {
            var sut = new SnowflakeConnectorProvider(_testContext.AppContext.Object);

            Func<Task> action = () => sut.GetAccountInformation(_testContext.ProviderUpdateContext, null, orgId, userId, providerDefId);

            action.Should().Throw<ArgumentNullException>()
                .And.ParamName.Should().Be("jobData");
        }

        [Theory, AutoData]
        public void GetAccountInformation_InvalidJobDataType_Throws(Guid orgId, Guid userId, Guid providerDefId)
        {
            var sut = new SnowflakeConnectorProvider(_testContext.AppContext.Object);

            Func<Task> action = () => sut.GetAccountInformation(
                _testContext.ProviderUpdateContext, new CrawlJobData(), orgId, userId, providerDefId);

            action.Should().Throw<ArgumentException>()
                .And.ParamName.Should().Be("jobData");

        }

        [Theory, AutoData]
        public async void GetAccountInformation_EmptyJobData_ReturnsEmpty(Guid orgId, Guid userId, Guid providerDefId)
        {
            var sut = new SnowflakeConnectorProvider(_testContext.AppContext.Object);
            var data = new SnowflakeConnectorJobData(new Dictionary<string, object>());

            var result = await sut.GetAccountInformation(
                _testContext.ProviderUpdateContext, data, orgId, userId, providerDefId);

            result.AccountId.Should().Be(".");
            result.AccountDisplay.Should().Be(".");
        }

        [Theory, AutoData]
        public async void GetAccountInformation_WithJobData_ReturnsValue(Guid orgId, Guid userId, Guid providerDefId)
        {
            var sut = new SnowflakeConnectorProvider(_testContext.AppContext.Object);
            var data = new SnowflakeConnectorJobData(new Dictionary<string, object> {
                { "username", "user" },
                { "databaseName", "database" },
                { "host", "host" },
                { "password", "password" },
                { "port", "port" },
                { "role", "role" },
                { "warehouse", "warehouse" },
                { "schema", "schema" },
                { "account", "account" }
            });

            var result = await sut.GetAccountInformation(
                _testContext.ProviderUpdateContext, data, orgId, userId, providerDefId);

            result.AccountId.Should().Be("host.database");
            result.AccountDisplay.Should().Be("host.database");
        }

        [Theory]
        [InlineAutoData(true)]
        [InlineAutoData(false)]
        public void Schedule_Returns_CronFormat(bool webhooksEnabled, DateTimeOffset dateTime)
        {
            var sut = new SnowflakeConnectorProvider(_testContext.AppContext.Object);

            var result = sut.Schedule(dateTime, webhooksEnabled);

            result.Should().Be($"{dateTime.Minute} 0/23 * * *");
        }

        [Fact]
        public void CreateWebHook_NullJobData_Throws()
        {
            var sut = new SnowflakeConnectorProvider(_testContext.AppContext.Object);

            Func<Task> action = () => sut.CreateWebHook(null, null, Mock.Of<IWebhookDefinition>(), new Dictionary<string, object>());

            action.Should().Throw<ArgumentNullException>()
                .And.ParamName.Should().Be("jobData");
        }

        [Fact]
        public void CreateWebHook_NullWebHookDefinition_Throws()
        {
            var sut = new SnowflakeConnectorProvider(_testContext.AppContext.Object);

            Func<Task> action = () => sut.CreateWebHook(null, new CrawlJobData(), null, new Dictionary<string, object>());

            action.Should().Throw<ArgumentNullException>()
                .And.ParamName.Should().Be("webhookDefinition");
        }

        [Fact]
        public void CreateWebHook_NullConfig_Throws()
        {
            var sut = new SnowflakeConnectorProvider(_testContext.AppContext.Object);

            Func<Task> action = () => sut.CreateWebHook(null, new CrawlJobData(), Mock.Of<IWebhookDefinition>(), null);

            action.Should().Throw<ArgumentNullException>()
                .And.ParamName.Should().Be("config");
        }

        [Fact]
        public void CreateWebHook_Throws_NotImplemented()
        {
            var sut = new SnowflakeConnectorProvider(_testContext.AppContext.Object);

            Func<Task> action = () => sut.CreateWebHook(null, new CrawlJobData(), Mock.Of<IWebhookDefinition>(), new Dictionary<string, object>());

            action.Should().Throw<NotImplementedException>();
        }

        [Fact]
        public void GetWebHooks_Throws_NotImplemented()
        {
            var sut = new SnowflakeConnectorProvider(_testContext.AppContext.Object);

            Func<Task> action = () => sut.GetWebHooks(_testContext.ProviderUpdateContext);

            action.Should().Throw<NotImplementedException>();
        }

        [Fact]
        public void DeleteWebHook_NullJobData_Throws()
        {
            var sut = new SnowflakeConnectorProvider(_testContext.AppContext.Object);

            Func<Task> action = () => sut.DeleteWebHook(null, null, Mock.Of<IWebhookDefinition>());

            action.Should().Throw<ArgumentNullException>()
                .And.ParamName.Should().Be("jobData");
        }

        [Fact]
        public void DeleteWebHook_NullWebHookDefinition_Throws()
        {
            var sut = new SnowflakeConnectorProvider(_testContext.AppContext.Object);

            Func<Task> action = () => sut.DeleteWebHook(null, new CrawlJobData(), null);

            action.Should().Throw<ArgumentNullException>()
                .And.ParamName.Should().Be("webhookDefinition");
        }

        [Fact]
        public void DeleteWebHook_Throws_NotImplemented()
        {
            var sut = new SnowflakeConnectorProvider(_testContext.AppContext.Object);

            Func<Task> action = () => sut.DeleteWebHook(null, new CrawlJobData(), Mock.Of<IWebhookDefinition>());

            action.Should().Throw<NotImplementedException>();
        }

        [Fact]
        public void WebhookManagementEndpoints_NullIds_Throws()
        {
            var sut = new SnowflakeConnectorProvider(_testContext.AppContext.Object);

            Action action = () => sut.WebhookManagementEndpoints(null);

            action.Should().Throw<ArgumentNullException>()
                .And.ParamName.Should().Be("ids");
        }

        [Fact]
        public void WebhookManagementEndpoints_Throws_NotImplemented()
        {
            var sut = new SnowflakeConnectorProvider(_testContext.AppContext.Object);

            Action action = () => sut.WebhookManagementEndpoints(Array.Empty<string>());

            action.Should().Throw<NotImplementedException>();
        }

        [Theory, AutoData]
        public void GetRemainingApiAllowance_NullJobData_Throws(Guid orgId, Guid userId, Guid providerDefId)
        {
            var sut = new SnowflakeConnectorProvider(_testContext.AppContext.Object);

            Action action = () => sut.GetRemainingApiAllowance(null, null, orgId, userId, providerDefId);

            action.Should().Throw<ArgumentNullException>()
                .And.ParamName.Should().Be("jobData");
        }

        [Theory, AutoData]
        public async void GetRemainingApiAllowance_WithJobData_Throws(Guid orgId, Guid userId, Guid providerDefId)
        {
            var sut = new SnowflakeConnectorProvider(_testContext.AppContext.Object);

            var result = await sut.GetRemainingApiAllowance(null, new CrawlJobData(), orgId, userId, providerDefId);

            result.RemainingApiCalls.Should().Be(-1);
            result.TimeUntilNextAvailableCalls.Should().Be(TimeSpan.Zero);
        }

    }
}
