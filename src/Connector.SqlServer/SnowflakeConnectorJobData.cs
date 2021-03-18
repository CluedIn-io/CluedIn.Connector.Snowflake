using System.Collections.Generic;
using CluedIn.Core.Crawling;

namespace CluedIn.Connector.Snowflake
{
    public class SnowflakeConnectorJobData : CrawlJobData
    {
        public SnowflakeConnectorJobData(IDictionary<string, object> configuration)
        {
            if (configuration == null)
            {
                return;
            }

            Username = GetValue<string>(configuration, SnowflakeConstants.KeyName.Username);
            DatabaseName = GetValue<string>(configuration, SnowflakeConstants.KeyName.DatabaseName);
            Host = GetValue<string>(configuration, SnowflakeConstants.KeyName.Host);
            Password = GetValue<string>(configuration, SnowflakeConstants.KeyName.Password);
            PortNumber = GetValue<int>(configuration, SnowflakeConstants.KeyName.PortNumber);
            Role = GetValue<string>(configuration, SnowflakeConstants.KeyName.Role);
            Warehouse = GetValue<string>(configuration, SnowflakeConstants.KeyName.Warehouse);
            Schema = GetValue<string>(configuration, SnowflakeConstants.KeyName.Schema);
            Account = GetValue<string>(configuration, SnowflakeConstants.KeyName.Account);
        }

        public IDictionary<string, object> ToDictionary()
        {
            return new Dictionary<string, object> {
                { SnowflakeConstants.KeyName.Username, Username },
                { SnowflakeConstants.KeyName.DatabaseName, DatabaseName },
                { SnowflakeConstants.KeyName.Host, Host },
                { SnowflakeConstants.KeyName.Password, Password },
                { SnowflakeConstants.KeyName.PortNumber, PortNumber },
                { SnowflakeConstants.KeyName.Role, Role },
                { SnowflakeConstants.KeyName.Warehouse, Warehouse },
                { SnowflakeConstants.KeyName.Schema, Schema },
                { SnowflakeConstants.KeyName.Account, Account}
            };
        }

        public string Username { get; set; }

        public string DatabaseName { get; set; }

        public string Host { get; set; }

        public string Password { get; set; }

        public int PortNumber { get; set; }

        public string Role { get; set; }
        public string Warehouse { get; set; }
        public string Schema { get; set; }

        public string Account { get; set; }
    }
}
