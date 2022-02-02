﻿using AutoFixture.Xunit2;
using CluedIn.Connector.Common.Helpers;
using CluedIn.Connector.Snowflake.Connector;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data.Vocabularies;
using System;
using System.Collections.Generic;
using Xunit;

namespace CluedIn.Connector.Snowflake.Unit.Tests
{
    public class SqlGenerationTests
    {
        private readonly SnowflakeClient _snowflakeClient;
        public SqlGenerationTests()
        {
            _snowflakeClient = new SnowflakeClient();
        }


        [Theory, InlineAutoData]
        public void CreateContainerWorks(string name)
        {
            var expectedName = SqlStringSanitizer.Sanitize(name);
            var model = new CreateContainerModel
            {
                Name = name,
                DataTypes = new List<ConnectionDataType>
                {
                    new ConnectionDataType { Name = "Field1", Type = VocabularyKeyDataType.Integer },
                    new ConnectionDataType { Name = "Field2", Type = VocabularyKeyDataType.Text },
                    new ConnectionDataType { Name = "Field3", Type = VocabularyKeyDataType.DateTime },
                    new ConnectionDataType { Name = "Field4", Type = VocabularyKeyDataType.Number },
                    new ConnectionDataType { Name = "Field5", Type = VocabularyKeyDataType.Boolean },
                }
            };

            var result = _snowflakeClient.BuildCreateContainerSql(model);

            Assert.Equal($"CREATE TABLE IF NOT EXISTS {expectedName} ( Field1 varchar NULL, Field2 varchar NULL, Field3 varchar NULL, Field4 varchar NULL, Field5 varchar NULL );", result.Trim().Replace(Environment.NewLine, " "));
        }

        [Theory, InlineAutoData]
        public void StoreDataWorks(string name, int field1, string field2, DateTime field3, decimal field4, bool field5)
        {
            var expectedName = SqlStringSanitizer.Sanitize(name);
            var data = new Dictionary<string, object>
                        {
                             { "Field1", field1   },
                             { "Field2", field2   },
                             { "Field3", field3  },
                             { "Field4", field4   },
                             { "Field5", field5   }
                        };

            var result = _snowflakeClient.BuildStoreDataSql(name, data, out var param);

            Assert.Equal($"MERGE INTO {expectedName} AS target" + Environment.NewLine +
                         $"USING (SELECT '{field1}', '{field2}', '{field3}', '{field4}', '{field5}') AS source (Field1, Field2, Field3, Field4, Field5)" + Environment.NewLine +
                         "  ON (target.OriginEntityCode = source.OriginEntityCode)" + Environment.NewLine +
                         "WHEN MATCHED THEN" + Environment.NewLine +
                         "  UPDATE SET target.Field1 = source.Field1, target.Field2 = source.Field2, target.Field3 = source.Field3, target.Field4 = source.Field4, target.Field5 = source.Field5" + Environment.NewLine +
                         "WHEN NOT MATCHED THEN" + Environment.NewLine +
                         "  INSERT (Field1, Field2, Field3, Field4, Field5)" + Environment.NewLine +
                         "  VALUES (source.Field1, source.Field2, source.Field3, source.Field4, source.Field5);", result.Trim());
            Assert.Equal(data.Count, param.Count);

            for (var index = 0; index < data.Count; index++)
            {
                var parameter = param[index];
                var val = data[$"Field{index + 1}"];
                Assert.Equal(val, parameter.Value);
            }
        }
    }
}
