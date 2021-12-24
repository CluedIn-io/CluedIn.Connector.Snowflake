using System;
using System.Collections.Generic;
using AutoFixture.Xunit2;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data.Vocabularies;
using Xunit;

namespace CluedIn.Connector.Snowflake.Unit.Tests
{
    public class SqlGenerationTests : SnowflakeConnectorTestsBase
    {

        //[Theory, InlineAutoData]
        //public void EmptyContainerWorks(string name, string originEntityCode,)
        //{
        //    var result = Sut.EmptyContainer(name);

        //    Assert.Equal($"TRUNCATE TABLE {name}", result.Trim());
        //}

        [Theory, InlineAutoData]
        public void CreateContainerWorks(string name)
        {
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

            var result = Sut.BuildCreateContainerSql(model, "dummy_db");

            Assert.Equal($"CREATE TABLE {name} ( Field1 varchar NULL, Field2 varchar NULL, Field3 varchar NULL, Field4 varchar NULL, Field5 varchar NULL );", result.Trim().Replace(Environment.NewLine, " "));
        }

        [Theory, InlineAutoData]
        public void StoreDataWorks(string name, int field1, string field2, DateTime field3, decimal field4, bool field5)
        {
            var data = new Dictionary<string, object>
                        {
                             { "Field1", field1   },
                             { "Field2", field2   },
                             { "Field3", field3  },
                             { "Field4", field4   },
                             { "Field5", field5   }
                        };

            var result = Sut.BuildStoreDataSql(name, data, "dummy_db", out var param);

            Assert.Equal($"MERGE INTO {name} AS target" + Environment.NewLine +
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
