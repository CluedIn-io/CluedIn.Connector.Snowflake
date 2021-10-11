using System;
using System.Collections.Generic;
using System.Linq;
using AutoFixture.Xunit2;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data.Vocabularies;
using Xunit;

namespace CluedIn.Connector.Snowflake.Unit.Tests
{
    public class SqlGenerationTests : SnowflakeConnectorTestsBase
    {
        [Theory, InlineAutoData]
        public void EmptyContainerWorks(string name)
        {
            var result = Sut.BuildEmptyContainerSql(name);

            Assert.Equal($"TRUNCATE TABLE {name}Edges", result.Trim());
        }

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

            Assert.Equal($"CREATE TABLE {name}Edges ( OriginEntityCode varchar, Code varchar );", result.Trim().Replace(Environment.NewLine, " "));
        }

        [Theory, InlineAutoData]
        public void StoreEdgeDataWorks(string name, string originEntityCode, List<string> edges)
        {
            var result = Sut.BuildEdgeStoreData(name, originEntityCode, edges, out var param);
            Assert.Equal(edges.Count + 1, param.Count); // params will also include origin entity code
            Assert.Contains(param, p => p.ParameterName == "@OriginEntityCode" && p.Value.Equals(originEntityCode));
            for(var index = 0; index < edges.Count; index++)
            {
                Assert.Contains(param, p => p.ParameterName == $"@{index}" && p.Value.Equals(edges[index]));
            }

            var expectedLines = new List<string>
            {
                $"INSERT INTO {name}Edges (OriginEntityCode, Code) values",
                string.Join(", ", Enumerable.Range(0, edges.Count).Select(i => $"(@OriginEntityCode, @{i})"))
            };

            var expectedSql = string.Join(Environment.NewLine, expectedLines);
            Assert.Equal(expectedSql, result.Trim());
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
