using CluedIn.Connector.Snowflake.Connector;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace CluedIn.Connector.Snowflake.Unit.Tests
{
    public class SnowflakeClientTests
    {
        //[Fact]
        //public async Task SaveData_SomeInput_SavedIntoCloud()
        //{
        //    var configuration = JsonConvert.DeserializeObject<SnowflakeConnectionData>("{\"Configurations\":{\"Host\":\"lg77993.west-europe.azure.snowflakecomputing.com\",\"DatabaseName\":\"TESTDB\",\"Username\":\"ROMAKLIMENKO\",\"Password\":\"***\",\"PortNumber\":\"443\",\"Account\":\"lg77993\",\"Schema\":\"TESTSCHEMA\",\"Warehouse\":\"COMPUTE_WH\",\"Role\":\"ACCOUNTADMIN\",\"firstTime\":true},\"ContainerName\":\"DLContainer\"}");
        //    var dataPart1 = JsonConvert.DeserializeObject<IDictionary<string, object>>("{\"Id\":\"f49a03ec-ea65-53a8-a380-4efb2d4b0d26\",\"PersistHash\":\"wwnnrmqf1xjjnp0il6v2na==\",\"OriginEntityCode\":\"/IMDb/Title/Basic#IMDb:tt0493331\",\"EntityType\":\"/IMDb/Title/Basic\",\"Codes\":[\"/IMDb/Title/Basic#IMDb:tt0493331\"],\"CreatedDate\":null,\"Description\":null,\"Name\":\"Shabhaye Barareh\",\"Type\":\"/IMDb/Title/Basic\"}");
        //    var dataPart2 = JsonConvert.DeserializeObject<IDictionary<string, object>>("{\"Id\":\"9b351807-7e8a-57db-9add-94bc24df1e74\",\"PersistHash\":\"cx1ykbylsww+zz7rnlys3a==\",\"OriginEntityCode\":\"/IMDb/Title/Basic#IMDb:tt0496212\",\"EntityType\":\"/IMDb/Title/Basic\",\"Codes\":[\"/IMDb/Title/Basic#IMDb:tt0496212\"],\"CreatedDate\":null,\"Description\":null,\"Name\":\"Skating with Celebrities\",\"Type\":\"/IMDb/Title/Basic\"}");

        //    var content = new List<IDictionary<string, object>> { dataPart1, dataPart2 };
        //    var snowflakeClient = new SnowflakeClient();

        //    await snowflakeClient.SaveData(configuration, content);
        //}
    }
}
