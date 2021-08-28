using System;
using System.Collections.Generic;
using System.Text;

namespace CluedIn.Connector.Snowflake.Connector
{
    class EdgeContainerHelper
    {
        public static string GetName(string containerName) => $"{containerName}Edges";
    }
}
