using System;
using System.Net.Http;
using InfluxDB.Collector.Pipeline;

namespace InfluxDB.Collector.Configuration
{
    public abstract class CollectorEmitConfiguration
    {
        public abstract CollectorConfiguration InfluxDB(Uri serverBaseAddress, string database, string username = null, string password = null, HttpMessageHandler handler = null);

        public CollectorConfiguration InfluxDB(string serverBaseAddress, string database, string username = null, string password = null)
        {
            return InfluxDB(new Uri(serverBaseAddress), database, username, password);
        }

        public abstract CollectorConfiguration Emitter(Action<PointData[]> emitter);
    }
}
