using InfluxDB.LineProtocol.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using InfluxDB.Collector.Pipeline;
using InfluxDB.Collector.Pipeline.Emit;

namespace InfluxDB.Collector.Configuration
{
    class PipelinedCollectorEmitConfiguration : CollectorEmitConfiguration
    {
        readonly CollectorConfiguration _configuration;
        readonly List<Action<PointData[]>> _emitters = new List<Action<PointData[]>>();
        private ILineProtocolClient _client;

        public PipelinedCollectorEmitConfiguration(
            CollectorConfiguration configuration)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            _configuration = configuration;
        }

        public override CollectorConfiguration InfluxDB(Uri serverBaseAddress, string database, string username = null, string password = null, HttpMessageHandler handler = null)
        {
           if (handler == null)
              handler = new HttpClientHandler();

            if (string.Compare(serverBaseAddress.Scheme, "udp", ignoreCase: true) == 0)
                _client = new LineProtocolUdpClient(serverBaseAddress, database, username, password);
            else
                _client = new LineProtocolClient(handler, serverBaseAddress, database, username, password);
            return _configuration;
        }

        public override CollectorConfiguration Emitter(Action<PointData[]> emitter)
        {
            if (emitter == null) throw new ArgumentNullException(nameof(emitter));
            _emitters.Add(emitter);
            return _configuration;
        }

        public IPointEmitter CreateEmitter(IPointEmitter parent, out Action dispose)
        {
            if (_client == null && !_emitters.Any())
            {
                dispose = null;
                return parent;
            }

            if (parent != null)
                throw new ArgumentException("Parent may not be specified here");

            var result = new List<IPointEmitter>();

            if (_client != null)
            {
                var emitter = new HttpLineProtocolEmitter(_client);
                dispose = emitter.Dispose;
                result.Add(emitter);
            }
            else
            {
                dispose = () => { };
            }

            foreach (var emitter in _emitters)
            {
                result.Add(new DelegateEmitter(emitter));
            }

            return new AggregateEmitter(result);
        }
    }
}
