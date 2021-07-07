using System;
using System.Collections.Generic;
using InfluxDB.Collector.Pipeline;
using InfluxDB.Collector.Pipeline.Aggregate;

namespace InfluxDB.Collector.Configuration
{
    class PipelinedCollectorAggregateConfiguration : CollectorAggregateConfiguration
    {
        private readonly CollectorConfiguration _configuration;

        bool _sumIncrements;
        Func<IEnumerable<long>, double> _timeAggregation;
        TimeSpan? _interval;

        public PipelinedCollectorAggregateConfiguration(CollectorConfiguration configuration)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            _configuration = configuration;
        }

        public override CollectorConfiguration AtInterval(TimeSpan interval)
        {
            _interval = interval;
            return _configuration;
        }

        public override CollectorConfiguration SumIncrements()
        {
            _sumIncrements = true;
            return _configuration;
        }

        public override CollectorConfiguration AggregateTimes(Func<IEnumerable<long>, double> func)
        {
            _timeAggregation = func;
            return _configuration;
        }

        public IPointEmitter CreateEmitter(IPointEmitter parent, out Action dispose)
        {
            if (_interval == null)
            {
                dispose = null;
                return parent;
            }

            var aggregator = new AggregatePointEmitter(_interval.Value, _sumIncrements, _timeAggregation, parent);
            dispose = aggregator.Dispose;
            return aggregator;
        }
    }
}