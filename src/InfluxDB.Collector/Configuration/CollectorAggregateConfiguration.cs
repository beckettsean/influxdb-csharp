using System;
using System.Collections.Generic;

namespace InfluxDB.Collector.Configuration
{
    public abstract class CollectorAggregateConfiguration
    {
        public abstract CollectorConfiguration AtInterval(TimeSpan interval);

        public abstract CollectorConfiguration SumIncrements();

        public abstract CollectorConfiguration AggregateTimes(Func<IEnumerable<long>, double> func);
    }
}