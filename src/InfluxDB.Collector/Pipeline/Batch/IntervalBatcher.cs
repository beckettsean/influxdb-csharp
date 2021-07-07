using System;
using System.Collections.Generic;
using System.Linq;
using InfluxDB.Collector.Pipeline.Common;
using InfluxDB.Collector.Util;

namespace InfluxDB.Collector.Pipeline.Batch
{
    class IntervalBatcher : IntervalEmitterBase
    {
        readonly IPointEmitter _parent;

        readonly int? _maxBatchSize;

        public IntervalBatcher(TimeSpan interval, int? maxBatchSize, IPointEmitter parent) : base(interval)
        {
            _maxBatchSize = maxBatchSize;
            _parent = parent;
        }

        protected override void HandleBatch(IReadOnlyCollection<PointData> batch)
        {
            if (_maxBatchSize == null || batch.Count <= _maxBatchSize.Value)
            {
                _parent.Emit(batch.ToArray());
            }
            else
            {
                foreach (var chunk in batch.Batch(_maxBatchSize.Value))
                {
                    _parent.Emit(chunk.ToArray());
                }
            }
        }
    }
}