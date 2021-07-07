using InfluxDB.Collector.Configuration;
using System;
using InfluxDB.Collector.Pipeline;

namespace InfluxDB.Collector
{
    public class CollectorConfiguration
    {
        readonly IPointEmitter _parent;
        readonly PipelinedCollectorTagConfiguration _tag;
        readonly PipelinedCollectorEmitConfiguration _emitter;
        readonly PipelinedCollectorBatchConfiguration _batcher;
        readonly PipelinedCollectorAggregateConfiguration _aggregator;

        public CollectorConfiguration()
            : this(null)
        {
        }

        internal CollectorConfiguration(IPointEmitter parent = null)
        {
            _parent = parent;
            _tag = new PipelinedCollectorTagConfiguration(this);
            _emitter = new PipelinedCollectorEmitConfiguration(this);
            _batcher = new PipelinedCollectorBatchConfiguration(this);
            _aggregator = new PipelinedCollectorAggregateConfiguration(this);
        }

        public CollectorTagConfiguration Tag => _tag;

        public CollectorEmitConfiguration WriteTo => _emitter;

        public CollectorBatchConfiguration Batch => _batcher;

        public CollectorAggregateConfiguration Aggregate => _aggregator;

        public MetricsCollector CreateCollector()
        {
            Action disposeEmitter;
            Action disposeBatcher;

            var emitter = _parent;
            emitter = _emitter.CreateEmitter(emitter, out disposeEmitter);
            emitter = _batcher.CreateEmitter(emitter, out disposeBatcher);
            emitter = _aggregator.CreateEmitter(emitter, out disposeEmitter);

            return new PipelinedMetricsCollector(emitter, _tag.CreateEnricher(), () =>
            {
                disposeBatcher?.Invoke();
                disposeEmitter?.Invoke();
            });
        }
    }
}