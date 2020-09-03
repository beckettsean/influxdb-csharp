using System;

namespace InfluxDB.Collector.Pipeline
{
    class PipelinedMetricsCollector : MetricsCollector
    {
        readonly IPointEmitter _emitter;
        readonly IPointEnricher _enricher;
        readonly Action _dispose;

        public PipelinedMetricsCollector(IPointEmitter emitter, IPointEnricher enricher, Action dispose)
        {
            _emitter = emitter;
            _enricher = enricher;
            _dispose = dispose;
        }

        protected override void Emit(PointData[] points)
        {
            foreach (var point in points)
                _enricher.Enrich(point);

            _emitter.Emit(points);
        }

        protected override void Emit(PointData point)
        {
            _enricher.Enrich(point);

            if (_emitter is ISinglePointEmitter singlePointEmitter)
            {
                singlePointEmitter.Emit(point);
            }
            else
            {
                _emitter.Emit(new[] { point });
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                _dispose();
        }
    }
}