using System;
using System.Collections.Generic;
using InfluxDB.Collector.Pipeline;

namespace InfluxDB.Collector.Configuration
{
    class AggregateEmitter : IPointEmitter, ISinglePointEmitter
    {
        readonly List<IPointEmitter> _emitters;

        public AggregateEmitter(List<IPointEmitter> emitters)
        {
            if (emitters == null) throw new ArgumentNullException(nameof(emitters));
            _emitters = emitters;
        }

        public void Emit(PointData[] points)
        {
            foreach (var emitter in _emitters)
                emitter.Emit(points);
        }

        public void Emit(PointData point)
        {
            foreach (var emitter in _emitters)
            {
                if (emitter is ISinglePointEmitter singlePointEmitter)
                {
                    singlePointEmitter.Emit(point);
                }
                else
                {
                    emitter.Emit(new[] { point });
                }
            }
        }
    }
}