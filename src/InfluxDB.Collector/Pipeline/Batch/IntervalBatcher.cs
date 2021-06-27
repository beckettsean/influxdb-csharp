using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using InfluxDB.Collector.Diagnostics;
using InfluxDB.Collector.Platform;
using InfluxDB.Collector.Util;

namespace InfluxDB.Collector.Pipeline.Batch
{
    class IntervalBatcher : IPointEmitter, IDisposable
    {
        ConcurrentQueue<PointData> _queue = new ConcurrentQueue<PointData>();

        readonly TimeSpan _interval;
        readonly int? _maxBatchSize;
        readonly IPointEmitter _parent;

        readonly object _stateLock = new object();
        readonly PortableTimer _timer;
        bool _unloading;
        bool _started;

        public IntervalBatcher(TimeSpan interval, int? maxBatchSize, IPointEmitter parent)
        {
            _parent = parent;
            _interval = interval;
            _maxBatchSize = maxBatchSize;
            _timer = new PortableTimer(cancel => OnTick());
        }

        void CloseAndFlush()
        {
            lock (_stateLock)
            {
                if (!_started || _unloading)
                    return;

                _unloading = true;
            }

            _timer.Dispose();

            OnTick();
        }

        public void Dispose()
        {
            CloseAndFlush();
        }

        Task OnTick()
        {
            try
            {
                if (_queue.IsEmpty)
                    return Task.Delay(0);
                
                var batch = Interlocked.Exchange(ref _queue, new ConcurrentQueue<PointData>()).ToArray();
                
                if (batch.Length == 0)
                    return Task.Delay(0);

                if (_maxBatchSize == null || batch.Length <= _maxBatchSize.Value)
                {
                    _parent.Emit(batch);
                }
                else
                {
                    foreach (var chunk in batch.Batch(_maxBatchSize.Value))
                    {
                        _parent.Emit(chunk.ToArray());
                    }
                }
            }
            catch (Exception ex)
            {
                CollectorLog.ReportError("Failed to emit metrics batch", ex);
            }
            finally
            {
                lock (_stateLock)
                {
                    if (!_unloading)
                        _timer.Start(_interval);
                }
            }

            return Task.Delay(0);
        }

        public void Emit(PointData[] points)
        {
            lock (_stateLock)
            {
                if (_unloading) return;
                if (!_started)
                {
                    _started = true;
                    _timer.Start(TimeSpan.Zero);
                }
            }

            foreach (var point in points)
                _queue.Enqueue(point);
        }
    }
}
