using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using InfluxDB.Collector.Diagnostics;
using InfluxDB.Collector.Platform;

namespace InfluxDB.Collector.Pipeline.Common
{
    internal abstract class IntervalEmitterBase : IPointEmitter, IDisposable
    {
        readonly object _queueLock = new object();
        Queue<PointData> _queue = new Queue<PointData>();

        protected readonly TimeSpan _interval;

        readonly object _stateLock = new object();
        readonly PortableTimer _timer;
        bool _unloading;
        bool _started;

        protected IntervalEmitterBase(TimeSpan interval)
        {
            _interval = interval;
            _timer = new PortableTimer(cancel => OnTick());
        }

        private void CloseAndFlush()
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

        protected Task OnTick()
        {
            try
            {
                Queue<PointData> batch;
                lock (_queueLock)
                {
                    if (_queue.Count == 0)
                        return Task.Delay(0);

                    batch = _queue;
                    _queue = new Queue<PointData>();
                }

                HandleBatch(batch);
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

            lock (_queueLock)
            {
                foreach (var point in points)
                    _queue.Enqueue(point);
            }
        }

        protected abstract void HandleBatch(IReadOnlyCollection<PointData> batch);
    }
}