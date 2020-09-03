namespace InfluxDB.Collector.Pipeline
{
    class NullMetricsCollector : MetricsCollector
    {
        protected override void Emit(PointData[] points)
        {
        }

        protected override void Emit(PointData point)
        {
        }
    }
}
