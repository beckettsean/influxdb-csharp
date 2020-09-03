namespace InfluxDB.Collector.Pipeline
{
    interface ISinglePointEmitter
    {
        void Emit(PointData point);
    }
}