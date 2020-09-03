using System;
using System.Collections.Generic;

namespace InfluxDB.Collector.Pipeline.Aggregate
{
    struct GroupingKey : IEquatable<GroupingKey>
    {
        private static readonly Dictionary<string, string> EmptyDict = new Dictionary<string, string>();

        public long Bucket { get; }

        public MeasurementKind Kind { get; }

        public string Measurement { get; }

        public Dictionary<string, string> Tags { get; }

        public GroupingKey(long bucket, MeasurementKind kind, string measurement, Dictionary<string, string> tags)
        {
            Bucket = bucket;
            Kind = kind;
            Measurement = measurement;
            Tags = tags ?? EmptyDict;
        }

        public bool Equals(GroupingKey other)
        {
            return Bucket == other.Bucket && Kind == other.Kind && Measurement == other.Measurement && DictionaryEquals(Tags, other.Tags);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }

            return obj is GroupingKey key && Equals(key);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int hashCode = Bucket.GetHashCode();
                hashCode = (hashCode * 397) ^ (int) Kind;
                hashCode = (hashCode * 397) ^ Measurement.GetHashCode();
                hashCode = (hashCode * 397) ^ TagsHashCode();
                return hashCode;
            }
        }

        int TagsHashCode()
        {
            unchecked
            {
                int hashCode = 1;
                foreach (var kvp in Tags)
                {
                    hashCode *= (kvp.Key.GetHashCode() * 397) ^ kvp.Key.GetHashCode();
                }

                return hashCode;
            }
        }

        static bool DictionaryEquals(Dictionary<string, string> dict, Dictionary<string, string> dict2)
        {
            if (dict.Count != dict2.Count)
            {
                return false;
            }

            foreach (var kvp in dict)
            {
                if (dict2.TryGetValue(kvp.Key, out string value))
                {
                    if (value != kvp.Value)
                    {
                        return false;
                    }
                }
                else
                {
                    return false;
                }
            }

            return true;
        }
    }
}