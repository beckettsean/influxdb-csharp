using Xunit;
using InfluxDB.LineProtocol.Payload;
using System.Collections.Generic;
using System;
using System.IO;

namespace InfluxDB.LineProtocol.Tests
{
    public class LineProtocolPointTests
    {
        [Fact]
        public void CompleteExampleFromDocs()
        {
            // Given in: https://influxdb.com/docs/v0.9/write_protocols/write_syntax.html
            const string expected = "\"measurement\\ with\\ quotes\",tag\\ key\\ with\\ spaces=tag\\,value\\,with\"commas\" field_key\\\\\\\\=\"string field value, only \\\" need be quoted\" 1441756800000000000";

            var point = new LineProtocolPoint(
                "\"measurement with quotes\"",
                new Dictionary<string, object>
                {
                    { "field_key\\\\\\\\", "string field value, only \" need be quoted" }
                },
                new Dictionary<string, string>
                {
                    { "tag key with spaces", "tag,value,with\"commas\"" }
                },
                new DateTime(2015, 9, 9, 0, 0, 0, DateTimeKind.Utc));

            var sw = new StringWriter();
            point.Format(sw);

            Assert.Equal(expected, sw.ToString());
        }

        [Fact]
        public void WriteNanosecondTimestamps()
        {
            const string expected = "a,t=1 f=1i 1490951520002000000";

            var point = new LineProtocolPoint(
                "a",
                new Dictionary<string, object>
                {
                    { "f", 1 }
                },
                new Dictionary<string, string>
                {
                    { "t", "1" }
                },
                new DateTime(636265483200020000L, DateTimeKind.Utc));

            var sw = new StringWriter();
            point.Format(sw);

            Assert.Equal(expected, sw.ToString());
        }

        [Fact]
        public void ExampleWithJsonTextWithNestedDoubleQuote()
        {
            const string expected = "\"measurement\\ with\\ json\\ with\\ quotes\",symbol=test field_key=\"{\\\"content\\\":\\\"test \\\\\\\" data\\\"}\" 1441756800000000000";

            var json = "{\"content\":\"test \\\" data\"}";

            var point = new LineProtocolPoint(
                "\"measurement with json with quotes\"",
                new Dictionary<string, object>
                {
                    { "field_key", json }
                },
                new Dictionary<string, string>
                {
                    { "symbol", "test" }
                },
                new DateTime(2015, 9, 9, 0, 0, 0, DateTimeKind.Utc));

            var sw = new StringWriter();
            point.Format(sw);

            Assert.Equal(expected, sw.ToString());
        }

    }
}
