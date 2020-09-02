using InfluxDB.LineProtocol.Payload;
using System;
using System.IO;
using System.IO.Compression;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace InfluxDB.LineProtocol.Client
{
    public class LineProtocolClient : LineProtocolClientBase
    {
        private readonly HttpClient _httpClient;
        private readonly bool _enableCompression;

        public LineProtocolClient(Uri serverBaseAddress, string database, string username = null, string password = null, bool enableCompression = false)
            : this(new HttpClientHandler(), serverBaseAddress, database, username, password, enableCompression)
        {
        }

        public LineProtocolClient(
                HttpMessageHandler handler,
                Uri serverBaseAddress,
                string database,
                string username,
                string password,
                bool enableCompression)
            :base(serverBaseAddress, database, username, password)
        {
            if (serverBaseAddress == null)
                throw new ArgumentNullException(nameof(serverBaseAddress));
            if (string.IsNullOrEmpty(database))
                throw new ArgumentException("A database must be specified");

            // Overload that allows injecting handler is protected to avoid HttpMessageHandler being part of our public api which would force clients to reference System.Net.Http when using the lib.
            _httpClient = new HttpClient(handler) { BaseAddress = serverBaseAddress };
            _enableCompression = enableCompression;
        }

        protected override async Task<LineProtocolWriteResult> OnSendAsync(
            string payload,
            Precision precision,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var endpoint = $"write?db={Uri.EscapeDataString(_database)}";
            if (!string.IsNullOrEmpty(_username))
                endpoint += $"&u={Uri.EscapeDataString(_username)}&p={Uri.EscapeDataString(_password)}";

            switch (precision)
            {
                case Precision.Microseconds:
                    endpoint += "&precision=u";
                    break;
                case Precision.Milliseconds:
                    endpoint += "&precision=ms";
                    break;
                case Precision.Seconds:
                    endpoint += "&precision=s";
                    break;
                case Precision.Minutes:
                    endpoint += "&precision=m";
                    break;
                case Precision.Hours:
                    endpoint += "&precision=h";
                    break;
            }

            HttpContent content;

            if (_enableCompression)
            {
                var compressed = Compress(Encoding.UTF8.GetBytes(payload));
                
                content = new ByteArrayContent(compressed);
                content.Headers.ContentEncoding.Add("gzip");
                content.Headers.ContentType = new MediaTypeHeaderValue("text/plain") { CharSet = "utf-8" };
            }
            else
            {
                content = new StringContent(payload, Encoding.UTF8);
            }

            var response = await _httpClient.PostAsync(endpoint, content, cancellationToken).ConfigureAwait(false);
            if (response.IsSuccessStatusCode)
            {
                return new LineProtocolWriteResult(true, null);
            }

            var body = string.Empty;

            if (response.Content != null)
            {
                body = await response.Content.ReadAsStringAsync();
            }

            return new LineProtocolWriteResult(false, $"{response.StatusCode} {response.ReasonPhrase} {body}");
        }

        private byte[] Compress(byte[] input)
        {
            using (var ms = new MemoryStream())
            {
                using (var gz = new GZipStream(ms, CompressionLevel.Fastest))
                    gz.Write(input, 0, input.Length);

                return ms.ToArray();
            }
        }
    }
}
