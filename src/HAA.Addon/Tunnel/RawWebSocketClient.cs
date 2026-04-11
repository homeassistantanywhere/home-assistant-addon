using System.Buffers;
using System.IO.Compression;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;

namespace HAA.Addon.Tunnel;

/// <summary>
/// Raw WebSocket client that handles frames at a low level without compression processing.
/// This allows proxying WebSocket connections that use compression without .NET's
/// incompatible permessage-deflate implementation.
/// </summary>
public class RawWebSocketClient : IDisposable
{
    private static readonly ArrayPool<byte> Pool = ArrayPool<byte>.Shared;

    private readonly TcpClient _tcpClient;
    private readonly Stream _stream;
    private readonly ILogger _logger;
    private readonly SemaphoreSlim _sendLock = new(1, 1);
    private readonly byte[] _headerBuffer = new byte[14]; // Max frame header size (2 + 8 + 4)
    private bool _disposed;

    public bool IsConnected => _tcpClient.Connected && !_disposed;

    /// <summary>
    /// Returns a rented buffer to the pool. Call this after processing frame payload.
    /// </summary>
    public static void ReturnBuffer(byte[] buffer)
    {
        Pool.Return(buffer);
    }

    private RawWebSocketClient(TcpClient tcpClient, Stream stream, ILogger logger)
    {
        _tcpClient = tcpClient;
        _stream = stream;
        _logger = logger;
    }

    /// <summary>
    /// Connects to a WebSocket server without requesting compression
    /// </summary>
    public static async Task<RawWebSocketClient> ConnectAsync(
        Uri uri,
        ILogger logger,
        Dictionary<string, string[]>? additionalHeaders = null,
        CancellationToken cancellationToken = default)
    {
        var host = uri.Host;
        var port = uri.Port > 0 ? uri.Port : (uri.Scheme == "wss" ? 443 : 80);
        var useSsl = uri.Scheme == "wss";

        var tcpClient = new TcpClient();

        try
        {
            await tcpClient.ConnectAsync(host, port, cancellationToken);

            Stream stream = tcpClient.GetStream();

            if (useSsl)
            {
                var sslStream = new SslStream(stream, false, (sender, cert, chain, errors) => true);
                await sslStream.AuthenticateAsClientAsync(new SslClientAuthenticationOptions
                {
                    TargetHost = host
                }, cancellationToken);
                stream = sslStream;
            }

            // Perform WebSocket handshake
            var key = Convert.ToBase64String(RandomNumberGenerator.GetBytes(16));
            var path = uri.PathAndQuery;

            // Build request with standard headers.
            // Omit the default port from the Host header (RFC 7230 §5.4) so strict parsers
            // like aiohttp don't treat "homeassistant:443" as a non-canonical authority.
            var isDefaultPort = (uri.Scheme == "wss" && port == 443) || (uri.Scheme == "ws" && port == 80);
            var hostHeader = isDefaultPort ? host : $"{host}:{port}";

            var requestBuilder = new StringBuilder();
            requestBuilder.Append($"GET {path} HTTP/1.1\r\n");
            requestBuilder.Append($"Host: {hostHeader}\r\n");
            requestBuilder.Append($"Upgrade: websocket\r\n");
            requestBuilder.Append($"Connection: Upgrade\r\n");
            requestBuilder.Append($"Sec-WebSocket-Key: {key}\r\n");
            requestBuilder.Append($"Sec-WebSocket-Version: 13\r\n");

            // Add any additional headers (like cookies, auth) from the original request
            if (additionalHeaders != null)
            {
                foreach (var (headerName, values) in additionalHeaders)
                {
                    // Skip WebSocket-specific and hop-by-hop headers that we handle ourselves
                    if (headerName.Equals("Host", StringComparison.OrdinalIgnoreCase) ||
                        headerName.Equals("Upgrade", StringComparison.OrdinalIgnoreCase) ||
                        headerName.Equals("Connection", StringComparison.OrdinalIgnoreCase) ||
                        headerName.Equals("Origin", StringComparison.OrdinalIgnoreCase) ||
                        headerName.StartsWith("Sec-WebSocket-", StringComparison.OrdinalIgnoreCase) ||
                        // Skip headers that we emit ourselves below - forwarding these would
                        // produce duplicates that aiohttp rejects with "HTTP/1.0 400 Bad Request"
                        headerName.Equals("User-Agent", StringComparison.OrdinalIgnoreCase) ||
                        headerName.Equals("Pragma", StringComparison.OrdinalIgnoreCase) ||
                        headerName.Equals("Cache-Control", StringComparison.OrdinalIgnoreCase) ||
                        // A GET WebSocket upgrade has no body; forwarding these confuses strict parsers
                        headerName.Equals("Content-Length", StringComparison.OrdinalIgnoreCase) ||
                        headerName.Equals("Transfer-Encoding", StringComparison.OrdinalIgnoreCase) ||
                        headerName.Equals("Expect", StringComparison.OrdinalIgnoreCase) ||
                        // Skip headers that could cause issues
                        headerName.StartsWith("X-Forwarded-", StringComparison.OrdinalIgnoreCase) ||
                        headerName.Equals("X-Real-IP", StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    // Forward all other headers (Cookie, Authorization, Accept-*, X-Ingress-Path, etc.)
                    requestBuilder.Append($"{headerName}: {string.Join(", ", values)}\r\n");
                }
            }

            requestBuilder.Append($"Origin: {(useSsl ? "https" : "http")}://{hostHeader}\r\n");
            requestBuilder.Append($"User-Agent: HAA-Addon/{HAA.Shared.Constants.Version}\r\n");
            requestBuilder.Append($"Pragma: no-cache\r\n");
            requestBuilder.Append($"Cache-Control: no-cache\r\n");
            requestBuilder.Append("\r\n");

            var request = requestBuilder.ToString();

            var requestBytes = Encoding.ASCII.GetBytes(request);
            await stream.WriteAsync(requestBytes, cancellationToken);

            // Read response
            var responseBuilder = new StringBuilder();
            var buffer = new byte[1];
            var lineBuilder = new StringBuilder();

            while (true)
            {
                var bytesRead = await stream.ReadAsync(buffer, 0, 1, cancellationToken);
                if (bytesRead == 0)
                    throw new Exception("Connection closed during handshake");

                var c = (char)buffer[0];
                lineBuilder.Append(c);

                if (lineBuilder.Length >= 2 &&
                    lineBuilder[^2] == '\r' &&
                    lineBuilder[^1] == '\n')
                {
                    var line = lineBuilder.ToString();
                    responseBuilder.Append(line);

                    if (line == "\r\n")
                        break; // End of headers

                    lineBuilder.Clear();
                }
            }

            var response = responseBuilder.ToString();

            if (!response.StartsWith("HTTP/1.1 101"))
            {
                // Read a bounded chunk of the response body so we can surface the real error
                // (aiohttp, nginx, etc. put the actual complaint in the body, not the status line).
                var bodyPreview = await TryReadErrorBodyAsync(stream, response, cancellationToken);
                var statusLine = response.Split('\r')[0];
                var detail = string.IsNullOrEmpty(bodyPreview)
                    ? statusLine
                    : $"{statusLine} — {bodyPreview}";
                throw new Exception($"WebSocket handshake failed: {detail}");
            }

            // Verify the accept key
            var expectedAccept = Convert.ToBase64String(
                SHA1.HashData(Encoding.ASCII.GetBytes(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")));

            if (!response.Contains($"Sec-WebSocket-Accept: {expectedAccept}"))
            {
                throw new Exception("WebSocket accept key mismatch");
            }

            return new RawWebSocketClient(tcpClient, stream, logger);
        }
        catch
        {
            tcpClient.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Receives a WebSocket frame. Returns the opcode, payload (rented from pool), actual length, and whether it's the final frame.
    /// IMPORTANT: Caller must call ReturnBuffer(payload) when done with the payload!
    /// </summary>
    public async Task<WebSocketFrame?> ReceiveFrameAsync(CancellationToken cancellationToken = default)
    {
        if (_disposed) return null;

        try
        {
            // Read frame header (2 bytes minimum) using reusable buffer
            if (!await ReadExactAsync(_headerBuffer, 0, 2, cancellationToken))
                return null;

            // Check if this looks like HTTP instead of WebSocket frames
            // "HT" (0x48 0x54) is the start of "HTTP" - server sent error page instead of WebSocket frames
            if (_headerBuffer[0] == 0x48 && _headerBuffer[1] == 0x54) // "HT"
            {
                // Read the rest of what's likely an HTTP response to log it
                var restBuffer = Pool.Rent(200);
                try
                {
                    int restLen = 0;
                    while (restLen < 200)
                    {
                        int read = await _stream.ReadAsync(restBuffer.AsMemory(restLen, 1), cancellationToken);
                        if (read == 0) break;
                        if (restBuffer[restLen] == '\n') { restLen++; break; }
                        restLen++;
                    }
                    var fullLine = "HT" + Encoding.ASCII.GetString(restBuffer, 0, restLen);
                    _logger.LogError("Server sent HTTP instead of WebSocket frame: {Line}", fullLine.Trim());
                }
                finally
                {
                    Pool.Return(restBuffer);
                }
                return null;
            }

            var fin = (_headerBuffer[0] & 0x80) != 0;
            var rsv1 = (_headerBuffer[0] & 0x40) != 0; // Compression bit
            var opcode = (WebSocketOpcode)(_headerBuffer[0] & 0x0F);
            var masked = (_headerBuffer[1] & 0x80) != 0;
            var payloadLen = (long)(_headerBuffer[1] & 0x7F);

            // Extended payload length
            if (payloadLen == 126)
            {
                if (!await ReadExactAsync(_headerBuffer, 2, 2, cancellationToken))
                    return null;
                payloadLen = (_headerBuffer[2] << 8) | _headerBuffer[3];
            }
            else if (payloadLen == 127)
            {
                if (!await ReadExactAsync(_headerBuffer, 2, 8, cancellationToken))
                    return null;
                payloadLen = 0;
                for (int i = 0; i < 8; i++)
                    payloadLen = (payloadLen << 8) | _headerBuffer[2 + i];
            }

            // Read masking key if present (server frames typically aren't masked)
            int maskOffset = payloadLen < 126 ? 2 : (payloadLen < 65536 ? 4 : 10);
            byte[]? maskingKey = null;
            if (masked)
            {
                if (!await ReadExactAsync(_headerBuffer, maskOffset, 4, cancellationToken))
                    return null;
                maskingKey = new byte[4];
                Array.Copy(_headerBuffer, maskOffset, maskingKey, 0, 4);
            }

            // Rent payload buffer from pool
            var payload = payloadLen > 0 ? Pool.Rent((int)payloadLen) : Array.Empty<byte>();
            var actualPayloadLen = (int)payloadLen;

            if (payloadLen > 0)
            {
                if (!await ReadExactAsync(payload, 0, (int)payloadLen, cancellationToken))
                {
                    if (payload.Length > 0) Pool.Return(payload);
                    return null;
                }

                // Unmask if needed
                if (masked && maskingKey != null)
                {
                    for (int i = 0; i < payloadLen; i++)
                        payload[i] ^= maskingKey[i % 4];
                }
            }

            // Decompress if RSV1 is set (permessage-deflate)
            if (rsv1 && payloadLen > 0 && opcode != WebSocketOpcode.Close)
            {
                try
                {
                    var decompressed = DecompressPayload(payload.AsSpan(0, (int)payloadLen));
                    // Return the original compressed buffer and use the decompressed one
                    Pool.Return(payload);
                    payload = decompressed.Buffer;
                    actualPayloadLen = decompressed.Length;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to decompress WebSocket payload, using original data");
                }
            }

            return new WebSocketFrame(opcode, payload, actualPayloadLen, fin);
        }
        catch (Exception ex) when (ex is IOException or ObjectDisposedException)
        {
            return null;
        }
    }

    /// <summary>
    /// Sends a WebSocket frame with the given opcode and payload
    /// </summary>
    public async Task SendFrameAsync(
        WebSocketOpcode opcode,
        ReadOnlyMemory<byte> payload,
        bool isFinal = true,
        CancellationToken cancellationToken = default)
    {
        if (_disposed) return;

        await _sendLock.WaitAsync(cancellationToken);

        var (frameBuffer, frameSize) = BuildFrame(opcode, payload, isFinal);

        try
        {
            await _stream.WriteAsync(frameBuffer.AsMemory(0, frameSize), cancellationToken);
            await _stream.FlushAsync(cancellationToken);
        }
        finally
        {
            Pool.Return(frameBuffer);
            _sendLock.Release();
        }
    }

    /// <summary>
    /// Builds a WebSocket frame synchronously (allows Span usage)
    /// </summary>
    private static (byte[] Buffer, int Size) BuildFrame(WebSocketOpcode opcode, ReadOnlyMemory<byte> payload, bool isFinal)
    {
        var payloadSpan = payload.Span;

        // Calculate frame size: header (2-10) + mask (4) + payload
        int headerSize = 2;
        if (payloadSpan.Length >= 126 && payloadSpan.Length < 65536)
            headerSize = 4;
        else if (payloadSpan.Length >= 65536)
            headerSize = 10;

        int frameSize = headerSize + 4 + payloadSpan.Length; // +4 for masking key
        var frameBuffer = Pool.Rent(frameSize);

        int offset = 0;

        // First byte: FIN + opcode
        frameBuffer[offset++] = (byte)((isFinal ? 0x80 : 0) | (int)opcode);

        // Payload length with mask bit (clients must mask)
        var maskBit = 0x80;
        if (payloadSpan.Length < 126)
        {
            frameBuffer[offset++] = (byte)(maskBit | payloadSpan.Length);
        }
        else if (payloadSpan.Length < 65536)
        {
            frameBuffer[offset++] = (byte)(maskBit | 126);
            frameBuffer[offset++] = (byte)(payloadSpan.Length >> 8);
            frameBuffer[offset++] = (byte)(payloadSpan.Length & 0xFF);
        }
        else
        {
            frameBuffer[offset++] = (byte)(maskBit | 127);
            var len = (long)payloadSpan.Length;
            for (int i = 7; i >= 0; i--)
                frameBuffer[offset++] = (byte)((len >> (i * 8)) & 0xFF);
        }

        // Masking key (random) - write directly to buffer
        Span<byte> maskingKey = frameBuffer.AsSpan(offset, 4);
        RandomNumberGenerator.Fill(maskingKey);
        offset += 4;

        // Write masked payload directly to frame buffer
        for (int i = 0; i < payloadSpan.Length; i++)
            frameBuffer[offset + i] = (byte)(payloadSpan[i] ^ maskingKey[i % 4]);

        return (frameBuffer, frameSize);
    }

    /// <summary>
    /// Sends a close frame and closes the connection
    /// </summary>
    public async Task CloseAsync(ushort statusCode = 1000, string? reason = null)
    {
        if (_disposed) return;

        try
        {
            var reasonBytes = string.IsNullOrEmpty(reason) ? Array.Empty<byte>() : Encoding.UTF8.GetBytes(reason);
            var payload = new byte[2 + reasonBytes.Length];
            payload[0] = (byte)(statusCode >> 8);
            payload[1] = (byte)(statusCode & 0xFF);
            if (reasonBytes.Length > 0)
                Array.Copy(reasonBytes, 0, payload, 2, reasonBytes.Length);

            await SendFrameAsync(WebSocketOpcode.Close, payload);
        }
        catch
        {
            // Ignore errors during close
        }
    }

    /// <summary>
    /// Attempts to read a small chunk of the error response body so we can log what the
    /// upstream server actually complained about. Parses Content-Length from the response
    /// headers if present, otherwise reads up to 1 KB opportunistically.
    /// </summary>
    private static async Task<string> TryReadErrorBodyAsync(Stream stream, string headerBlock, CancellationToken cancellationToken)
    {
        try
        {
            int bytesToRead = 1024;
            var clIdx = headerBlock.IndexOf("Content-Length:", StringComparison.OrdinalIgnoreCase);
            if (clIdx >= 0)
            {
                var eol = headerBlock.IndexOf("\r\n", clIdx, StringComparison.Ordinal);
                if (eol > clIdx &&
                    int.TryParse(headerBlock.AsSpan(clIdx + "Content-Length:".Length, eol - clIdx - "Content-Length:".Length).Trim(), out var cl))
                {
                    bytesToRead = Math.Min(cl, 1024);
                }
            }

            if (bytesToRead <= 0) return string.Empty;

            var buf = new byte[bytesToRead];
            var total = 0;
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(TimeSpan.FromSeconds(2));
            try
            {
                while (total < bytesToRead)
                {
                    var read = await stream.ReadAsync(buf.AsMemory(total, bytesToRead - total), timeoutCts.Token);
                    if (read == 0) break;
                    total += read;
                }
            }
            catch (OperationCanceledException) { /* stop on timeout, return what we got */ }

            if (total == 0) return string.Empty;
            var text = Encoding.UTF8.GetString(buf, 0, total).Trim();
            // Collapse whitespace so log lines stay single-line
            return text.Replace('\r', ' ').Replace('\n', ' ');
        }
        catch
        {
            return string.Empty;
        }
    }

    private async Task<bool> ReadExactAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        int totalRead = 0;
        while (totalRead < count)
        {
            var bytesRead = await _stream.ReadAsync(buffer.AsMemory(offset + totalRead, count - totalRead), cancellationToken);
            if (bytesRead == 0)
                return false;
            totalRead += bytesRead;
        }
        return true;
    }

    /// <summary>
    /// Decompresses a permessage-deflate compressed payload.
    /// Returns a pooled buffer - caller must return it via ReturnBuffer().
    /// </summary>
    private static (byte[] Buffer, int Length) DecompressPayload(ReadOnlySpan<byte> compressedData)
    {
        // permessage-deflate requires appending 0x00 0x00 0xFF 0xFF to the compressed data
        // before decompressing (these bytes are stripped during compression)
        var dataWithTail = Pool.Rent(compressedData.Length + 4);
        try
        {
            compressedData.CopyTo(dataWithTail);
            dataWithTail[compressedData.Length] = 0x00;
            dataWithTail[compressedData.Length + 1] = 0x00;
            dataWithTail[compressedData.Length + 2] = 0xFF;
            dataWithTail[compressedData.Length + 3] = 0xFF;

            using var input = new MemoryStream(dataWithTail, 0, compressedData.Length + 4);
            using var deflate = new DeflateStream(input, CompressionMode.Decompress);

            // Read into a pooled buffer, starting with estimated size
            var outputBuffer = Pool.Rent(compressedData.Length * 4); // Estimate 4x expansion
            try
            {
                int totalRead = 0;

                while (true)
                {
                    int bytesRead = deflate.Read(outputBuffer, totalRead, outputBuffer.Length - totalRead);
                    if (bytesRead == 0)
                        break;

                    totalRead += bytesRead;

                    // Need more space? Rent larger buffer
                    if (totalRead == outputBuffer.Length)
                    {
                        var newBuffer = Pool.Rent(outputBuffer.Length * 2);
                        Array.Copy(outputBuffer, newBuffer, totalRead);
                        Pool.Return(outputBuffer);
                        outputBuffer = newBuffer;
                    }
                }

                return (outputBuffer, totalRead);
            }
            catch
            {
                Pool.Return(outputBuffer);
                throw;
            }
        }
        finally
        {
            Pool.Return(dataWithTail);
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        try { _stream.Dispose(); } catch { }
        try { _tcpClient.Dispose(); } catch { }
        _sendLock.Dispose();
    }
}

public enum WebSocketOpcode : byte
{
    Continuation = 0x0,
    Text = 0x1,
    Binary = 0x2,
    Close = 0x8,
    Ping = 0x9,
    Pong = 0xA
}

/// <summary>
/// Represents a received WebSocket frame with a pooled buffer.
/// IMPORTANT: Call RawWebSocketClient.ReturnBuffer(Payload) when done!
/// </summary>
public readonly record struct WebSocketFrame(WebSocketOpcode Opcode, byte[] Payload, int PayloadLength, bool IsFinal)
{
    /// <summary>
    /// Gets the payload as a memory slice of the actual data (not the full rented buffer).
    /// </summary>
    public ReadOnlyMemory<byte> PayloadMemory => Payload.AsMemory(0, PayloadLength);
}
