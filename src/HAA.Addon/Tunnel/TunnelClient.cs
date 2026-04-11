using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using HAA.Shared;
using HAA.Shared.Protocol;

namespace HAA.Addon.Tunnel;

/// <summary>
/// Client that maintains a tunnel connection to the server
/// </summary>
public class TunnelClient : IAsyncDisposable
{
    private readonly string _serverUrl;
    private readonly string _connectionKey;
    private readonly string _homeAssistantUrl;
    private readonly HttpClient _haHttpClient;
    private readonly ILogger<TunnelClient> _logger;
    private readonly ConcurrentDictionary<string, RawWebSocketClient> _activeWebSockets = new();
    private readonly ConcurrentDictionary<string, CancellationTokenSource> _pendingHttpRequests = new();
    // Track whether we're in the middle of a fragmented message for each WebSocket connection
    // Value is the original opcode (Text or Binary) when in a fragment, null otherwise
    private readonly ConcurrentDictionary<string, WebSocketOpcode?> _webSocketFragmentState = new();
    // Lock to prevent concurrent WebSocket sends which can corrupt the connection
    private readonly SemaphoreSlim _sendLock = new(1, 1);

    private ClientWebSocket? _webSocket;
    private CancellationTokenSource? _cts;
    private int _reconnectAttempts;

    public bool IsConnected => _webSocket?.State == WebSocketState.Open;
    public string? Subdomain { get; private set; }

    public TunnelClient(
        string serverUrl,
        string connectionKey,
        string homeAssistantUrl,
        ILogger<TunnelClient> logger)
    {
        _serverUrl = serverUrl.TrimEnd('/');
        _connectionKey = connectionKey;
        _homeAssistantUrl = homeAssistantUrl.TrimEnd('/');
        _logger = logger;

        // Skip SSL certificate validation for internal connections
        // This is safe because we're connecting within the Docker network
        // and the certificate may be issued for an external domain
        var handler = new HttpClientHandler
        {
            ServerCertificateCustomValidationCallback = (message, cert, chain, errors) => true
        };

        _haHttpClient = new HttpClient(handler)
        {
            BaseAddress = new Uri(_homeAssistantUrl),
            Timeout = TimeSpan.FromSeconds(Constants.DefaultRequestTimeoutSeconds)
        };
    }

    /// <summary>
    /// Connects to the server and maintains the connection
    /// </summary>
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        while (!_cts.Token.IsCancellationRequested)
        {
            try
            {
                await ConnectAndRunAsync(_cts.Token);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                _logger.LogInformation("Tunnel client shutting down");
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Connection error, will retry...");
            }

            if (!_cts.Token.IsCancellationRequested)
            {
                var delay = CalculateReconnectDelay();
                _logger.LogInformation("Reconnecting in {Delay} seconds...", delay.TotalSeconds);
                await Task.Delay(delay, _cts.Token);
                _reconnectAttempts++;
            }
        }
    }

    private TimeSpan CalculateReconnectDelay()
    {
        // Exponential backoff with jitter
        var baseDelay = Math.Min(
            Constants.InitialReconnectDelaySeconds * Math.Pow(2, _reconnectAttempts),
            Constants.MaxReconnectDelaySeconds);

        var jitter = Random.Shared.NextDouble() * 0.3 * baseDelay;
        return TimeSpan.FromSeconds(baseDelay + jitter);
    }

    private async Task ConnectAndRunAsync(CancellationToken cancellationToken)
    {
        _webSocket = new ClientWebSocket();
        _webSocket.Options.KeepAliveInterval = TimeSpan.FromSeconds(30);

        var wsUrl = $"{_serverUrl}{Constants.TunnelWebSocketPath}";

        _logger.LogInformation("Connecting to {Url}", wsUrl);

        await _webSocket.ConnectAsync(new Uri(wsUrl), cancellationToken);
        _logger.LogInformation("WebSocket connected, authenticating...");

        // Send authentication
        if (!await AuthenticateAsync(cancellationToken))
        {
            throw new Exception("Authentication failed");
        }

        _reconnectAttempts = 0; // Reset on successful connection
        _logger.LogInformation("Authenticated as {Subdomain}, tunnel is active", Subdomain);

        // Run receive loop (blocking until disconnected).
        // The addon doesn't send heartbeats; it only responds to server heartbeats in HandleHeartbeatAsync.
        await RunReceiveLoopAsync(cancellationToken);
    }

    private async Task<bool> AuthenticateAsync(CancellationToken cancellationToken)
    {
        var authMessage = new AuthenticateMessage
        {
            ConnectionKey = _connectionKey,
            AddonVersion = Constants.Version,
            HaVersion = await GetHomeAssistantVersionAsync()
        };

        await SendMessageAsync(authMessage, cancellationToken);

        // Wait for response
        var response = await ReceiveMessageAsync(cancellationToken);

        if (response is AuthenticateResponseMessage authResponse)
        {
            if (authResponse.Success)
            {
                Subdomain = authResponse.Subdomain;
                return true;
            }

            _logger.LogError("Authentication failed: {Code} - {Message}",
                authResponse.ErrorCode, authResponse.ErrorMessage);
            return false;
        }

        _logger.LogError("Unexpected response during authentication: {Type}", response?.Type);
        return false;
    }

    private Task<string?> GetHomeAssistantVersionAsync()
    {
        // Skip this check - it requires auth and isn't essential
        // The HA version can be obtained through other means if needed
        return Task.FromResult<string?>(null);
    }

    private async Task RunReceiveLoopAsync(CancellationToken cancellationToken)
    {
        // Use smaller buffer - WebSocket fragments large messages, we accumulate in MemoryStream
        var buffer = new byte[65536];

        try
        {
            while (!cancellationToken.IsCancellationRequested && IsConnected)
            {
                using var ms = new MemoryStream();
                WebSocketReceiveResult result;

                do
                {
                    result = await _webSocket!.ReceiveAsync(buffer, cancellationToken);

                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        _logger.LogInformation("Server closed connection");
                        return;
                    }

                    ms.Write(buffer, 0, result.Count);
                }
                while (!result.EndOfMessage);

                if (result.MessageType == WebSocketMessageType.Text)
                {
                    // Use GetBuffer() to avoid allocation, parse directly from bytes
                    var message = TunnelMessage.FromBytes(ms.GetBuffer().AsSpan(0, (int)ms.Length));
                    if (message != null)
                    {
                        _ = HandleMessageAsync(message, cancellationToken);
                    }
                    else
                    {
                        _logger.LogWarning("Failed to parse message ({Length} bytes)", ms.Length);
                    }
                }
            }
        }
        catch (WebSocketException ex) when (ex.WebSocketErrorCode == WebSocketError.ConnectionClosedPrematurely)
        {
            _logger.LogWarning("Connection closed prematurely");
        }
    }

    private async Task HandleMessageAsync(TunnelMessage message, CancellationToken cancellationToken)
    {
        try
        {
            switch (message)
            {
                case TunnelHttpRequest request:
                    await HandleHttpRequestAsync(request, cancellationToken);
                    break;

                case HeartbeatMessage heartbeat:
                    await HandleHeartbeatAsync(heartbeat, cancellationToken);
                    break;

                case DisconnectMessage disconnect:
                    _logger.LogInformation("Server requested disconnect: {Reason}", disconnect.Reason);
                    await _cts!.CancelAsync();
                    break;

                case WebSocketOpenMessage wsOpen:
                    _ = HandleWebSocketOpenAsync(wsOpen, cancellationToken);
                    break;

                case WebSocketDataMessage wsData:
                    await HandleWebSocketDataAsync(wsData, cancellationToken);
                    break;

                case WebSocketCloseMessage wsClose:
                    await HandleWebSocketCloseAsync(wsClose, cancellationToken);
                    break;

                case HttpRequestAbortedMessage aborted:
                    HandleHttpRequestAborted(aborted);
                    break;

                default:
                    _logger.LogDebug("Received message type {Type}", message.Type);
                    break;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error handling message");
        }
    }

    // Maximum HTTP response chunk size (64KB raw = ~85KB base64)
    private const int MaxHttpChunkSize = 65536;

    private async Task HandleHttpRequestAsync(TunnelHttpRequest request, CancellationToken cancellationToken)
    {
        var startTime = DateTimeOffset.UtcNow;

        // Check if this is a streaming endpoint (e.g., /logs/follow)
        var isStreamingEndpoint = request.Path.Contains("/follow");
        var timeout = isStreamingEndpoint ? 300 : request.TimeoutSeconds;

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(timeout));

        // Register this request so it can be cancelled if client disconnects
        _pendingHttpRequests[request.RequestId] = cts;

        try
        {
            // Build HTTP request to Home Assistant
            using var haRequest = new HttpRequestMessage(new HttpMethod(request.Method), request.Path);

            // Copy body as-is
            var body = request.GetBodyBytes();
            if (body != null && body.Length > 0)
            {
                haRequest.Content = new ByteArrayContent(body);
            }

            // Copy headers - always to request headers, content-type to content if we have content
            foreach (var (key, values) in request.Headers)
            {
                if (IsRestrictedHeader(key))
                    continue;

                // Content-Type should go to content headers if we have content
                if (key.Equals("Content-Type", StringComparison.OrdinalIgnoreCase) && haRequest.Content != null)
                {
                    haRequest.Content.Headers.TryAddWithoutValidation(key, values);
                }
                else
                {
                    haRequest.Headers.TryAddWithoutValidation(key, values);
                }
            }

            using var haResponse = await _haHttpClient.SendAsync(
                haRequest,
                HttpCompletionOption.ResponseHeadersRead,
                cts.Token);

            // Collect headers
            var headers = new Dictionary<string, string[]>();
            foreach (var header in haResponse.Headers)
            {
                headers[header.Key] = header.Value.ToArray();
            }
            foreach (var header in haResponse.Content.Headers)
            {
                headers[header.Key] = header.Value.ToArray();
            }

            var processingTime = (long)(DateTimeOffset.UtcNow - startTime).TotalMilliseconds;

            // Stream the response body in chunks
            await using var stream = await haResponse.Content.ReadAsStreamAsync(cts.Token);
            var buffer = new byte[MaxHttpChunkSize];
            bool isFirstChunk = true;
            bool endOfStream = false;

            while (!endOfStream)
            {
                int bytesInBuffer = 0;

                if (isStreamingEndpoint)
                {
                    // For streaming endpoints, send data immediately without buffering
                    var bytesRead = await stream.ReadAsync(buffer, cts.Token);
                    if (bytesRead == 0)
                        endOfStream = true;
                    else
                        bytesInBuffer = bytesRead;
                }
                else
                {
                    // For regular requests, fill buffer completely if possible
                    while (bytesInBuffer < MaxHttpChunkSize)
                    {
                        var bytesRead = await stream.ReadAsync(
                            buffer.AsMemory(bytesInBuffer, MaxHttpChunkSize - bytesInBuffer),
                            cts.Token);

                        if (bytesRead == 0)
                        {
                            endOfStream = true;
                            break;
                        }
                        bytesInBuffer += bytesRead;
                    }
                }

                // Skip empty chunks (except first one which carries headers)
                if (bytesInBuffer == 0 && !isFirstChunk)
                    break;

                var response = new TunnelHttpResponse
                {
                    RequestId = request.RequestId,
                    StatusCode = (int)haResponse.StatusCode,
                    ReasonPhrase = haResponse.ReasonPhrase,
                    ProcessingTimeMs = processingTime,
                    HasMoreChunks = !endOfStream
                };

                // Only send headers on first chunk
                if (isFirstChunk)
                {
                    response.Headers = headers;
                    isFirstChunk = false;
                }

                if (bytesInBuffer > 0)
                {
                    response.SetBody(buffer.AsSpan(0, bytesInBuffer));
                }

                await SendMessageAsync(response, cancellationToken);
            }
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested)
        {
            // Request was cancelled (client disconnected or timeout) - no response needed
            _logger.LogDebug("Request {RequestId} was cancelled", request.RequestId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error handling request {RequestId}", request.RequestId);

            var errorResponse = TunnelHttpResponse.CreateError(
                request.RequestId,
                502,
                "Bad Gateway - Could not reach Home Assistant");

            await SendMessageAsync(errorResponse, cancellationToken);
        }
        finally
        {
            _pendingHttpRequests.TryRemove(request.RequestId, out _);
        }
    }

    private void HandleHttpRequestAborted(HttpRequestAbortedMessage aborted)
    {
        if (_pendingHttpRequests.TryRemove(aborted.RequestId, out var cts))
        {
            _logger.LogDebug("Aborting request {RequestId}: {Reason}", aborted.RequestId, aborted.Reason);
            cts.Cancel();
        }
    }

    private async Task HandleHeartbeatAsync(HeartbeatMessage heartbeat, CancellationToken cancellationToken)
    {
        var ack = new HeartbeatAckMessage
        {
            Sequence = heartbeat.Sequence
        };

        await SendMessageAsync(ack, cancellationToken);
    }

    #region WebSocket Proxying

    private async Task HandleWebSocketOpenAsync(WebSocketOpenMessage open, CancellationToken cancellationToken)
    {
        var response = new WebSocketOpenResponseMessage
        {
            ConnectionId = open.ConnectionId
        };

        try
        {
            var wsUrl = _homeAssistantUrl.Replace("http://", "ws://").Replace("https://", "wss://");
            wsUrl = $"{wsUrl}{open.Path}";

            var wsClient = await RawWebSocketClient.ConnectAsync(new Uri(wsUrl), _logger, open.Headers, cancellationToken);

            if (!_activeWebSockets.TryAdd(open.ConnectionId, wsClient))
            {
                await wsClient.CloseAsync(1011, "Duplicate connection");
                wsClient.Dispose();
                response.Success = false;
                response.ErrorMessage = "Duplicate connection ID";
            }
            else
            {
                response.Success = true;

                // Start receiving data from HA and relaying to server
                _ = RelayWebSocketFromHaAsync(open.ConnectionId, wsClient, cancellationToken);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to open WebSocket {ConnectionId}", open.ConnectionId);
            response.Success = false;
            response.ErrorMessage = ex.Message;
        }

        await SendMessageAsync(response, cancellationToken);
    }

    // Maximum chunk size for WebSocket data to reduce memory pressure from base64 encoding
    // 64KB raw = ~85KB base64, manageable for GC
    private const int MaxChunkSize = 65536;

    private async Task RelayWebSocketFromHaAsync(string connectionId, RawWebSocketClient wsClient, CancellationToken cancellationToken)
    {
        // Track the opcode of the current fragmented message
        WebSocketOpcode? fragmentedMessageOpcode = null;
        bool closeSent = false;

        try
        {
            while (!cancellationToken.IsCancellationRequested && wsClient.IsConnected)
            {
                var frame = await wsClient.ReceiveFrameAsync(cancellationToken);
                if (frame == null)
                    break;

                var opcode = frame.Value.Opcode;
                var payloadLength = frame.Value.PayloadLength;
                var isFinal = frame.Value.IsFinal;
                var payload = frame.Value.Payload;

                try
                {
                    switch (opcode)
                    {
                        case WebSocketOpcode.Close:
                            var statusCode = payloadLength >= 2
                                ? (payload[0] << 8) | payload[1]
                                : 1000;
                            var reason = payloadLength > 2
                                ? Encoding.UTF8.GetString(payload, 2, payloadLength - 2)
                                : null;

                            var closeMessage = new WebSocketCloseMessage
                            {
                                ConnectionId = connectionId,
                                StatusCode = statusCode,
                                Reason = reason
                            };
                            await SendMessageAsync(closeMessage, cancellationToken);
                            closeSent = true;
                            return;

                        case WebSocketOpcode.Ping:
                            await wsClient.SendFrameAsync(WebSocketOpcode.Pong, payload.AsMemory(0, payloadLength), true, cancellationToken);
                            continue;

                        case WebSocketOpcode.Pong:
                            continue;

                        case WebSocketOpcode.Text:
                        case WebSocketOpcode.Binary:
                            fragmentedMessageOpcode = opcode;
                            await SendChunkedDataAsync(connectionId, payload, payloadLength,
                                opcode == WebSocketOpcode.Text, isFinal, cancellationToken);
                            if (isFinal)
                                fragmentedMessageOpcode = null;
                            break;

                        case WebSocketOpcode.Continuation:
                            await SendChunkedDataAsync(connectionId, payload, payloadLength,
                                fragmentedMessageOpcode == WebSocketOpcode.Text, isFinal, cancellationToken);
                            if (isFinal)
                                fragmentedMessageOpcode = null;
                            break;
                    }
                }
                finally
                {
                    // Always return the payload buffer to the pool
                    if (payloadLength > 0)
                    {
                        RawWebSocketClient.ReturnBuffer(payload);
                    }
                }
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(ex, "WebSocket relay error for {ConnectionId}", connectionId);
        }
        finally
        {
            // TryRemove returns false if HandleWebSocketCloseAsync already cleaned up -
            // in that case the server initiated the close and already has final state,
            // so we must not send a second (unsolicited) close message.
            var wasActive = _activeWebSockets.TryRemove(connectionId, out _);
            _webSocketFragmentState.TryRemove(connectionId, out _);
            try { await wsClient.CloseAsync(1000, "Connection closed"); } catch { }
            wsClient.Dispose();

            // Notify server only if we owned the cleanup and haven't sent a close yet
            if (!closeSent && wasActive)
            {
                try
                {
                    var closeMsg = new WebSocketCloseMessage
                    {
                        ConnectionId = connectionId,
                        StatusCode = 1001,
                        Reason = "Connection lost"
                    };
                    await SendMessageAsync(closeMsg, CancellationToken.None);
                }
                catch { }
            }
        }
    }

    /// <summary>
    /// Sends payload data in chunks to reduce memory pressure from base64 encoding
    /// </summary>
    private async Task SendChunkedDataAsync(string connectionId, byte[] payload, int payloadLength,
        bool isText, bool isFinalFrame, CancellationToken cancellationToken)
    {
        int offset = 0;

        while (offset < payloadLength)
        {
            var chunkSize = Math.Min(MaxChunkSize, payloadLength - offset);
            var isLastChunk = (offset + chunkSize) >= payloadLength;

            var dataMessage = new WebSocketDataMessage
            {
                ConnectionId = connectionId,
                IsText = isText,
                // Only mark as end of message if this is the last chunk AND the original frame was final
                EndOfMessage = isLastChunk && isFinalFrame
            };
            dataMessage.SetData(payload.AsSpan(offset, chunkSize));
            await SendMessageAsync(dataMessage, cancellationToken);

            offset += chunkSize;
        }
    }

    private async Task HandleWebSocketDataAsync(WebSocketDataMessage data, CancellationToken cancellationToken)
    {
        if (!_activeWebSockets.TryGetValue(data.ConnectionId, out var wsClient))
        {
            // This can happen due to race conditions when a WebSocket is closing -
            // data messages may arrive after the connection was removed. This is normal.
            _logger.LogTrace("Received WebSocket data for unknown connection {ConnectionId} (likely already closed)", data.ConnectionId);
            return;
        }

        if (!wsClient.IsConnected)
            return;

        var bytes = data.GetDataBytes();
        if (bytes == null)
            return;

        try
        {
            // Determine the correct opcode based on WebSocket framing rules:
            // - First frame of a message uses Text (0x1) or Binary (0x2)
            // - Continuation frames must use Continuation (0x0)
            WebSocketOpcode opcode;
            var originalOpcode = data.IsText ? WebSocketOpcode.Text : WebSocketOpcode.Binary;

            if (_webSocketFragmentState.TryGetValue(data.ConnectionId, out var fragmentOpcode) && fragmentOpcode.HasValue)
            {
                // We're in a fragmented message - use Continuation opcode
                opcode = WebSocketOpcode.Continuation;
            }
            else
            {
                // Start of a new message - use the actual opcode
                opcode = originalOpcode;
            }

            // Update fragment state
            if (data.EndOfMessage)
            {
                // Message complete - clear fragment state
                _webSocketFragmentState.TryRemove(data.ConnectionId, out _);
            }
            else
            {
                // Message continues - store the original opcode for reference
                _webSocketFragmentState[data.ConnectionId] = originalOpcode;
            }

            await wsClient.SendFrameAsync(opcode, bytes.AsMemory(), data.EndOfMessage, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Failed to send to HA WebSocket {ConnectionId}", data.ConnectionId);
            // Clear fragment state on error to avoid corrupt state
            _webSocketFragmentState.TryRemove(data.ConnectionId, out _);
        }
    }

    private async Task HandleWebSocketCloseAsync(WebSocketCloseMessage close, CancellationToken cancellationToken)
    {
        // Clean up fragment state
        _webSocketFragmentState.TryRemove(close.ConnectionId, out _);

        if (_activeWebSockets.TryRemove(close.ConnectionId, out var wsClient))
        {
            try
            {
                await wsClient.CloseAsync((ushort)close.StatusCode, close.Reason);
            }
            catch { }
            wsClient.Dispose();
        }
    }

    #endregion

    private async Task SendMessageAsync(TunnelMessage message, CancellationToken cancellationToken)
    {
        await _sendLock.WaitAsync(cancellationToken);
        try
        {
            if (!IsConnected)
                return;

            var bytes = message.ToBytes();
            await _webSocket!.SendAsync(bytes, WebSocketMessageType.Text, true, cancellationToken);
        }
        finally
        {
            _sendLock.Release();
        }
    }

    private async Task<TunnelMessage?> ReceiveMessageAsync(CancellationToken cancellationToken)
    {
        // Use smaller buffer - WebSocket fragments large messages anyway
        var buffer = new byte[65536];
        using var ms = new MemoryStream();

        WebSocketReceiveResult result;
        do
        {
            result = await _webSocket!.ReceiveAsync(buffer, cancellationToken);
            ms.Write(buffer, 0, result.Count);
        }
        while (!result.EndOfMessage);

        if (result.MessageType == WebSocketMessageType.Text)
        {
            // Use GetBuffer() to avoid allocation
            return TunnelMessage.FromBytes(ms.GetBuffer().AsSpan(0, (int)ms.Length));
        }

        return null;
    }

    private static bool IsRestrictedHeader(string header)
    {
        // Headers that should not be forwarded to Home Assistant
        return header.Equals("Host", StringComparison.OrdinalIgnoreCase) ||
               header.Equals("Connection", StringComparison.OrdinalIgnoreCase) ||
               header.Equals("Content-Length", StringComparison.OrdinalIgnoreCase) ||
               header.Equals("Accept-Encoding", StringComparison.OrdinalIgnoreCase) ||
               header.Equals("Transfer-Encoding", StringComparison.OrdinalIgnoreCase) ||
               // Filter sec-* headers that can cause issues
               header.StartsWith("sec-", StringComparison.OrdinalIgnoreCase) ||
               // Filter X-Forwarded-* and X-Real-IP headers that cause 400 errors
               header.StartsWith("X-Forwarded-", StringComparison.OrdinalIgnoreCase) ||
               header.Equals("X-Real-IP", StringComparison.OrdinalIgnoreCase);
    }

    public async ValueTask DisposeAsync()
    {
        if (_cts != null)
        {
            await _cts.CancelAsync();
            _cts.Dispose();
        }

        // Close all active WebSocket connections
        foreach (var (_, ws) in _activeWebSockets)
        {
            if (ws.IsConnected)
            {
                try
                {
                    await ws.CloseAsync(1000, "Addon shutdown");
                }
                catch { }
            }
            ws.Dispose();
        }
        _activeWebSockets.Clear();

        if (_webSocket != null)
        {
            if (_webSocket.State == WebSocketState.Open)
            {
                try
                {
                    await _webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Addon shutdown", CancellationToken.None);
                }
                catch { }
            }
            _webSocket.Dispose();
        }

        _haHttpClient.Dispose();
        _sendLock.Dispose();
    }
}
