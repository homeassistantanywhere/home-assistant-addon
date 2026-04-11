namespace HAA.Shared;

/// <summary>
/// Shared constants used across the application
/// </summary>
public static class Constants
{
    /// <summary>
    /// The base domain for tunnel URLs
    /// </summary>
    public const string BaseDomain = "homeaccessanywhere.com";

    /// <summary>
    /// WebSocket endpoint path on the server
    /// </summary>
    public const string TunnelWebSocketPath = "/tunnel/ws";

    /// <summary>
    /// Default port for Home Assistant
    /// </summary>
    public const int DefaultHomeAssistantPort = 8123;

    /// <summary>
    /// Default timeout for HTTP requests through the tunnel
    /// </summary>
    public const int DefaultRequestTimeoutSeconds = 30;

    /// <summary>
    /// Heartbeat interval in seconds
    /// </summary>
    public const int HeartbeatIntervalSeconds = 30;

    /// <summary>
    /// Number of missed heartbeats before considering connection dead
    /// </summary>
    public const int MaxMissedHeartbeats = 3;

    /// <summary>
    /// Maximum size of a WebSocket message in bytes (1 MB)
    /// </summary>
    public const int MaxWebSocketMessageSize = 1 * 1024 * 1024;

    /// <summary>
    /// Initial reconnect delay in seconds
    /// </summary>
    public const int InitialReconnectDelaySeconds = 5;

    /// <summary>
    /// Maximum reconnect delay in seconds (for exponential backoff)
    /// </summary>
    public const int MaxReconnectDelaySeconds = 300;

    /// <summary>
    /// Application version
    /// </summary>
    public const string Version = "1.0.2";
}
