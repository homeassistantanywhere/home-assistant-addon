#!/usr/bin/env bash
set -e

CONFIG_PATH=/data/options.json
HA_CONFIG_PATH=/homeassistant/configuration.yaml
HA_STORAGE_HTTP=/homeassistant/.storage/http

# Read configuration
CONNECTION_KEY=$(jq -r '.connection_key' $CONFIG_PATH)

if [ -z "$CONNECTION_KEY" ] || [ "$CONNECTION_KEY" == "null" ]; then
    echo "============================================"
    echo "ERROR: Connection key not configured!"
    echo ""
    echo "1. Go to https://homeaccessanywhere.com"
    echo "2. Register and get your Connection Key"
    echo "3. Enter it in the addon configuration"
    echo "============================================"
    exit 1
fi

# Auto-detect Home Assistant URL
echo "Auto-detecting Home Assistant URL..."

# Default values
HA_PORT=8123
HA_SSL=false

# Since HA 2026.8 the http settings live in .storage/http (managed via the UI)
# and take precedence over any http block left in configuration.yaml
if [ -f "$HA_STORAGE_HTTP" ]; then
    CONFIGURED_PORT=$(jq -r '.data.server_port // ""' "$HA_STORAGE_HTTP" 2>/dev/null)
    if [ -n "$CONFIGURED_PORT" ] && [ "$CONFIGURED_PORT" != "null" ]; then
        HA_PORT=$CONFIGURED_PORT
    fi

    HAS_SSL_CERT=$(jq -r '.data.ssl_certificate // ""' "$HA_STORAGE_HTTP" 2>/dev/null)
    HAS_SSL_KEY=$(jq -r '.data.ssl_key // ""' "$HA_STORAGE_HTTP" 2>/dev/null)
    if [ -n "$HAS_SSL_CERT" ] && [ "$HAS_SSL_CERT" != "null" ] && \
       [ -n "$HAS_SSL_KEY" ] && [ "$HAS_SSL_KEY" != "null" ]; then
        HA_SSL=true
    fi
elif [ -f "$HA_CONFIG_PATH" ]; then
    # Pre-2026.8: read the http block from configuration.yaml
    CONFIGURED_PORT=$(yq '.http.server_port // ""' "$HA_CONFIG_PATH" 2>/dev/null)
    if [ -n "$CONFIGURED_PORT" ] && [ "$CONFIGURED_PORT" != "null" ]; then
        HA_PORT=$CONFIGURED_PORT
    fi

    # Check if SSL is configured
    HAS_SSL_CERT=$(yq '.http.ssl_certificate // ""' "$HA_CONFIG_PATH" 2>/dev/null)
    HAS_SSL_KEY=$(yq '.http.ssl_key // ""' "$HA_CONFIG_PATH" 2>/dev/null)
    if [ -n "$HAS_SSL_CERT" ] && [ "$HAS_SSL_CERT" != "null" ] && \
       [ -n "$HAS_SSL_KEY" ] && [ "$HAS_SSL_KEY" != "null" ]; then
        HA_SSL=true
    fi
else
    echo "Warning: Could not read Home Assistant configuration, using defaults"
fi

# Build URL
if [ "$HA_SSL" = "true" ]; then
    HOME_ASSISTANT_URL="https://homeassistant:${HA_PORT}"
else
    HOME_ASSISTANT_URL="http://homeassistant:${HA_PORT}"
fi

echo "Starting Home Access Anywhere..."
echo "Connection key: ${CONNECTION_KEY:0:8}..."
echo "Home Assistant URL: $HOME_ASSISTANT_URL"

# Configure environment
export ServerUrl="wss://api.homeaccessanywhere.com"
export ConnectionKey="$CONNECTION_KEY"
export HomeAssistantUrl="$HOME_ASSISTANT_URL"

# Run the addon
cd /app
exec dotnet HAA.Addon.dll
