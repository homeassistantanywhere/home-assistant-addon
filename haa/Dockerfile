# Multi-arch Dockerfile for Home Assistant Addon
ARG BUILD_FROM
FROM ${BUILD_FROM}

# Install dependencies
RUN apk add --no-cache \
    bash \
    icu-libs \
    krb5-libs \
    libgcc \
    libintl \
    libssl3 \
    libstdc++ \
    zlib \
    yq

# Download and install .NET Runtime
ARG DOTNET_VERSION=8.0.11
ARG TARGETARCH
# TARGETARCH is only set by BuildKit (CI); fall back to uname -m for
# local Supervisor builds that use the legacy builder
RUN ARCH="${TARGETARCH:-$(uname -m)}" && \
    case "$ARCH" in \
        amd64|x86_64)  DOTNET_ARCH="x64" ;; \
        arm64|aarch64) DOTNET_ARCH="arm64" ;; \
        arm|armv7l)    DOTNET_ARCH="arm" ;; \
        *) echo "Unsupported architecture: $ARCH" && exit 1 ;; \
    esac && \
    wget -O dotnet.tar.gz "https://builds.dotnet.microsoft.com/dotnet/Runtime/${DOTNET_VERSION}/dotnet-runtime-${DOTNET_VERSION}-linux-musl-${DOTNET_ARCH}.tar.gz" && \
    mkdir -p /usr/share/dotnet && \
    tar -oxzf dotnet.tar.gz -C /usr/share/dotnet && \
    rm dotnet.tar.gz && \
    ln -s /usr/share/dotnet/dotnet /usr/bin/dotnet

WORKDIR /app

# Copy pre-built application
COPY rootfs/ /

# Set permissions
RUN chmod +x /run.sh

CMD ["/run.sh"]
