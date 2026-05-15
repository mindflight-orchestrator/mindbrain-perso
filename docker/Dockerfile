FROM ubuntu:24.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive \
    ZIG_VERSION=0.16.0

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        libsqlite3-dev \
        wget \
        xz-utils && \
    rm -rf /var/lib/apt/lists/*

ARG TARGETARCH=amd64
RUN MACHINE="$(uname -m)" && \
    case "${MACHINE}" in \
      aarch64|arm64) ZIG_ARCH=aarch64-linux;; \
      *) ZIG_ARCH=x86_64-linux;; \
    esac && \
    cd /tmp && \
    wget -q https://ziglang.org/download/${ZIG_VERSION}/zig-${ZIG_ARCH}-${ZIG_VERSION}.tar.xz && \
    tar -xf zig-${ZIG_ARCH}-${ZIG_VERSION}.tar.xz && \
    mv zig-${ZIG_ARCH}-${ZIG_VERSION} /usr/local/zig && \
    ln -s /usr/local/zig/zig /usr/local/bin/zig && \
    rm zig-${ZIG_ARCH}-${ZIG_VERSION}.tar.xz

WORKDIR /app

COPY src/ ./src/
COPY deps/ ./deps/
COPY build.zig build.zig.zon ./

RUN zig build standalone-tool standalone-http benchmark-tool -Doptimize=ReleaseFast

# ── runtime ──────────────────────────────────────────────────────────────────
FROM ubuntu:24.04

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libsqlite3-0 && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/zig-out/bin/ /usr/local/bin/

EXPOSE 8080
CMD ["mindbrain-http"]
