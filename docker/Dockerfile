FROM ubuntu:22.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive \
    PG_MAJOR=17 \
    ZIG_VERSION=0.15.2

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        curl \
        gcc \
        gnupg2 \
        git \
        libc-dev \
        make \
        wget \
        xz-utils && \
    rm -rf /var/lib/apt/lists/*

RUN curl -sSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
    echo "deb http://apt.postgresql.org/pub/repos/apt/ jammy-pgdg main" > /etc/apt/sources.list.d/pgdg.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        postgresql-server-dev-$PG_MAJOR && \
    rm -rf /var/lib/apt/lists/*

ARG TARGETARCH=amd64
RUN MACHINE="$(uname -m)" && \
    case "${MACHINE}" in \
      aarch64|arm64) RUNTIME_ARCH=arm64;; \
      x86_64|amd64) RUNTIME_ARCH=amd64;; \
      *) RUNTIME_ARCH="${TARGETARCH}";; \
    esac && \
    case "${RUNTIME_ARCH}" in \
      arm64) ZIG_ARCH=aarch64-linux; ZIG_TARGET=aarch64-linux-gnu;; \
      *) ZIG_ARCH=x86_64-linux; ZIG_TARGET=x86_64-linux-gnu;; \
    esac && \
    printf '%s' "${ZIG_TARGET}" > /tmp/zig_target && \
    cd /tmp && \
    wget https://ziglang.org/download/${ZIG_VERSION}/zig-${ZIG_ARCH}-${ZIG_VERSION}.tar.xz && \
    tar -xf zig-${ZIG_ARCH}-${ZIG_VERSION}.tar.xz && \
    mv zig-${ZIG_ARCH}-${ZIG_VERSION} /usr/local/zig && \
    ln -s /usr/local/zig/zig /usr/local/bin/zig && \
    rm zig-${ZIG_ARCH}-${ZIG_VERSION}.tar.xz

WORKDIR /tmp/pg_mindbrain

COPY src/ ./src/
COPY deps/ ./deps/
COPY sql/ ./sql/
COPY build.zig ./
COPY pg_mindbrain.control ./
COPY META.json ./

RUN cd deps/pg_roaringbitmap && \
    make clean && \
    make && \
    make install

RUN ZIG_TARGET="$(tr -d '\n' </tmp/zig_target)" && \
    zig build -Dtarget="${ZIG_TARGET}" -Doptimize=ReleaseFast && \
    test -s zig-out/lib/libpg_mindbrain.so

FROM pgvector/pgvector:pg17

ENV DEBIAN_FRONTEND=noninteractive \
    PG_MAJOR=17

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        postgresql-contrib-$PG_MAJOR && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /tmp/pg_mindbrain/zig-out/lib/libpg_mindbrain.so /usr/lib/postgresql/$PG_MAJOR/lib/libpg_mindbrain.so
RUN ln -sf libpg_mindbrain.so /usr/lib/postgresql/$PG_MAJOR/lib/pg_mindbrain.so

COPY --from=builder /tmp/pg_mindbrain/pg_mindbrain.control /usr/share/postgresql/$PG_MAJOR/extension/
COPY --from=builder /tmp/pg_mindbrain/sql/sqlite_mindbrain--1.0.0.sql /usr/share/postgresql/$PG_MAJOR/extension/
COPY --from=builder /tmp/pg_mindbrain/sql/stopwords/import_bm25_stopwords.sql /docker-entrypoint-initdb.d/02-import-bm25-stopwords.sql

COPY --from=builder /usr/lib/postgresql/$PG_MAJOR/lib/roaringbitmap.so /usr/lib/postgresql/$PG_MAJOR/lib/roaringbitmap.so
COPY --from=builder /tmp/pg_mindbrain/deps/pg_roaringbitmap/roaringbitmap.control /usr/share/postgresql/$PG_MAJOR/extension/
COPY --from=builder /tmp/pg_mindbrain/deps/pg_roaringbitmap/roaringbitmap--*.sql /usr/share/postgresql/$PG_MAJOR/extension/

COPY docker/init/01-init-postgres.sql /docker-entrypoint-initdb.d/

EXPOSE 5432
CMD ["postgres"]
