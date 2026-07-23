# --- build ---------------------------------------------------------------
# Pure-Rust deps now (tokio-tungstenite + sqlx-mysql + serde_json), no C
# toolchain needed — the presage/libsignal/sqlcipher build is gone.
FROM rust:1-bookworm AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
RUN cargo build --release && cp target/release/signal-archiver /signal-archiver

# --- runtime -------------------------------------------------------------
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
# 65532 is the conventional "nonroot" id, matched by k8s/04-ingester.yaml.
RUN groupadd --gid 65532 archiver \
    && useradd --uid 65532 --gid archiver --no-create-home --shell /usr/sbin/nologin archiver
COPY --from=build /signal-archiver /usr/local/bin/signal-archiver
USER archiver
ENTRYPOINT ["signal-archiver"]
