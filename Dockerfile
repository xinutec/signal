# --- build ---------------------------------------------------------------
# Pure-Rust deps now (tokio-tungstenite + sqlx-mysql + serde_json), no C
# toolchain needed — the presage/libsignal/sqlcipher build is gone.
FROM rust:1-bookworm AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
RUN cargo build --release \
    && cp target/release/signal-archiver /signal-archiver \
    && cp target/release/import_irclogs /import_irclogs

# --- runtime -------------------------------------------------------------
FROM debian:bookworm-slim
# rsync + openssh-client are for the IRC importer alone: irssi's autologs live on
# a PVC in a *different* k3s cluster (amun), so they are pulled over ssh before
# each import rather than mounted. The key that does it is restricted to
# `rrsync -ro` on the far side, so this image cannot get a shell there.
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates openssh-client rsync \
    && rm -rf /var/lib/apt/lists/*
# 65532 is the conventional "nonroot" id, matched by k8s/04-ingester.yaml.
RUN groupadd --gid 65532 archiver \
    && useradd --uid 65532 --gid archiver --no-create-home --shell /usr/sbin/nologin archiver
COPY --from=build /signal-archiver /usr/local/bin/signal-archiver
COPY --from=build /import_irclogs /usr/local/bin/import_irclogs
USER archiver
# The ingester is what this image runs by default; the IRC importer is a CronJob
# that overrides the command, because it is a periodic pull rather than a daemon.
ENTRYPOINT ["signal-archiver"]
