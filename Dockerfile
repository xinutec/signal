# --- build ---------------------------------------------------------------
# Pure-Rust deps now (tokio-tungstenite + sqlx-mysql + serde_json), no C
# toolchain needed — the presage/libsignal/sqlcipher build is gone.
FROM rust:1-bookworm AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
RUN cargo build --release \
    && cp target/release/signal-archiver /signal-archiver \
    && cp target/release/import_irclogs /import_irclogs \
    && cp target/release/irc_tail /irc_tail

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
COPY --from=build /irc_tail /usr/local/bin/irc_tail
USER archiver
# Three programs, one image, because they share the schema and the parser: the
# ingester (default), the IRC importer that a CronJob runs periodically, and
# `irc_tail`, the Deployment that holds a long poll open to irssi so a line
# reaches the archive in under a second. The importer is the reconciler for what
# `irc_tail` misses; they write the same rows on the same dedupe key, which is
# only true because they share `irclog.rs`.
ENTRYPOINT ["signal-archiver"]
