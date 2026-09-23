# --- build ---------------------------------------------------------------
# Pure Rust; no C toolchain.
#
# Four binaries sharing the schema: the Signal ingester (default entrypoint),
# the IRC importer CronJob, `irc_tail`, and `telegram`. Each must also be
# copied into the runtime stage, or the image builds and then crashloops.
FROM rust:1-bookworm AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
RUN cargo build --release \
    && cp target/release/signal-archiver /signal-archiver \
    && cp target/release/import_irclogs /import_irclogs \
    && cp target/release/irc_tail /irc_tail \
    && cp target/release/telegram /telegram

# --- runtime -------------------------------------------------------------
FROM debian:bookworm-slim
# rsync + openssh-client pull irssi's autologs from amun's cluster for the IRC
# importer; the key is restricted to `rrsync -ro` there.
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates openssh-client rsync \
    && rm -rf /var/lib/apt/lists/*
# 65532, the conventional "nonroot" id.
RUN groupadd --gid 65532 archiver \
    && useradd --uid 65532 --gid archiver --no-create-home --shell /usr/sbin/nologin archiver
COPY --from=build /signal-archiver /usr/local/bin/signal-archiver
COPY --from=build /import_irclogs /usr/local/bin/import_irclogs
COPY --from=build /irc_tail /usr/local/bin/irc_tail
COPY --from=build /telegram /usr/local/bin/telegram
USER archiver
ENTRYPOINT ["signal-archiver"]
