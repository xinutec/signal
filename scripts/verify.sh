#!/usr/bin/env bash
# signal-archiver verify — rust backend (fmt + clippy + tests) + shared dev-lint
# rules. Toolchain comes from the flake devshell (rev-pinned via flake.lock), so
# it's reproducible without cargo on PATH. The frame-parser tests (tests/parse.rs)
# are pure units — no MariaDB needed (the app uses runtime sqlx::query, no compile-
# time query! macros).
set -euo pipefail
cd "$(dirname "$0")/.."
nix develop -c bash -c '
  set -euo pipefail
  cargo fmt --all --check
  # Clippy gets its own target dir: clippy-driver and rustc fingerprint the
  # workspace differently and evict each other in a shared dir, forcing a full
  # recompile. A dedicated dir keeps both caches warm.
  CARGO_TARGET_DIR="${CARGO_CLIPPY_TARGET_DIR:-$HOME/.cache/cargo/clippy-target}" \
    cargo clippy --all-targets -- -D warnings
  cargo test
'
dev_lint_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/dev-lint"
[ -d "$dev_lint_dir" ] || dev_lint_dir="$HOME/Code/dev-lint"
[ -d "$dev_lint_dir" ] || dev_lint_dir="$HOME/code/dev-lint"
nix run "$dev_lint_dir" -- . # dev-lint
