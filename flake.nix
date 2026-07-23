# Dev shell for signal-archiver (Rust). Enter with: nix develop
# Pure-Rust deps (tokio-tungstenite + sqlx-mysql + reqwest, all plaintext
# in-cluster), so no openssl/pkg-config native dep — a bare Rust toolchain suffices.
{
  description = "signal-archiver — Signal receive-websocket → MariaDB ingester";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs = { self, nixpkgs }:
    let
      systems = [ "aarch64-darwin" "x86_64-linux" ];
      forAll = f: nixpkgs.lib.genAttrs systems (s: f nixpkgs.legacyPackages.${s});
    in {
      devShells = forAll (pkgs: {
        default = pkgs.mkShell {
          packages = [
            pkgs.cargo
            pkgs.rustc
            pkgs.rust-analyzer
            pkgs.rustfmt
            pkgs.clippy
          ];
        };
      });
    };
}
