{-
signal/gate.dhall — this repository's commit gate.

Was `scripts/verify.sh`. What that script contained beyond these rows was
`set -euo pipefail`, a `cd` to the repository root, and a `nix develop -c bash -c`
wrapper around the middle three — none of which is about this repository. The
rows are.

The generated `gate.json` is committed and `the table matches its Dhall`
re-renders and diffs it, the way a lockfile is checked, so nothing here needs
`dhall` installed to run the gate.
-}

let G = ../dev-lint/gate/schema.dhall

let inDevShell =
      \(argv : List Text) ->
        [ "nix", "develop", "--command" ] # argv

in  { name = "signal-archiver"
    , checks =
      [ G.Check::{
        , name = "formatting"
        , argv = inDevShell [ "cargo", "fmt", "--all", "--check" ]
        , timeout_s = 120
        }
      , G.Check::{
        , name = "clippy"
        , argv =
            inDevShell [ "cargo", "clippy", "--all-targets", "--", "-D", "warnings" ]
        , {-  Clippy gets its own target directory: clippy-driver and rustc
              fingerprint the workspace differently and evict each other in a
              shared one, forcing a full recompile. A dedicated directory keeps
              both caches warm.

              The shell spelled this `${CARGO_CLIPPY_TARGET_DIR:-$HOME/...}`, so
              it could be overridden per machine. Nothing overrode it, and a
              table cannot read `$HOME` — so the path is stated, and if a machine
              ever needs a different one that is an edit here rather than an
              environment variable nobody knew about.
          -}
          env = toMap { CARGO_TARGET_DIR = "/Users/pippijn/.cache/cargo/clippy-target" }
        , timeout_s = 900
        }
      , G.Check::{
        , name = "tests"
        , argv = inDevShell [ "cargo", "test" ]
        , timeout_s = 900
        }
      , {-  The lockfile check: `gate.json` is what runs, `gate.dhall` is what
              typechecks, and this is what stops them drifting. `gate` does the
              comparison itself rather than a `bash -c` with process
              substitution, so this row is the same in every repository's table.
          -}
        G.Check::{
        , name = "the table matches its Dhall"
        , argv =
            [ "nix", "run", "../dev-lint#gate", "--", "--check-table", "gate.dhall", "gate.json" ]
        , timeout_s = 120
        }
      , G.Check::{
        , name = "dev-lint"
        , argv = [ "nix", "run", "../dev-lint", "--", "." ]
        , timeout_s = 600
        }
      ]
    }
