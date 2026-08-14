{-
signal/gate.dhall — this repository's commit gate.

Was `scripts/verify.sh`. What that script contained beyond these rows was
`set -euo pipefail`, a `cd` to the repository root, and a `nix develop -c bash -c`
wrapper around the middle three — none of which is about this repository. The
rows are.

The generated `gate.json` is committed and `the table matches its Dhall`
re-renders and diffs it, the way a lockfile is checked, so nothing here needs
`dhall` installed to run the gate.

**The vocabulary moved into the schema.** `inDevShell`, the clippy target
directory, the Angular worker cap, and the `ng-build` / `dev-lint` /
`check-table` rows were spelled out here and in a dozen other tables
identically — the duplication the shared tools were built to remove, recreated
one level up. They are `G.` values now. Two consequences the rendered JSON
shows: every dev-shell row gains `--no-warn-dirty`, because a gate that prints
"Git tree is dirty" on every row of every run has trained everyone to ignore a
warning; and dev-lint is pinned to its committed HEAD rather than run out of its
worktree, which is what stops a neighbour's half-finished edit failing this gate
for a reason no commit anywhere explains.

-}

let G = ../dev-lint/gate/schema.dhall

in  { name = "signal-archiver"
    , checks =
      [ G.Check::{
        , name = "formatting"
        , argv = G.inDevShell [ "cargo", "fmt", "--all", "--check" ]
        , timeout_s = 120
        }
      , G.Check::{
        , name = "clippy"
        , argv =
            G.inDevShell [ "cargo", "clippy", "--all-targets", "--", "-D", "warnings" ]
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
          env = G.clippyTarget
        , timeout_s = 900
        }
      , {-  ⚠ THROUGH A REAL MARIADB, and the row exists because `cargo test`
              alone would have SKIPPED the tests that matter most here. The
              importer's incremental behaviour — which files it decides not to
              read — is only testable against a database, so `tests/import_irclogs.rs`
              is written to skip when `SIGNAL_TEST_DATABASE_URL` is unset. Run
              without this, those four tests print "skipping" and the gate stays
              green while the one failure mode that produces no error, no warning
              and no row goes unchecked.

              Port 3320: fleetwatch's ephemeral server takes 3317, messages 3318
              and coach 3319, so the fleet gate can run them all at once.

              No `--grant-all`: this suite uses the one database it is given.
          -}
        G.Check::{
        , name = "tests (against a real MariaDB)"
        , argv =
              G.inDevShell [ "nix", "run", "../dev-lint#with-test-db", "--" ]
            # [ "--database"
              , "signal_test"
              , "--user"
              , "signal"
              , "--password"
              , "signal"
              , "--port"
              , "3320"
              , "--url-env"
              , "SIGNAL_TEST_DATABASE_URL"
              , "--"
              , "cargo"
              , "test"
              ]
        , timeout_s = 1800
        }
      , {-  The lockfile check: `gate.json` is what runs, `gate.dhall` is what
              typechecks, and this is what stops them drifting. `gate` does the
              comparison itself rather than a `bash -c` with process
              substitution, so this row is the same in every repository's table.
          -}
        G.checkTable "../dev-lint"
      , G.devLint "../"
      ]
    }
