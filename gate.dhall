{-
signal/gate.dhall — this repository's commit gate.

The generated `gate.json` is committed, and `the table matches its Dhall`
re-renders and diffs it, so running the gate needs no `dhall`.

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
              fingerprint differently and would evict each other's cache.
          -}
          env = G.clippyTarget
        , timeout_s = 900
        }
      , G.cargoDoc
      , {-  Against a real MariaDB: without `SIGNAL_TEST_DATABASE_URL` the SQL
              tests skip.

              The port must be unique across the fleet's gates, which run
              concurrently; check with a grep for `"--port"` across every
              gate.dhall.
          -}
        G.Check::{
        , name = "tests (against a real MariaDB)"
        , argv =
            G.withTestDb
              "../"
              [ "--database"
              , "signal_test"
              , "--user"
              , "signal"
              , "--password"
              , "signal"
              , "--port"
              , "3322"
              , "--url-env"
              , "SIGNAL_TEST_DATABASE_URL"
              , "--"
              , "cargo"
              , "test"
              ]
        , timeout_s = 1800
        }
      , G.checkTable "../dev-lint"
      , G.devLint "../"
      ]
    }
