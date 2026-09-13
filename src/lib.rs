//! signal-archiver library: the pure parsing logic (`parse` for the Signal
//! receive websocket, `irclog` for irssi's autologs, `telegram::map` for
//! Telegram's wire types) and the MariaDB store (`db`). The binaries wire these
//! to their feeds. Split into a lib so the parsing is unit-testable (see
//! `tests/`).

pub mod attach;
pub mod db;
pub mod irclog;
pub mod parse;
pub mod telegram;
