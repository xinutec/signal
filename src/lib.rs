//! signal-archiver library: pure parsing (`parse` for Signal frames, `irclog`
//! for irssi autologs, `telegram::map` for Telegram) and the MariaDB store
//! (`db`). The binaries connect these to their feeds.

pub mod attach;
pub mod db;
pub mod irclog;
pub mod parse;
pub mod telegram;
