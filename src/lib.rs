//! signal-archiver library: the pure parsing logic (`parse` for the Signal
//! receive websocket, `irclog` for irssi's autologs) and the MariaDB store
//! (`db`). The binary (`main.rs`) wires these to the receive websocket.
//! Split into a lib so the parsing is unit-testable (see `tests/`).

pub mod attach;
pub mod db;
pub mod irclog;
pub mod parse;
