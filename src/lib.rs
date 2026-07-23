//! signal-archiver library: the pure parsing logic (`parse`) and the MariaDB
//! store (`db`). The binary (`main.rs`) wires these to the receive websocket.
//! Split into a lib so the parsing is unit-testable (see `tests/`).

pub mod db;
pub mod parse;
