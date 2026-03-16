//! Logging utilities

// a temporary solution while we work on something better.
// This needs to be replaced by a proper logging solution that prolog can hook into.

#[cfg(feature = "eprint_log")]
#[macro_export]
macro_rules! chrono_log {
    ($msg:expr) => {
        eprint!("{:?}: ", chrono::offset::Local::now());
        eprintln!($msg)
    };
    ($format:expr, $($arg:expr),+) => {
        eprint!("{:?}: ", chrono::offset::Local::now());
        eprintln!($format, ($($arg),+))
    }
}

#[cfg(not(feature = "eprint_log"))]
#[macro_export]
macro_rules! chrono_log {
    ($msg:expr) => {};
    ($format:expr, $($arg:expr),+) => {};
}
