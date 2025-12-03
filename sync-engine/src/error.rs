use std::fmt;

#[derive(Debug)]
pub enum Error {
    Storage(String),
    NotFound { pk: String, col: Option<String> },
    Conflict { pk: String, col: String, local_version: u64, remote_version: u64 },
    InvalidState(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Storage(msg) => write!(f, "storage error: {}", msg),
            Error::NotFound { pk, col: Some(col) } => write!(f, "not found: {}:{}", pk, col),
            Error::NotFound { pk, col: None } => write!(f, "not found: {}", pk),
            Error::Conflict { pk, col, local_version, remote_version } => {
                write!(f, "conflict at {}:{} (local v{}, remote v{})", pk, col, local_version, remote_version)
            }
            Error::InvalidState(msg) => write!(f, "invalid state: {}", msg),
        }
    }
}

impl std::error::Error for Error {}

impl From<rusqlite::Error> for Error {
    fn from(e: rusqlite::Error) -> Self {
        Error::Storage(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;
