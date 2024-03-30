
use crate::value::Value;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum KvError {
    #[error("Not Found for table:{0}, key:{1}")]
    NotFound(String, String),

    #[error("Cannot parse command: `{0}`")]
    InvalidCommand(String),

    #[error("Cannot convert value: {0:?} to {1}")]
    ConvertError(Value, &'static str),

    #[error("Cannot process command {0} with table: {1}, key:{2}, Error:{3}")]
    StorageError(&'static str, String, String, String),

    #[error("Failed to decode protobuf message")]
    EncodeError(#[from] prost::EncodeError),

    #[error("Failed to decode protobuf message")]
    DecodeError(#[from] prost::DecodeError),

    #[error("Internal error:{0}")]
    Internal(String),

    #[error("FrameError")]
    FrameError,

    #[error("TLS error")]
    TlsError(#[from] tokio_rustls::rustls::TLSError),

    #[error("Certificate parse error: error to load{0},{1}")]
    CertifacteParserError(&'static str, &'static str),

    #[error("I/O error")]
    IoError(#[from] std::io::Error),

    #[error("Yamux Connection error")]
    YamuxConnectionError(#[from] yamux::ConnectionError),
}

impl From<sled::Error> for KvError {
    fn from(value: sled::Error) -> Self {
        Self::Internal(value.to_string())
    }
}

// impl From<std::io::Error> for KvError {
//     fn from(value: std::io::Error) -> Self {
//         Self::Internal(value.to_string())
//     }
// }