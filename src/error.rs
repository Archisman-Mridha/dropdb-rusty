pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
  #[error("{0}")]
  IO(#[from] std::io::Error),

  #[error("Failed encoding WAL entry : {0}")]
  EncodingWALEntry(#[from] prost::EncodeError),

  #[error("Failed decoding WAL entry : {0}")]
  DecodingWALEntry(#[from] prost::DecodeError),

  #[error("Corrupted WAL entry (lsn = {lsn})")]
  CorruptedWALEntry {
    lsn: u32
  }
}
