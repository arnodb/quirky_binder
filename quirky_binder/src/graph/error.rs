use crate::chain::error::ChainErrorWithTrace;

#[derive(Error, Debug)]
pub enum GraphGenerationError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Error: {0}")]
    Chain(#[from] ChainErrorWithTrace),
}
