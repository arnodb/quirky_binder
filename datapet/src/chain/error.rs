#[derive(Error, Debug)]
pub enum ChainError {
    #[error("Field {0} not found")]
    FieldNotFound(String),
}
