use super::Trace;

#[derive(Error, Debug)]
pub enum ChainError {
    #[error("Field {0} not found\nstack backtrace:\n{1}")]
    FieldNotFound(String, Trace<'static>),
}
