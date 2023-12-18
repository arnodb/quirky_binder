use super::Trace;

#[derive(Error, Debug)]
pub enum ChainError {
    #[error("Field {field} not found\nstack backtrace:\n{trace}")]
    FieldNotFound {
        field: String,
        trace: Trace<'static>,
    },
    #[error("Expected {more_info} with minimal order {expected} but found {actual}\nstack backtrace:\n{trace}")]
    ExpectedMinimalOrder {
        more_info: String,
        expected: String,
        actual: String,
        trace: Trace<'static>,
    },
    #[error("Expected {more_info} with distinct {expected} but found {actual}\nstack backtrace:\n{trace}")]
    ExpectedDistinct {
        more_info: String,
        expected: String,
        actual: String,
        trace: Trace<'static>,
    },
}
