use super::Trace;

#[derive(Error, Debug)]
pub enum ChainError {
    #[error("Stream {stream} already exists\nstack backtrace:\n{trace}")]
    StreamAlreadyExists {
        stream: String,
        trace: Trace<'static>,
    },
    #[error("Stream {stream} not found\nstack backtrace:\n{trace}")]
    StreamNotFound {
        stream: String,
        trace: Trace<'static>,
    },
    #[error("Input {input_index} is already derived to output {output_index}\nstack backtrace:\n{trace}")]
    InputAlreadyDerived {
        input_index: usize,
        output_index: usize,
        trace: Trace<'static>,
    },
    #[error("Expected {expected} streams but got {actual}\nstack backtrace:\n{trace}")]
    UnexpectedNumberOfStreams {
        expected: usize,
        actual: usize,
        trace: Trace<'static>,
    },
    #[error("Field {field} not found\nstack backtrace:\n{trace}")]
    FieldNotFound {
        field: String,
        trace: Trace<'static>,
    },
    #[error("Invalid field name {name}\nstack backtrace:\n{trace}")]
    InvalidFieldName { name: String, trace: Trace<'static> },
    #[error("Invalid field type {r#type_name}\nstack backtrace:\n{trace}")]
    InvalidFieldType {
        type_name: String,
        trace: Trace<'static>,
    },
    #[error("Invalid token stream {name}: {msg}\nstack backtrace:\n{trace}")]
    InvalidTokenStream {
        name: String,
        msg: String,
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
    #[error("{msg}\nstack backtrace:\n{trace}")]
    Other { msg: String, trace: Trace<'static> },
}
