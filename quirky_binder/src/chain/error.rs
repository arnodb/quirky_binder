use super::{Trace, TraceElement, WithTraceElement};

#[derive(Error, Debug)]
pub enum ChainError {
    #[error("Stream {stream} already exists")]
    StreamAlreadyExists { stream: String },
    #[error("Stream {stream} not found")]
    StreamNotFound { stream: String },
    #[error("Input {input_index} is already derived to output {output_index}")]
    InputAlreadyDerived {
        input_index: usize,
        output_index: usize,
    },
    #[error("Expected {expected} streams but got {actual}")]
    UnexpectedNumberOfStreams { expected: usize, actual: usize },
    #[error("Field {field} not found")]
    FieldNotFound { field: String },
    #[error("Invalid field name {name}")]
    InvalidFieldName { name: String },
    #[error("Invalid field type {r#type_name}")]
    InvalidFieldType { type_name: String },
    #[error("Invalid token stream {name}: {msg}")]
    InvalidTokenStream { name: String, msg: String },
    #[error("Expected {more_info} with minimal order {expected} but found {actual}")]
    ExpectedMinimalOrder {
        more_info: String,
        expected: String,
        actual: String,
    },
    #[error("Expected {more_info} with distinct {expected} but found {actual}")]
    ExpectedDistinct {
        more_info: String,
        expected: String,
        actual: String,
    },
    #[error("{msg}")]
    Other { msg: String },
}

impl WithTraceElement for ChainError {
    type WithTraceType = ChainErrorWithTrace;

    fn with_trace_element<Element>(self, element: Element) -> Self::WithTraceType
    where
        Element: Fn() -> TraceElement<'static>,
    {
        ChainErrorWithTrace(self, Trace::new_leaf(element()))
    }
}

#[derive(Error, Debug)]
#[error("{0}\nstack backtrace:\n{1}")]
pub struct ChainErrorWithTrace(ChainError, Trace<'static>);

impl WithTraceElement for ChainErrorWithTrace {
    type WithTraceType = ChainErrorWithTrace;

    fn with_trace_element<Element>(self, element: Element) -> Self::WithTraceType
    where
        Element: Fn() -> TraceElement<'static>,
    {
        ChainErrorWithTrace(self.0, self.1.push(element()))
    }
}
