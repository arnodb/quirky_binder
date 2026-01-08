use crate::chain::{
    error::{ChainError, ChainErrorWithTrace},
    trace::{TraceElement, WithTraceElement},
};

/// A [`Result`] where the error is a chain error with a trace.
pub type ChainResultWithTrace<T> = Result<T, ChainErrorWithTrace>;

/// A [`Result`] where the error is a simple chain error without any additional information.
///
/// Use [`with_trace_element`](WithTraceElement::with_trace_element) to attach a trace to it.
pub type ChainResult<T> = Result<T, ChainError>;

impl<T> WithTraceElement for ChainResult<T> {
    type WithTraceType = ChainResultWithTrace<T>;

    fn with_trace_element<Element>(self, element: Element) -> Self::WithTraceType
    where
        Element: Fn() -> TraceElement<'static>,
    {
        self.map_err(|err| err.with_trace_element(element))
    }
}

impl<T> WithTraceElement for ChainResultWithTrace<T> {
    type WithTraceType = ChainResultWithTrace<T>;

    fn with_trace_element<Element>(self, element: Element) -> Self::WithTraceType
    where
        Element: Fn() -> TraceElement<'static>,
    {
        self.map_err(|err| err.with_trace_element(element))
    }
}
