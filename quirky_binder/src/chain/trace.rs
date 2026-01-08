use std::{borrow::Cow, fmt::Display};

use quirky_binder_lang::location::Location;

/// Trace used to determine where errors are located.
///
/// A trace contains elements from the leaf location to the root location.
#[derive(Clone, Debug)]
pub struct Trace<'a> {
    elements: Vec<TraceElement<'a>>,
}

impl<'a> Trace<'a> {
    /// Creates a new trace to be progressively filled by calling [`push`](Self::push).
    pub fn new_leaf(element: TraceElement<'a>) -> Self {
        Trace {
            elements: vec![element],
        }
    }

    /// Pushes a new element to the trace.
    ///
    /// An element is pushed whenever its location is insightful to debug errors.
    pub fn push(mut self, element: TraceElement<'a>) -> Self {
        self.elements.push(element);
        self
    }

    /// Converts the trace to a fully owned instance.
    pub fn to_owned(&self) -> Trace<'static> {
        Trace {
            elements: self.elements.iter().map(TraceElement::to_owned).collect(),
        }
    }
}

impl Display for Trace<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (index, element) in self.elements.iter().enumerate() {
            f.write_fmt(format_args!("{index:4}: {element}"))?;
        }
        Ok(())
    }
}

/// An element of a [`Trace`].
///
/// It contains:
///
/// * a source file name
/// * a name identifying a specific item in the source file (usually a filter name)
/// * a location in the source file (line and column)
#[derive(Clone, Debug, new)]
pub struct TraceElement<'a> {
    source: Cow<'a, str>,
    name: Cow<'a, str>,
    location: Location,
}

impl TraceElement<'_> {
    /// Converts the element to a fully owned instance.
    pub fn to_owned(&self) -> TraceElement<'static> {
        TraceElement {
            source: self.source.clone().into_owned().into(),
            name: self.name.clone().into_owned().into(),
            location: self.location,
        }
    }
}

impl Display for TraceElement<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "{}\n             at {}:{}:{}\n",
            self.name, self.source, self.location.line, self.location.col
        ))
    }
}

/// Trait to implement in order to attach [`Trace`] capabilities.
pub trait WithTraceElement {
    type WithTraceType;

    fn with_trace_element<Element>(self, element: Element) -> Self::WithTraceType
    where
        Element: Fn() -> TraceElement<'static>;
}

/// Generates a callback responsible for creating a [`TraceElement`] with the current source file,
/// the provided name, and the location in the current source file.
#[macro_export]
macro_rules! trace_element {
    ($name:expr) => {
        || {
            TraceElement::new(
                std::file!().into(),
                ($name).into(),
                $crate::chain::Location::new(std::line!() as usize, std::column!() as usize),
            )
            .to_owned()
        }
    };
}
