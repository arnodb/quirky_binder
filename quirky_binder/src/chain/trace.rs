use std::{borrow::Cow, cell::Cell, fmt::Display};

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

thread_local! {
    pub static TRACE_NAME: Cell<(usize, &'static str)> = const { Cell::new((0, "unknown")) };
}

pub struct TraceName {
    parent_level: usize,
    parent_name: &'static str,
    name: &'static str,
}

impl TraceName {
    pub fn push(name: &'static str) -> Self {
        let (parent_level, parent_name) = TRACE_NAME.get();
        TRACE_NAME.set((parent_level + 1, name));
        Self {
            parent_level,
            parent_name,
            name,
        }
    }
}

impl Drop for TraceName {
    fn drop(&mut self) {
        let (level, name) = TRACE_NAME.get();
        if level != self.parent_level + 1 || name != self.name {
            panic!(
                "Expected ({}, {}), found ({}, {})",
                self.parent_level + 1,
                self.name,
                level,
                name
            );
        }
        TRACE_NAME.set((self.parent_level, self.parent_name));
    }
}

/// Generates a callback responsible for creating a [`TraceElement`] with the current source file,
/// the current trace name, and the location in the current source file.
#[macro_export]
macro_rules! trace_element {
    () => {
        || {
            TraceElement::new(
                std::file!().into(),
                $crate::chain::trace::TRACE_NAME.get().1.into(),
                $crate::chain::Location::new(std::line!() as usize, std::column!() as usize),
            )
            .to_owned()
        }
    };
}
