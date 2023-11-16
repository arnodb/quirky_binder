use ::nom::error::ErrorKind;

pub mod nom_impl;

#[cfg(feature = "crafted_parser")]
pub mod crafted_impl;

pub use nom_impl::module;

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct SpannedError<I> {
    pub kind: SpannedErrorKind,
    pub span: I,
}

impl<I> nom::error::ParseError<I> for SpannedError<I> {
    fn from_error_kind(input: I, kind: nom::error::ErrorKind) -> Self {
        SpannedError {
            kind: SpannedErrorKind::Nom(kind),
            span: input,
        }
    }

    fn append(_input: I, _kind: nom::error::ErrorKind, other: Self) -> Self {
        other
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SpannedErrorKind {
    Identifier,
    Token(&'static str),
    // Nom errors that have not been mapped but should probably be
    Nom(ErrorKind),
    NomIncomplete,
}

impl SpannedErrorKind {
    pub fn description(&self) -> &str {
        match self {
            Self::Identifier => "identifier",
            Self::Token(token) => token,
            Self::Nom(nom) => nom.description(),
            Self::NomIncomplete => "incomplete",
        }
    }
}

#[cfg(test)]
pub mod fuzzer;

#[cfg(test)]
mod fuzz_tests;
