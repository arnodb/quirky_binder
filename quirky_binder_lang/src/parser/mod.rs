use std::borrow::Cow;

use crate::ast::{Module, StreamLineInput};

#[cfg(feature = "crafted_parser")]
pub mod crafted_impl;

#[cfg(feature = "crafted_parser")]
pub fn parse_crafted(input: &str) -> Result<Module<'_>, SpannedError<&str>> {
    use self::crafted_impl::lexer::lexer;
    let mut lexer = lexer(input);
    self::crafted_impl::module(&mut lexer)
}

#[cfg(feature = "nom_parser")]
pub mod nom_impl;

#[cfg(feature = "nom_parser")]
pub fn parse_nom(input: &str) -> Result<Module<'_>, SpannedError<&str>> {
    self::nom_impl::module(input)
        .map(|(tail, module)| {
            assert_eq!("", tail);
            module
        })
        .map_err(|err| err.to_spanned_error(input))
}

#[cfg(not(any(feature = "crafted_parser", feature = "nom_parser")))]
compile_error!("Need to have at least one parser");

pub fn parse_module(input: &str) -> Result<Module<'_>, SpannedError<&str>> {
    // TODO outside customization
    match None::<()> {
        Some(_) => {
            unreachable!();
        }
        None => {
            // Default
            #[cfg(feature = "crafted_parser")]
            {
                parse_crafted(input)
            }
            #[cfg(all(not(feature = "crafted_parser"), feature = "nom_parser"))]
            {
                parse_nom(input)
            }
        }
    }
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct SpannedError<I> {
    pub kind: SpannedErrorKind,
    pub span: I,
}

#[cfg(feature = "nom_parser")]
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

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum SpannedErrorKind {
    Identifier,
    Token(&'static str),
    Code,
    UnbalancedCode,
    UnterminatedChar,
    UnterminatedString,
    UnrecognizedToken,
    AnnotationsParseError(ron::de::SpannedError),
    // Nom errors that have not been mapped but should probably be
    #[cfg(feature = "nom_parser")]
    Nom(nom::error::ErrorKind),
    #[cfg(feature = "nom_parser")]
    NomIncomplete,
}

impl SpannedErrorKind {
    pub fn description(&self) -> Cow<'_, str> {
        match self {
            Self::Identifier => "identifier".into(),
            Self::Token(token) => (*token).into(),
            Self::Code => "code".into(),
            Self::UnbalancedCode => "unbalanced code".into(),
            Self::UnterminatedChar => "unterminated char".into(),
            Self::UnterminatedString => "unterminated string".into(),
            Self::UnrecognizedToken => "unrecognized token".into(),
            Self::AnnotationsParseError(err) => format!("annotations parse error: {err}").into(),
            #[cfg(feature = "nom_parser")]
            Self::Nom(nom) => nom.description().into(),
            #[cfg(feature = "nom_parser")]
            Self::NomIncomplete => "incomplete".into(),
        }
    }
}

pub trait ToSpannedError<'a> {
    fn to_spanned_error(&self, input: &'a str) -> SpannedError<&'a str>;
}

impl<'a> ToSpannedError<'a> for SpannedError<&'a str> {
    fn to_spanned_error(&self, _input: &'a str) -> SpannedError<&'a str> {
        self.clone()
    }
}

#[cfg(feature = "nom_parser")]
impl<'a> ToSpannedError<'a> for nom::Err<SpannedError<&'a str>> {
    fn to_spanned_error(&self, input: &'a str) -> SpannedError<&'a str> {
        match self {
            nom::Err::Incomplete(_) => SpannedError {
                kind: SpannedErrorKind::NomIncomplete,
                span: &input[input.len()..],
            },
            nom::Err::Error(err) | nom::Err::Failure(err) => err.clone(),
        }
    }
}

fn assemble_inputs<'a>(
    first: StreamLineInput<'a>,
    extra_streams: Vec<&'a str>,
) -> Vec<StreamLineInput<'a>> {
    Some(first)
        .into_iter()
        .chain(extra_streams.into_iter().map(StreamLineInput::Named))
        .collect()
}

#[cfg(test)]
pub mod fuzzer;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod fuzz_tests;
