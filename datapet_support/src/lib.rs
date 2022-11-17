#![cfg_attr(test, allow(clippy::comparison_to_empty))]

#[cfg(test)]
#[macro_use]
extern crate assert_matches;
#[macro_use]
extern crate derive_new;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate thiserror;

pub mod iterator;

use std::sync::mpsc::{RecvError, SendError};

#[derive(Error, Debug)]
pub enum DatapetError {
    #[error("Error: {0}")]
    Custom(String),
    #[error("Pipe read error")]
    PipeRead,
    #[error("Pipe write error")]
    PipeWrite,
}

impl From<RecvError> for DatapetError {
    fn from(_: RecvError) -> Self {
        Self::PipeRead
    }
}

impl<T> From<SendError<T>> for DatapetError {
    fn from(_: SendError<T>) -> Self {
        Self::PipeWrite
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, new)]
pub struct AnchorId<const TABLE_ID: usize>(usize);
