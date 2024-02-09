#![cfg_attr(test, allow(clippy::comparison_to_empty))]

#[cfg(test)]
#[macro_use]
extern crate assert_matches;
#[macro_use]
extern crate derive_more;
#[macro_use]
extern crate derive_new;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate thiserror;

pub use datapet_codegen_macro::{tracking_allocator_main, tracking_allocator_static};

pub mod chain;
pub mod data;
pub mod iterator;

use std::sync::mpsc::{RecvError, SendError};

use serde::{Deserialize, Serialize};

#[derive(Error, Debug)]
pub enum DatapetError {
    #[error("Error: {0}")]
    Custom(String),
    #[error("Pipe read error")]
    PipeRead,
    #[error("Pipe write error")]
    PipeWrite,
    #[error("Bincode error {0}")]
    Bincode(#[from] bincode::Error),
}

impl DatapetError {
    pub fn custom(error: String) -> Self {
        Self::Custom(error)
    }
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

#[derive(
    Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Display, Deref, new, Serialize, Deserialize,
)]
pub struct AnchorId<const TABLE_ID: usize>(usize);
