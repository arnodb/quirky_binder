#![cfg_attr(coverage_nightly, feature(coverage_attribute))]
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

pub use quirky_binder_codegen_macro::{tracking_allocator_main, tracking_allocator_static};

pub mod chain;
pub mod data;
pub mod input;
pub mod iterator;
pub mod output;
pub mod prelude;
pub mod status;

use serde::{Deserialize, Serialize};

#[derive(
    Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Display, Deref, new, Serialize, Deserialize,
)]
pub struct AnchorId<const TABLE_ID: usize>(usize);
