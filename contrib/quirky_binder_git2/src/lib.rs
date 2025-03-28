#[macro_use]
extern crate getset;
#[macro_use]
extern crate quote;

mod walk_commits;

pub use walk_commits::walk_commits;
