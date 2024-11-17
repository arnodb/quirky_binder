#[macro_use]
extern crate getset;
#[macro_use]
extern crate quote;

mod read;

pub use read::read_csv;
