use datapet_support::chain::configuration::ChainConfiguration;

#[macro_use]
extern crate static_assertions;

#[allow(dead_code)]
#[allow(clippy::borrowed_box)]
#[allow(clippy::module_inception)]
mod chain;

datapet_support::tracking_allocator_static!();

#[datapet_support::tracking_allocator_main]
fn main() {
    chain::main(ChainConfiguration::default()).unwrap();
}
