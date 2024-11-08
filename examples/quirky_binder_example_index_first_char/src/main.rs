use quirky_binder_support::chain::configuration::ChainConfiguration;

#[macro_use]
extern crate static_assertions;

#[allow(dead_code)]
#[allow(clippy::borrowed_box)]
#[allow(clippy::module_inception)]
mod chain;

quirky_binder_support::tracking_allocator_static!();

#[quirky_binder_support::tracking_allocator_main]
fn main() {
    chain::main(ChainConfiguration::default()).unwrap();
}
