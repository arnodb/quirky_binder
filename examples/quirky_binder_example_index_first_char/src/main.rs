use quirky_binder_capnp::run_chain;
use quirky_binder_support::chain::configuration::ChainConfiguration;

#[macro_use]
extern crate static_assertions;

#[allow(dead_code)]
#[allow(clippy::borrowed_box)]
#[allow(clippy::module_inception)]
mod chain;

quirky_binder_support::tracking_allocator_static!();

#[quirky_binder_support::tracking_allocator_main]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (chain_status, join) = chain::main(ChainConfiguration::default()).unwrap();

    run_chain(chain_status, || join.join_all())
}
