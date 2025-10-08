use quirky_binder_capnp::run_chain;
use quirky_binder_support::prelude::*;

#[macro_use]
extern crate static_assertions;

#[allow(dead_code)]
#[allow(clippy::borrowed_box)]
#[allow(clippy::module_inception)]
mod chain {
    include!(concat!(env!("OUT_DIR"), "/chain.rs"));
}

quirky_binder_support::tracking_allocator_static!();

#[quirky_binder_support::tracking_allocator_main]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut chain_configuration = ChainConfiguration::new();

    let mut args = std::env::args();
    let root = args
        .next()
        .and_then(|_| args.next())
        .unwrap_or_else(|| "quirky_binder".to_owned());
    chain_configuration
        .variables
        .insert("root".to_owned(), root);

    let (chain_status, join) = chain::main(chain_configuration).unwrap();

    run_chain(chain_status, || join.join_all())
}
