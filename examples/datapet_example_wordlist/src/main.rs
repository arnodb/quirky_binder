use datapet_support::chain::configuration::ChainConfiguration;

#[macro_use]
extern crate static_assertions;

#[allow(dead_code)]
#[allow(clippy::borrowed_box)]
#[allow(clippy::module_inception)]
mod chain;

fn main() {
    chain::main(ChainConfiguration::default()).unwrap();
}
