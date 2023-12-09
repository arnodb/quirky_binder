use datapet_support::chain::configuration::ChainConfiguration;

#[macro_use]
extern crate static_assertions;

#[allow(dead_code)]
#[allow(clippy::borrowed_box)]
#[allow(clippy::module_inception)]
mod chain {
    include!(concat!(env!("OUT_DIR"), "/chain.rs"));
}

fn main() {
    let mut chain_configuration = ChainConfiguration::new();

    let mut args = std::env::args();
    let root = args
        .next()
        .and_then(|_| args.next())
        .unwrap_or_else(|| "datapet".to_owned());
    chain_configuration
        .variables
        .insert("root".to_owned(), root);

    chain::main(chain_configuration).unwrap();
}
