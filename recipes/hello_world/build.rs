use std::path::Path;

use quirky_binder::{prelude::*, quirky_binder};
use truc::record::type_resolver::{StaticTypeResolver, TypeResolver};

fn main() {
    quirky_binder!(include("main.qb"));

    let type_resolver = {
        let mut resolver = StaticTypeResolver::new();
        resolver.add_std_types();
        resolver
    };

    let graph = quirky_binder_main(GraphBuilder::new(
        &type_resolver,
        ChainCustomizer::default(),
    ))
    .unwrap_or_else(|err| {
        panic!("{}", err);
    });

    let out_dir = std::env::var("OUT_DIR").unwrap();
    graph.generate(Path::new(&out_dir)).unwrap();
}
