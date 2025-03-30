use std::{fs::File, io::Write, path::Path};

use quirky_binder::{prelude::*, quirky_binder};
use truc::record::type_resolver::{StaticTypeResolver, TypeResolver};

fn main() {
    quirky_binder!(include_glob_test("quirky_binder_src", "**/*.qb"));

    let type_resolver = {
        let mut resolver = StaticTypeResolver::new();
        resolver.add_all_types();
        resolver
    };

    let out_dir = std::env::var("OUT_DIR").unwrap();
    quirky_binder_generate_deep(Path::new(&out_dir), &type_resolver, |module_path| {
        let module_name = FullyQualifiedName::new_n(&["crate", "all_chains"]).sub_n(module_path);
        let streams_module_name = module_name.sub("streams");
        let customizer = ChainCustomizer {
            streams_module_name,
            module_name,
            ..Default::default()
        };
        GraphBuilder::new(customizer)
    })
    .unwrap_or_else(|err| {
        panic!("{}", err);
    });
}
