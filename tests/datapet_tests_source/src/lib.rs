use datapet::{dtpt, prelude::*};
use std::{fs::File, io::Write, path::Path};
use truc::record::type_resolver::{StaticTypeResolver, TypeResolver};

dtpt!(include_glob_test("dtpt_tests", "**/*.dtpt"));

pub fn generate_tests(out_dir: &Path) {
    let type_resolver = {
        let mut resolver = StaticTypeResolver::new();
        resolver.add_std_types();
        resolver
    };

    dtpt_generate_deep(out_dir, |module_path| {
        let module_name = FullyQualifiedName::new_n(&["crate", "all_chains"]).sub_n(module_path);
        let streams_module_name = module_name.sub("streams");
        let customizer = ChainCustomizer {
            streams_module_name,
            module_name,
            ..Default::default()
        };
        GraphBuilder::new(&type_resolver, customizer)
    })
    .unwrap();
}
