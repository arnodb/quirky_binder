use std::{collections::BTreeMap, fs::File, path::Path};

use truc::{
    generator::{
        config::GeneratorConfig,
        fragment::{clone::CloneImplGenerator, serde::SerdeImplGenerator, FragmentGenerator},
    },
    record::type_resolver::TypeResolver,
};

use crate::{codegen::Module, prelude::*};

pub mod builder;
pub mod error;
pub mod node;
pub mod visit;

pub struct Graph {
    chain_customizer: ChainCustomizer,
    record_definitions: BTreeMap<StreamRecordType, QuirkyRecordDefinition>,
    entry_nodes: Vec<Box<dyn DynNode>>,
}

impl Graph {
    pub fn chain_customizer(&self) -> &ChainCustomizer {
        &self.chain_customizer
    }

    pub fn record_definitions(&self) -> &BTreeMap<StreamRecordType, QuirkyRecordDefinition> {
        &self.record_definitions
    }

    pub fn generate<R>(&self, output: &Path, type_resolver: R) -> Result<(), std::io::Error>
    where
        R: TypeResolver + Copy,
    {
        use std::io::Write;

        /*
        {
            use crate::drawing::{format_svg, graph::stream::draw_streams};
            let drawing = draw_streams(self);
            let mut svg = String::new();
            format_svg(&mut svg, &drawing).unwrap();
            let mut f = File::create(output.join(format!(
                "{}_streams.svg",
                std::env::var("CARGO_PKG_NAME").expect("CARGO_PKG_NAME")
            )))
            .expect("Unable to create file");
            f.write_all(svg.as_bytes()).expect("Unable to write data");
        }

        {
            use crate::drawing::{format_svg, graph::record::draw_records};
            let drawing = draw_records(self);
            let mut svg = String::new();
            format_svg(&mut svg, &drawing).unwrap();
            let mut f = File::create(output.join(format!(
                "{}_records.svg",
                std::env::var("CARGO_PKG_NAME").expect("CARGO_PKG_NAME")
            )))
            .expect("Unable to create file");
            f.write_all(svg.as_bytes()).expect("Unable to write data");
        }
        */

        let variants_mapping = {
            let mut file = File::create(output.join("streams.rs")).unwrap();
            let mut root_module = Module::default();
            let mut variants_mapping = BTreeMap::new();
            for (record_type, definition) in &self.record_definitions {
                let module = record_type
                    .iter()
                    .skip(1)
                    .fold(root_module.get_or_new_module(&record_type[0]), |m, n| {
                        m.get_or_new_module(n)
                    });
                let (truc_definition, def_variants_mapping) =
                    definition.truc(&self.chain_customizer, type_resolver);
                module.fragment(truc::generator::generate(
                    &truc_definition,
                    &GeneratorConfig::default_with_custom_generators([
                        Box::new(CloneImplGenerator) as Box<dyn FragmentGenerator>,
                        Box::new(SerdeImplGenerator) as Box<dyn FragmentGenerator>,
                    ]),
                ));
                variants_mapping.insert(record_type, def_variants_mapping);
            }
            write!(file, "{}", root_module).unwrap();
            variants_mapping
        };
        rustfmt_generated_file(output.join("streams.rs").as_path());

        {
            let mut root_module = Module::default();
            for (path, ty) in &self.chain_customizer.custom_module_imports {
                root_module.import(path, ty);
            }

            root_module.fragment("mod streams;");

            let mut chain = Chain::new(&self.chain_customizer, &mut root_module, &variants_mapping);

            for node in &self.entry_nodes {
                node.gen_chain(self, &mut chain);
            }

            chain.gen_chain();

            let mut file = File::create(output.join("chain.rs")).unwrap();
            write!(file, "{}", root_module).unwrap();
        }
        rustfmt_generated_file(output.join("chain.rs").as_path());

        Ok(())
    }
}

fn rustfmt_generated_file(file: &Path) {
    use std::process::Command;

    let rustfmt = if let Ok(rustfmt) = which::which("rustfmt") {
        rustfmt
    } else {
        eprintln!("Rustfmt activated, but it could not be found in global path.");
        return;
    };

    let mut cmd = Command::new(rustfmt);

    if let Ok(output) = cmd.arg(file).output() {
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            match output.status.code() {
                Some(2) => {
                    eprintln!("Rustfmt parsing errors:\n{}", stderr);
                }
                Some(3) => {
                    eprintln!("Rustfmt could not format some lines:\n{}", stderr);
                }
                _ => {
                    eprintln!("Internal rustfmt error:\n{}", stderr);
                }
            }
        }
    } else {
        eprintln!("Error executing rustfmt!");
    }
}
