#[macro_use]
extern crate quote;

use datapet::prelude::*;
use datapet_codegen::dtpt;
use std::path::Path;
use truc::record::type_resolver::{StaticTypeResolver, TypeResolver};

fn main() {
    dtpt! {
        def r###"
use datapet::{
    filter::{
        accumulate::accumulate,
        fork::{
            extract_fields::extract_fields,
            join::join,
        },
        group::group,
        sink::sink,
        sort::sort,
        source::function::function_source,
        unwrap::unwrap,
    },
};

{
  (
      function_source#read_fs(
        &[("id", "usize"), ("file_name", "String"), ("path", "String"), ("parent_id", "Option<usize>")], &["id".asc()], &["id"],
        r#"{
        use std::collections::BTreeMap;
        use std::collections::btree_map::Entry;
        use std::path::PathBuf;
        use walkdir::WalkDir;

        let mut full_name_index = BTreeMap::<PathBuf, usize>::new();

        for (id, entry) in WalkDir::new(thread_control.chain_configuration.variables["root"].clone()).into_iter().enumerate() {
            let entry = entry.map_err(|err| DatapetError::Custom(err.to_string()))?;

            let parent_id = entry.path().parent()
                .and_then(|parent_path| {
                    full_name_index.get(parent_path).copied()
                });

            match full_name_index.entry(entry.path().to_path_buf()) {
                Entry::Vacant(vacant) => {
                    vacant.insert(id);
                },
                Entry::Occupied(occupied) => {
                    return Err(DatapetError::Custom(format!("Already seen file {}", occupied.key().to_string_lossy())));
                },
            }

            let record = new_record(
                id,
                entry.file_name().to_string_lossy().to_string(),
                entry.path().to_string_lossy().to_string(),
                parent_id,
            );
            out.send(Some(record))?;
        }
        out.send(None)?;
        Ok(())
        }"#
      )
    - extract_fields(&["id", "parent_id"]) [extracted]
    - accumulate()
    - [children] join(&["id"], &["parent_id"])
    - sink(
        Some(quote! {
            println!(
                "({}) {} ({:?}) / {}, children: {:?}",
                record.id(),
                record.path(),
                record.parent_id(),
                record.file_name(),
                record.children().iter().map(|rec| rec.id()).collect::<Vec<_>>()
            );
        })
      )
  )

  ( < extracted
    - unwrap(&["parent_id"], true)
    - sort(&["parent_id".asc(), "id".asc()])
    - group(&["id"], "children")
    -> children
  )
}
"###
    }

    let type_resolver = {
        let mut resolver = StaticTypeResolver::new();
        resolver.add_std_types();
        resolver
    };

    let graph = dtpt_main(GraphBuilder::new(
        &type_resolver,
        ChainCustomizer::default(),
    ));

    let out_dir = std::env::var("OUT_DIR").unwrap();
    graph.generate(Path::new(&out_dir)).unwrap();
}
