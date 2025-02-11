use std::path::Path;

use quirky_binder::{prelude::*, quirky_binder};
use truc::record::type_resolver::{StaticTypeResolver, TypeResolver};

fn main() {
    quirky_binder!(inline(
        r###"
use quirky_binder::{
    filter::{
        accumulate::accumulate,
        fork::{
            extract_fields::extract_fields,
            join::join,
        },
        function::{
            produce::function_produce,
            terminate::function_terminate,
        },
        group::group,
        sort::sort,
        unwrap::unwrap,
    },
};

{
  (
      function_produce#read_fs(
        fields: [("id", "usize"), ("file_name", "String"), ("path", "String"), ("parent_id", "Option<usize>")],
        order_fields: Some(["id"]),
        distinct_fields: Some(["id"]),
        body: r#"
            use std::collections::BTreeMap;
            use std::collections::btree_map::Entry;
            use std::path::PathBuf;
            use walkdir::WalkDir;

            let mut full_name_index = BTreeMap::<PathBuf, usize>::new();

            for (id, entry) in WalkDir::new(thread_control.chain_configuration.variables["root"].clone()).into_iter().enumerate() {
                let entry = entry?;

                let parent_id = entry.path().parent()
                    .and_then(|parent_path| {
                        full_name_index.get(parent_path).copied()
                    });

                match full_name_index.entry(entry.path().to_path_buf()) {
                    Entry::Vacant(vacant) => {
                        vacant.insert(id);
                    },
                    Entry::Occupied(occupied) => {
                        return Err(anyhow::anyhow!("Already seen file {}", occupied.key().to_string_lossy()));
                    },
                }

                let record = new_record(
                    id,
                    entry.file_name().to_string_lossy().to_string(),
                    entry.path().to_string_lossy().to_string(),
                    parent_id,
                );
                output.send(Some(record))?;
            }
            output.send(None)?;
            Ok(())
"#,
      )
    - extract_fields(fields: ["id", "parent_id"]) [extracted]
    - accumulate()
    - [children] join(primary_fields: ["id"], secondary_fields: ["parent_id"])
    - function_terminate(
        body: r#"
            while let Some(record) = input.next()? {
                println!(
                    "({}) {} ({:?}) / {}, children: {:?}",
                    record.id(),
                    record.path(),
                    record.parent_id(),
                    record.file_name(),
                    record.children().iter().map(|rec| rec.id()).collect::<Vec<_>>()
                );
            }
            Ok(())
"#,
      )
  )

  ( < extracted
    - unwrap(fields: ["parent_id"], skip_nones: true)
    - sort(fields: ["parent_id", "id"])
    - group(by_fields: ["parent_id"], group_field: "children")
    -> children
  )
}

#(
    name: "quirky_binder_monitor",
    feature: "quirky_binder_monitor",
)
{ ( quirky_binder::filter::monitor::monitor() ) }

"###
    ));

    let type_resolver = {
        let mut resolver = StaticTypeResolver::new();
        resolver.add_all_types();
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
