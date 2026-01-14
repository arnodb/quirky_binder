use std::collections::BTreeMap;

use quirky_binder_codegen_macro::quirky_binder_internal;
use truc::record::type_resolver::StaticTypeResolver;

use crate::{codegen::Module, prelude::*};

#[test]
fn pipe_generates_noop_thread() {
    quirky_binder_internal!(inline(
        r###"
use crate::{
    filter::{
        function::{
            produce::function_produce,
            terminate::function_terminate,
        },
        pipe::pipe,
    },
};

{
  (
      function_produce(
        fields: [("hello", "String")],
        body: r#"
            let record = "world".to_string().into();
            output.send(Some(record))?;
            output.send(None)?;
            Ok(())
"#,
      )
    - pipe()
    - function_terminate(
        body: r#"
            let record = input.next()?.unwrap();
            assert_eq!(record.hello(), "world");
            assert!(input.next()?.is_none());
            Ok(())
"#,
      )
  )
}
"###
    ));

    let _type_resolver = {
        let mut resolver = StaticTypeResolver::new();
        resolver.add_all_types();
        resolver
    };

    let graph =
        quirky_binder_main(GraphBuilder::new(ChainCustomizer::default())).unwrap_or_else(|err| {
            panic!("{}", err);
        });

    let mut root_module = Module::default();
    for (path, ty) in &graph.chain_customizer.custom_module_imports {
        root_module.import(path, ty);
    }

    root_module.fragment("mod streams;");

    let mut chain = Chain::new(&graph.chain_customizer, &mut root_module);

    for node in &graph.entry_nodes {
        node.gen_chain(&graph, &mut chain);
    }

    assert_eq!(chain.threads().len(), 3);
    assert_eq!(chain.pipe_count(), 2);
}
