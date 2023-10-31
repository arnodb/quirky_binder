#[macro_use]
extern crate quote;

use datapet::prelude::*;
use datapet_codegen::dtpt;
use datapet_support::AnchorId;
use std::path::Path;
use truc::record::type_resolver::{StaticTypeResolver, TypeResolver};

fn main() {
    dtpt! {
        def r###"
use datapet::{
    filter::{
        anchor::anchor, dedup::dedup, hof::index::wordlist::build_word_list, sink::sink,
        sort::sort,
        source::function::function_source,
    },
};

{
  (
      function_source#read_token(
        &[("token", "Box<str>")], &[], &[],
        r#"{
        use std::io::BufRead;

        let stdin = std::io::stdin();
        let mut input = stdin.lock();
        let mut buffer = String::new();
        loop {
            let read = input.read_line(&mut buffer).map_err(|err| DatapetError::Custom(err.to_string()))?;
            if read > 0 {
                let value = std::mem::take(&mut buffer);
                let value = value.trim_end_matches('\n');
                let record = new_record(value.to_string().into_boxed_str());
                out.send(Some(record))?;
            } else {
                out.send(None)?;
                return Ok(());
            }
        }
        }"#
      )
    - sort#sort_token(&["token".asc()])
    - dedup#dedup_token()
    - anchor#anchor("anchor")
    - build_word_list#word_list("token", "anchor", "ci_anchor", "ci_refs") [s2, s3, s4]
    - sink#sink_1(
        Some(quote! { println!("sink_1 {} (id = {:?})", record.token(), record.anchor()); })
      )
  )

  ( < s2
    - sink#sink_2(
        Some(quote! { println!("sink_2 {} (id = {:?})", record.token(), record.anchor()); })
      )
  )

  ( < s3
    - sink#sink_3(
        Some(quote! {
            println!("sink_3 {} (ci id = {:?}) == {}", record.token(), record.ci_anchor(), record.ci_refs().len());
            for r in record.ci_refs().iter() {
                println!("    {:?}", r.anchor());
            }
        })
      )
  )

  ( < s4
    - sink#sink_4(
        Some(
            quote! { println!("sink_4 {} (ci id = {:?})", record.token(), record.ci_anchor()); },
        )
      )
  )
}
"###
    }

    let type_resolver = {
        let mut resolver = StaticTypeResolver::new();
        resolver.add_std_types();
        resolver.add_type::<AnchorId<0>>();
        resolver
    };

    let graph = dtpt_main(GraphBuilder::new(
        &type_resolver,
        ChainCustomizer::default(),
    ));

    let out_dir = std::env::var("OUT_DIR").unwrap();
    graph.generate(Path::new(&out_dir)).unwrap();
}
