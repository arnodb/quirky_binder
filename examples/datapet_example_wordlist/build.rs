use datapet::dtpt;
use datapet::prelude::*;
use datapet_support::AnchorId;
use std::path::Path;
use truc::record::type_resolver::{StaticTypeResolver, TypeResolver};

fn main() {
    dtpt!(inline(
        r###"
use datapet::{
    filter::{
        anchor::anchor, dedup::dedup, hof::index::wordlist::build_word_list,
        function::{
            produce::function_produce,
            terminate::function_terminate,
        },
        sort::sort,
    },
};

{
  (
      function_produce#read_token(
        fields: [("token", "Box<str>")],
        body: r#"{
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
                    output.send(Some(record))?;
                } else {
                    output.send(None)?;
                    return Ok(());
                }
            }
        }"#,
      )
    - sort#sort_token(fields: ["token"])
    - dedup#dedup_token()
    - anchor#anchor(anchor_field: "anchor")
    - build_word_list#word_list(
        token_field: "token",
        anchor_field: "anchor",
        ci_anchor_field: "ci_anchor",
        ci_refs_field: "ci_refs",
      ) [s2, s3, s4]
    - function_terminate#term_1(
        body: r#"
            while let Some(record) = input.next()? {
                println!("term_1 {} (id = {:?})", record.token(), record.anchor());
            }
            Ok(())
"#,
      )
  )

  ( < s2
    - function_terminate#term_2(
        body: r#"
            while let Some(record) = input.next()? {
                println!("term_2 {} (id = {:?})", record.token(), record.anchor());
            }
            Ok(())
"#,
      )
  )

  ( < s3
    - function_terminate#term_3(
        body: r#"
            while let Some(record) = input.next()? {
                println!("term_3 {} (ci id = {:?}) == {}", record.token(), record.ci_anchor(), record.ci_refs().len());
                for r in record.ci_refs().iter() {
                    println!("    {:?}", r.anchor());
                }
            }
            Ok(())
"#,
      )
  )

  ( < s4
    - function_terminate#term_4(
        body: r#"
            while let Some(record) = input.next()? {
                println!("term_4 {} (ci id = {:?})", record.token(), record.ci_anchor());
            }
            Ok(())
"#,
      )
  )
}
"###
    ));

    let type_resolver = {
        let mut resolver = StaticTypeResolver::new();
        resolver.add_std_types();
        resolver.add_type::<AnchorId<0>>();
        resolver
    };

    let graph = dtpt_main(GraphBuilder::new(
        &type_resolver,
        ChainCustomizer::default(),
    ))
    .unwrap();

    let out_dir = std::env::var("OUT_DIR").unwrap();
    graph.generate(Path::new(&out_dir)).unwrap();
}
