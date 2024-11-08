use std::path::Path;

use quirky_binder::{prelude::*, quirky_binder};
use quirky_binder_support::AnchorId;
use truc::record::type_resolver::{StaticTypeResolver, TypeResolver};

fn main() {
    quirky_binder!(inline(
        r###"
use quirky_binder::{
    filter::{
        anchor::anchor, dedup::dedup, hof::index::wordlist::build_word_list,
        function::{
            produce::function_produce,
            terminate::function_terminate,
        },
        sort::sort,
    },
};

#(
    name: "quirky_binder_monitor",
    feature: "quirky_binder_monitor",
)
{ ( quirky_binder::filter::monitor::monitor() ) }

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
                let read = input.read_line(&mut buffer).map_err(|err| QuirkyBinderError::Custom(err.to_string()))?;
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
    - [s2, s3, s4] function_terminate#dot(
        body: r#"
            println!("digraph wordlist \{{");
            let mut input_0 = Some(input_0);
            let mut input_1 = Some(input_1);
            let mut input_2 = Some(input_2);
            let mut input_3 = Some(input_3);
            while input_0.is_some() || input_1.is_some() || input_2.is_some() || input_3.is_some() {
                let mut read = 0;
                if let Some(input) = &input_0 {
                    let mut closed = false;
                    for record in input.try_iter() {
                        if let Some(record) = record {
                            println!("word_{} [label=\"{}\"]", record.anchor(), record.token());
                            read += 1;
                        } else {
                            closed = true;
                        }
                    }
                    if closed {
                        input_0 = None;
                    }
                }
                if let Some(input) = &input_1 {
                    let mut closed = false;
                    for record in input.try_iter() {
                        if let Some(record) = record {
                            println!("rev_word_{} [label=\"{}\"]", record.anchor(), record.token());
                            println!("word_{} -> rev_word_{}", record.anchor(), record.anchor());
                            read += 1;
                        } else {
                            closed = true;
                        }
                    }
                    if closed {
                        input_1 = None;
                    }
                }
                if let Some(input) = &input_2 {
                    let mut closed = false;
                    for record in input.try_iter() {
                        if let Some(record) = record {
                            println!("ci_word_{} [label=\"{}\"]", record.ci_anchor(), record.token());
                            for ref_record in record.ci_refs() {
                                println!("word_{} -> ci_word_{}", ref_record.anchor(), record.ci_anchor());
                            }
                            read += 1;
                        } else {
                            closed = true;
                        }
                    }
                    if closed {
                        input_2 = None;
                    }
                }
                if let Some(input) = &input_3 {
                    let mut closed = false;
                    for record in input.try_iter() {
                        if let Some(record) = record {
                            println!("rev_ci_word_{} [label=\"{}\"]", record.ci_anchor(), record.token());
                            println!("ci_word_{} -> rev_ci_word_{}", record.ci_anchor(), record.ci_anchor());
                            read += 1;
                        } else {
                            closed = true;
                        }
                    }
                    if closed {
                        input_3 = None;
                    }
                }
                if read == 0 {
                    std::thread::sleep(std::time::Duration::from_millis(42));
                }
            }
            println!("}}");
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
