#[macro_use]
extern crate getset;
#[macro_use]
extern crate quote;

use datapet::{graph::StreamsBuilder, prelude::*};
use datapet_codegen::dtpt_mod;
use std::path::Path;

#[derive(Getters)]
struct ReadStdin {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    #[allow(dead_code)]
    inputs: [NodeStream; 0],
    #[getset(get = "pub")]
    outputs: [NodeStream; 1],
    field: String,
}

impl ReadStdin {
    fn new(
        graph: &mut GraphBuilder,
        name: FullyQualifiedName,
        inputs: [NodeStream; 0],
        field: &str,
    ) -> Self {
        let mut streams = StreamsBuilder::new(&name, &inputs);
        streams.new_main_stream(graph);

        {
            let output_stream = streams.new_main_output(graph).for_update();
            let mut output_stream_def = output_stream.borrow_mut();
            output_stream_def.add_datum::<Box<str>, _>("token");
        }

        let outputs = streams.build();

        Self {
            name,
            inputs,
            outputs,
            field: field.to_string(),
        }
    }
}

impl DynNode for ReadStdin {
    fn gen_chain(&self, graph: &Graph, chain: &mut Chain) {
        let thread_id = chain.new_thread(
            self.name.clone(),
            Box::new([]),
            self.outputs.to_vec().into_boxed_slice(),
            None,
            false,
            Some(self.name.clone()),
        );

        let scope = chain.get_or_new_module_scope(
            self.name.iter().take(self.name.len() - 1),
            graph.chain_customizer(),
            thread_id,
        );
        let mut import_scope = ImportScope::default();
        import_scope.add_error_type();

        {
            let fn_name = format_ident!("{}", **self.name.last().expect("local name"));
            let thread_module = format_ident!("thread_{}", thread_id);
            let error_type = graph.chain_customizer().error_type.to_name();

            let def =
                self.outputs[0].definition_fragments(&graph.chain_customizer().streams_module_name);
            let record = def.record();
            let unpacked_record = def.unpacked_record();

            let field = format_ident!("{}", self.field);

            let fn_def = quote! {
                pub fn #fn_name(mut thread_control: #thread_module::ThreadControl) -> impl FnOnce() -> Result<(), #error_type> {
                    move || {
                        let tx = thread_control.output_0.take().expect("output");
                        use std::io::BufRead;

                        let stdin = std::io::stdin();
                        let mut input = stdin.lock();
                        let mut buffer = String::new();
                        loop {
                            let read = input.read_line(&mut buffer).map_err(|err| DatapetError::Custom(err.to_string()))?;
                            if read > 0 {
                                let value = std::mem::take(&mut buffer);
                                let value = value.trim_end_matches('\n');
                                let record = #record::new(
                                    #unpacked_record { #field: value.to_string().into_boxed_str() },
                                );
                                tx.send(Some(record))?;
                            } else {
                                tx.send(None)?;
                                return Ok(());
                            }
                        }
                    }
                }
            };
            scope.raw(&fn_def.to_string());
        }

        import_scope.import(scope, graph.chain_customizer());
    }
}

#[derive(Getters)]
struct ReadStdinIterator {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    #[allow(dead_code)]
    inputs: [NodeStream; 0],
    #[getset(get = "pub")]
    outputs: [NodeStream; 1],
    field: String,
}

impl ReadStdinIterator {
    fn new(
        graph: &mut GraphBuilder,
        name: FullyQualifiedName,
        inputs: [NodeStream; 0],
        field: &str,
    ) -> Self {
        let mut streams = StreamsBuilder::new(&name, &inputs);
        streams.new_main_stream(graph);

        {
            let output_stream = streams.new_main_output(graph).for_update();
            let mut output_stream_def = output_stream.borrow_mut();
            output_stream_def.add_datum::<Box<str>, _>("token");
        }

        let outputs = streams.build();

        Self {
            name,
            inputs,
            outputs,
            field: field.to_string(),
        }
    }
}

impl DynNode for ReadStdinIterator {
    fn gen_chain(&self, graph: &Graph, chain: &mut Chain) {
        let thread_id = chain.new_thread(
            self.name.clone(),
            Box::new([]),
            self.outputs.to_vec().into_boxed_slice(),
            None,
            false,
            None,
        );

        let scope = chain.get_or_new_module_scope(
            self.name.iter().take(self.name.len() - 1),
            graph.chain_customizer(),
            thread_id,
        );
        let mut import_scope = ImportScope::default();
        import_scope.add_error_type();

        {
            let fn_name = format_ident!("{}", **self.name.last().expect("local name"));
            let thread_module = format_ident!("thread_{}", thread_id);
            let error_type = graph.chain_customizer().error_type.to_name();

            let def =
                self.outputs[0].definition_fragments(&graph.chain_customizer().streams_module_name);
            let record = def.record();
            let unpacked_record = def.unpacked_record();

            let field = format_ident!("{}", self.field);

            let fn_def = quote! {
                pub fn #fn_name(_thread_control: #thread_module::ThreadControl) -> impl FallibleIterator<Item = #record, Error = #error_type> {
                    datapet_support::iterator::io::buf::ReadStdinLines::new()
                        .map(|line| {{
                            let record = #record::new(
                                #unpacked_record { #field: line.to_string().into_boxed_str() },
                            );
                            Ok(record)
                        }})
                    .map_err(|err| DatapetError::Custom(err.to_string()))
                }
            };
            scope.raw(&fn_def.to_string());
        }

        import_scope.import(scope, graph.chain_customizer());
    }
}

#[allow(dead_code)]
fn read_stdin_old(graph: &mut GraphBuilder, name: FullyQualifiedName, field: &str) -> ReadStdin {
    ReadStdin::new(graph, name, [], field)
}

fn read_stdin(
    graph: &mut GraphBuilder,
    name: FullyQualifiedName,
    inputs: [NodeStream; 0],
    field: &str,
) -> ReadStdinIterator {
    ReadStdinIterator::new(graph, name, inputs, field)
}

fn main() {
    dtpt_mod! {
        r#"
use datapet::{
    filter::{
        anchor::anchorize, dedup::dedup, hof::index::wordlist::build_word_list, sink::sink,
        sort::sort,
    },
};

use super::read_stdin;

{
  (
      read_stdin#read_token("token")
    - sort#sort_token(&["token"])
    - dedup#dedup_token()
    - anchorize#anchor("anchor")
    - build_word_list#word_list("token", "anchor", "sim_anchor", "sim_rs") [s2, s3, s4]
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
            println!("sink_3 {} (sim id = {:?}) == {}", record.token(), record.sim_anchor(), record.sim_rs().len());
            for r in record.sim_rs().iter() {
                println!("    {:?}", r.anchor());
            }
        })
      )
  )

  ( < s4
    - sink#sink_4(
        Some(
            quote! { println!("sink_4 {} (sim id = {:?})", record.token(), record.sim_anchor()); },
        )
      )
  )
}
"#
    }

    let graph = dtpt_main(GraphBuilder::new(ChainCustomizer::default()));

    let out_dir = std::env::var("OUT_DIR").unwrap();
    graph.generate(Path::new(&out_dir)).unwrap();
}
