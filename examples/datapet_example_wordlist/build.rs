#[macro_use]
extern crate datapet_codegen;
#[macro_use]
extern crate quote;

use datapet::{
    filter::{
        anchor::anchorize, dedup::dedup, hof::index::wordlist::build_word_list, sink::sink,
        sort::sort,
    },
    prelude::*,
};
use std::{cell::RefCell, path::Path};
use truc::record::definition::{RecordDefinitionBuilder, RecordVariantId};

#[datapet_node(
    out_mut = "-",
    arg = "field: &str",
    init = "streams",
    fields = "field: field.to_string()"
)]
struct ReadStdin {
    name: FullyQualifiedName,
    inputs: [NodeStream; 0],
    outputs: [NodeStream; 1],
    field: String,
}

impl ReadStdin {
    fn initialize_streams(
        output_stream: &RefCell<RecordDefinitionBuilder>,
        _field: &str,
    ) -> [RecordVariantId; 1] {
        let mut output_stream = output_stream.borrow_mut();
        output_stream.add_datum::<Box<str>, _>("token");
        [output_stream.close_record_variant()]
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

#[datapet_node(
    out_mut = "-",
    arg = "field: &str",
    init = "streams",
    fields = "field: field.to_string()"
)]
struct ReadStdinIterator {
    name: FullyQualifiedName,
    inputs: [NodeStream; 0],
    outputs: [NodeStream; 1],
    field: String,
}

impl ReadStdinIterator {
    fn initialize_streams(
        output_stream: &RefCell<RecordDefinitionBuilder>,
        _field: &str,
    ) -> [RecordVariantId; 1] {
        let mut output_stream = output_stream.borrow_mut();
        output_stream.add_datum::<Box<str>, _>("token");
        [output_stream.close_record_variant()]
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
    field: &str,
) -> ReadStdinIterator {
    ReadStdinIterator::new(graph, name, [], field)
}

fn main() {
    let mut graph = GraphBuilder::new(ChainCustomizer::default());

    let root = FullyQualifiedName::default();

    let read_token = read_stdin(&mut graph, root.sub("read_token"), "token");
    let sort_token = sort(
        &mut graph,
        root.sub("sort_token"),
        [read_token.outputs()[0].clone()],
        &["token"],
    );
    let dedup_token = dedup(
        &mut graph,
        root.sub("dedup_token"),
        [sort_token.outputs()[0].clone()],
    );
    let anchorize = anchorize(
        &mut graph,
        root.sub("anchor"),
        [dedup_token.outputs()[0].clone()],
        "anchor",
    );

    let word_list = build_word_list(
        &mut graph,
        root.sub("word_list"),
        [anchorize.outputs()[0].clone()],
        "token",
        "anchor",
        "sim_anchor",
        "sim_rs",
    );

    let sink_1 = sink(
        &mut graph,
        root.sub("sink_1"),
        [word_list.outputs()[0].clone()],
        Some(quote! { println!("sink_1 {} (id = {:?})", record.token(), record.anchor()); }),
    );
    let sink_2 = sink(
        &mut graph,
        root.sub("sink_2"),
        [word_list.outputs()[1].clone()],
        Some(quote! { println!("sink_2 {} (id = {:?})", record.token(), record.anchor()); }),
    );
    let sink_3 = sink(
        &mut graph,
        root.sub("sink_3"),
        [word_list.outputs()[2].clone()],
        Some(quote! {
            println!("sink_3 {} (sim id = {:?}) == {}", record.token(), record.sim_anchor(), record.sim_rs().len());
            for r in record.sim_rs().iter() {
                println!("    {:?}", r.anchor());
            }
        }),
    );
    let sink_4 = sink(
        &mut graph,
        root.sub("sink_4"),
        [word_list.outputs()[3].clone()],
        Some(
            quote! { println!("sink_4 {} (sim id = {:?})", record.token(), record.sim_anchor()); },
        ),
    );

    let graph = graph.build(vec![
        Box::new(read_token),
        Box::new(sort_token),
        Box::new(dedup_token),
        Box::new(anchorize),
        Box::new(word_list),
        Box::new(sink_1),
        Box::new(sink_2),
        Box::new(sink_3),
        Box::new(sink_4),
    ]);

    let out_dir = std::env::var("OUT_DIR").unwrap();
    graph.generate(Path::new(&out_dir)).unwrap();
}
