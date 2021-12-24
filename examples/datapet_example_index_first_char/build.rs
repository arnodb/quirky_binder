#[macro_use]
extern crate getset;
#[macro_use]
extern crate quote;

use datapet::{
    filter::{group::group, sink::sink, sort::sort},
    graph::StreamsBuilder,
    prelude::*,
};
use std::path::Path;

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
            output_stream_def.add_datum::<Box<str>, _>("words");
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

fn read_stdin(
    graph: &mut GraphBuilder,
    name: FullyQualifiedName,
    field: &str,
) -> ReadStdinIterator {
    ReadStdinIterator::new(graph, name, [], field)
}

#[derive(Getters)]
pub struct Tokenize {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 1],
    #[getset(get = "pub")]
    outputs: [NodeStream; 1],
}

impl Tokenize {
    fn new(graph: &mut GraphBuilder, name: FullyQualifiedName, inputs: [NodeStream; 1]) -> Self {
        let mut streams = StreamsBuilder::new(&name, &inputs);

        {
            let output_stream = streams.output_from_input(0, graph).for_update();
            let input_variant_id = output_stream.input_variant_id();
            let mut output_stream_def = output_stream.borrow_mut();
            let datum = output_stream_def
                .get_variant_datum_definition_by_name(input_variant_id, "words")
                .unwrap_or_else(|| panic!(r#"datum "{}""#, "words"));
            let datum_id = datum.id();
            output_stream_def.remove_datum(datum_id);
            output_stream_def.add_datum::<Box<str>, _>("word");
            output_stream_def.add_datum::<char, _>("first_char");
        }

        let outputs = streams.build();

        Self {
            name,
            inputs,
            outputs,
        }
    }
}

impl DynNode for Tokenize {
    fn gen_chain(&self, graph: &Graph, chain: &mut Chain) {
        let thread = chain.get_thread_id_and_module_by_source(self.inputs[0].source(), &self.name);

        let scope = chain.get_or_new_module_scope(
            self.name.iter().take(self.name.len() - 1),
            graph.chain_customizer(),
            thread.thread_id,
        );
        let mut import_scope = ImportScope::default();
        import_scope.add_import_with_error_type("fallible_iterator", "FallibleIterator");

        {
            let fn_name = format_ident!("{}", **self.name.last().expect("local name"));
            let thread_module = format_ident!("thread_{}", thread.thread_id);
            let error_type = graph.chain_customizer().error_type.to_name();

            let def =
                self.outputs[0].definition_fragments(&graph.chain_customizer().streams_module_name);
            let record = def.record();

            let input = thread.format_input(
                self.inputs[0].source(),
                graph.chain_customizer(),
                &mut import_scope,
            );

            let fn_def = quote! {
                pub fn #fn_name(#[allow(unused_mut)] mut thread_control: #thread_module::ThreadControl) -> impl FallibleIterator<Item = #record, Error = #error_type> {
                    #input
                    crate::chain::tokenize::tokenize(input)
                }
            };
            scope.raw(&fn_def.to_string());
        }

        import_scope.import(scope, graph.chain_customizer());

        chain.update_thread_single_stream(thread.thread_id, &self.outputs[0]);
    }
}

pub fn tokenize(
    graph: &mut GraphBuilder,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
) -> Tokenize {
    Tokenize::new(graph, name, inputs)
}

fn main() {
    let mut graph = GraphBuilder::new(ChainCustomizer::default());

    let root = FullyQualifiedName::default();

    let read = read_stdin(&mut graph, root.sub("read_input"), "words");
    let tokenize = tokenize(
        &mut graph,
        root.sub("tokenize"),
        [read.outputs()[0].clone()],
    );
    let sort = sort(
        &mut graph,
        root.sub("sort"),
        [tokenize.outputs()[0].clone()],
        &["first_char", "word"],
    );
    let group = group(
        &mut graph,
        root.sub("group"),
        [sort.outputs()[0].clone()],
        &["word"],
        "words",
    );

    let sink = sink(
        &mut graph,
        root.sub("sink"),
        [group.outputs()[0].clone()],
        Some(quote! {
            use itertools::Itertools;

            println!(
                "{} - {}",
                record.first_char(),
                format!(
                    "[{}]",
                    record.words()
                    .iter()
                    .map(|word|word.word())
                    .join(", ")
                )
            );
        }),
    );

    let graph = graph.build(vec![
        Box::new(read),
        Box::new(tokenize),
        Box::new(sort),
        Box::new(group),
        Box::new(sink),
    ]);

    let out_dir = std::env::var("OUT_DIR").unwrap();
    graph.generate(Path::new(&out_dir)).unwrap();
}
