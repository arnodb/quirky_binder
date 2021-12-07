#[macro_use]
extern crate quote;

use datapet_codegen::{
    chain::{Chain, ChainCustomizer, ImportScope},
    dyn_node,
    filter::{group::group, sink::sink, sort::sort},
    graph::{DynNode, Graph, GraphBuilder, Node},
    stream::{NodeStream, NodeStreamSource, StreamRecordType},
    support::FullyQualifiedName,
};
use std::path::Path;

struct ReadStdinIterator {
    name: FullyQualifiedName,
    field: String,
    outputs: [NodeStream; 1],
}

impl Node<0, 1> for ReadStdinIterator {
    fn inputs(&self) -> &[NodeStream; 0] {
        &[]
    }

    fn outputs(&self) -> &[NodeStream; 1] {
        &self.outputs
    }

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

dyn_node!(ReadStdinIterator);

fn read_stdin(
    graph: &mut GraphBuilder,
    name: FullyQualifiedName,
    field: &str,
) -> ReadStdinIterator {
    let record_type = StreamRecordType::from(name.sub("read"));
    graph.new_stream(record_type.clone());

    let variant_id = {
        let mut stream = graph
            .get_stream(&record_type)
            .unwrap_or_else(|| panic!(r#"stream "{}""#, record_type))
            .borrow_mut();

        stream.add_datum::<Box<str>, _>("words");
        stream.close_record_variant()
    };

    ReadStdinIterator {
        name: name.clone(),
        field: field.to_string(),
        outputs: [NodeStream::new(
            record_type,
            variant_id,
            NodeStreamSource::from(name),
        )],
    }
}

pub struct Tokenize {
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    outputs: [NodeStream; 1],
}

impl Node<1, 1> for Tokenize {
    fn inputs(&self) -> &[NodeStream; 1] {
        &self.inputs
    }

    fn outputs(&self) -> &[NodeStream; 1] {
        &self.outputs
    }

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

dyn_node!(Tokenize);

pub fn tokenize(
    graph: &mut GraphBuilder,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
) -> Tokenize {
    let [input] = inputs;

    let variant_id = {
        let mut stream = graph
            .get_stream(input.record_type())
            .unwrap_or_else(|| panic!(r#"stream "{}""#, input.record_type()))
            .borrow_mut();

        let datum = stream
            .get_variant_datum_definition_by_name(input.variant_id(), "words")
            .unwrap_or_else(|| panic!(r#"datum "{}""#, "words"));
        let datum_id = datum.id();
        stream.remove_datum(datum_id);
        stream.add_datum::<Box<str>, _>("word");
        stream.add_datum::<char, _>("first_char");
        stream.close_record_variant()
    };

    let record_type = input.record_type().clone();
    Tokenize {
        name: name.clone(),
        inputs: [input],
        outputs: [NodeStream::new(
            record_type,
            variant_id,
            NodeStreamSource::from(name),
        )],
    }
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
        group.outputs()[0].clone(),
        Some(
            r#"use itertools::Itertools;

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
);"#
            .to_string(),
        ),
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
