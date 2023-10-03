#[macro_use]
extern crate getset;
#[macro_use]
extern crate quote;

use datapet::{graph::StreamsBuilder, prelude::*, stream::UniqueNodeStream};
use datapet_codegen::dtpt_mod;
use std::path::Path;
use truc::record::type_resolver::{StaticTypeResolver, TypeResolver};

#[derive(Getters)]
pub struct Tokenize {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 1],
    #[getset(get = "pub")]
    outputs: [NodeStream; 1],
}

impl Tokenize {
    fn new<R: TypeResolver + Copy>(
        graph: &mut GraphBuilder<R>,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
    ) -> Self {
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
        let thread = chain.get_thread_id_and_module_by_source(
            self.inputs.unique(),
            &self.name,
            self.outputs.some_unique(),
        );

        let def = chain.stream_definition_fragments(self.outputs.unique());

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

            let record = def.record();

            let input = thread.format_input(
                self.inputs.unique().source(),
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
    }
}

pub fn tokenize<R: TypeResolver + Copy>(
    graph: &mut GraphBuilder<R>,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
) -> Tokenize {
    Tokenize::new(graph, name, inputs)
}

fn main() {
    dtpt_mod! {
        r###"
use datapet::{
    filter::{
        group::group, sink::sink, sort::sort,
        source::function::function_source,
    },
};

use super::tokenize;

{
  (
      function_source#read_input(
        &[("words", "Box<str>")],
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
    - tokenize#tokenize()
    - sort#sort(&["first_char", "word"])
    - group#group(&["word"], "words")
    - sink#sink(
        Some(quote! {
            use itertools::Itertools;

            println!(
                "{} - [{}]",
                record.first_char(),
                record.words()
                    .iter()
                    .map(|word|word.word())
                    .join(", ")
            );
        })
      )
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
