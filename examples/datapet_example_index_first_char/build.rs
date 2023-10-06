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

        let outputs = streams.build(graph);

        Self {
            name,
            inputs,
            outputs,
        }
    }
}

impl DynNode for Tokenize {
    fn name(&self) -> &FullyQualifiedName {
        &self.name
    }

    fn gen_chain(&self, _graph: &Graph, chain: &mut Chain) {
        let inline_body = quote! {
            crate::chain::tokenize::tokenize(input)
        };

        chain.implement_inline_node(
            self,
            self.inputs.unique(),
            self.outputs.unique(),
            &inline_body,
        );
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
