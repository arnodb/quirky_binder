#[macro_use]
extern crate getset;
#[macro_use]
extern crate quote;

use std::{collections::BTreeMap, path::Path};

use quirky_binder::{prelude::*, quirky_binder, trace_element};
use truc::record::type_resolver::StaticTypeResolver;

const TOKENIZE_TRACE_NAME: &str = "tokenize";

#[derive(Debug, Getters)]
pub struct Tokenize {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 1],
    #[getset(get = "pub")]
    outputs: [NodeStream; 1],
}

impl Tokenize {
    fn new(
        graph: &mut GraphBuilder,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        _params: (),
    ) -> ChainResultWithTrace<Self> {
        let mut streams = StreamsBuilder::new(&name, &inputs);

        streams
            .output_from_input(0, true, graph)
            .with_trace_element(trace_element!())?
            .update(&mut streams, |output_stream, facts_proof| {
                let input_variant_id = output_stream.input_variant_id();
                let mut output_stream_def = output_stream.record_definition().borrow_mut();
                let datum = output_stream_def
                    .get_variant_datum_definition_by_name(input_variant_id, "words")
                    .unwrap_or_else(|| panic!(r#"datum "{}""#, "words"));
                let datum_id = datum.id();
                output_stream_def
                    .remove_datum(datum_id)
                    .map_err(|err| ChainError::Other { msg: err })
                    .with_trace_element(trace_element!())?;
                output_stream_def
                    .add_datum(
                        "word",
                        QuirkyDatumType::Simple {
                            type_name: "Box<str>".to_owned(),
                        },
                    )
                    .map_err(|err| ChainError::Other { msg: err })
                    .with_trace_element(trace_element!())?;
                output_stream_def
                    .add_datum(
                        "first_char",
                        QuirkyDatumType::Simple {
                            type_name: "char".to_owned(),
                        },
                    )
                    .map_err(|err| ChainError::Other { msg: err })
                    .with_trace_element(trace_element!())?;
                Ok(facts_proof.order_facts_updated().distinct_facts_updated())
            })?;

        let outputs = streams.build().with_trace_element(trace_element!())?;

        Ok(Self {
            name,
            inputs,
            outputs,
        })
    }
}

impl DynNode for Tokenize {
    fn name(&self) -> &FullyQualifiedName {
        &self.name
    }

    fn inputs(&self) -> &[NodeStream] {
        &self.inputs
    }

    fn outputs(&self) -> &[NodeStream] {
        &self.outputs
    }

    fn gen_chain(&self, _graph: &Graph, chain: &mut Chain) {
        let inline_body = quote! {
            crate::chain::tokenize::tokenize(input)
        };

        chain.implement_inline_node(
            self,
            self.inputs.some_single(),
            self.outputs.single(),
            &inline_body,
        );
    }
}

pub fn tokenize(
    graph: &mut GraphBuilder,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    params: (),
) -> ChainResultWithTrace<Tokenize> {
    let _trace_name = TraceName::push(TOKENIZE_TRACE_NAME);
    Tokenize::new(graph, name, inputs, params)
}

fn main() {
    quirky_binder!(inline(
        r###"
use quirky_binder::{
    filter::{
        function::{
            produce::function_produce,
            terminate::function_terminate,
        },
        group::group, sort::sort,
    },
};

use super::tokenize;

{
  (
      function_produce#read_input(
        fields: [("words", "Box<str>")],
        body: r#"
            use std::io::BufRead;

            let stdin = std::io::stdin();
            let mut input = stdin.lock();
            let mut buffer = String::new();
            loop {
                let read = input.read_line(&mut buffer)?;
                if read > 0 {
                    let value = std::mem::take(&mut buffer);
                    let value = value.trim_end_matches('\n');
                    let record = value.to_string().into_boxed_str().into();
                    output.send(Some(record))?;
                } else {
                    output.send(None)?;
                    return Ok(());
                }
            }
"#,
      )
    - tokenize#tokenize()
    - sort#sort(fields: ["first_char", "word"])
    - group#group(by_fields: ["first_char"], group_field: "words")
    - function_terminate#term(
        body: r#"
            use itertools::Itertools;
            while let Some(record) = input.next()? {
                println!(
                    "{} - [{}]",
                    record.first_char(),
                    record.words()
                        .iter()
                        .map(|word|word.word())
                        .join(", ")
                );
            }
            Ok(())
"#,
      )
  )
}

#(
    name: "quirky_binder_monitor",
    feature: "quirky_binder_monitor",
)
{ ( quirky_binder::filter::monitor::monitor() ) }

"###
    ));

    let type_resolver = {
        let mut resolver = StaticTypeResolver::new();
        resolver.add_all_types();
        resolver
    };

    let graph =
        quirky_binder_main(GraphBuilder::new(ChainCustomizer::default())).unwrap_or_else(|err| {
            panic!("{}", err);
        });

    let out_dir = std::env::var("OUT_DIR").unwrap();
    graph.generate(Path::new(&out_dir), &type_resolver).unwrap();
}
