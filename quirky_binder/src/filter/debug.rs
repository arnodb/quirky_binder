use truc::record::{definition::DatumDefinition, type_resolver::TypeResolver};

use crate::{prelude::*, trace_element};

const DEBUG_TRACE_NAME: &str = "debug";

#[derive(Getters)]
pub struct Debug {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 1],
    #[getset(get = "pub")]
    outputs: [NodeStream; 1],
}

impl Debug {
    fn new<R: TypeResolver + Copy>(
        graph: &mut GraphBuilder<R>,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        _params: (),
    ) -> ChainResultWithTrace<Self> {
        let mut streams = StreamsBuilder::new(&name, &inputs);
        streams
            .output_from_input(0, true, graph)
            .with_trace_element(trace_element!(DEBUG_TRACE_NAME))?
            .pass_through(|builder, facts_proof| {
                let def = builder.record_definition().borrow();
                eprintln!("=== Filter {}:", name);
                for d in def.get_current_data() {
                    eprintln!("    {:?}", def.get_datum_definition(d).expect("datum"));
                }
                eprintln!("    {:?}", builder.facts());
                fn indent(depth: usize) {
                    eprint!("{: <1$}", "", depth * 4);
                }
                fn debug_sub_stream<R: TypeResolver + Copy>(
                    depth: usize,
                    datum: &DatumDefinition,
                    sub_stream: &NodeSubStream,
                    graph: &GraphBuilder<R>,
                ) -> ChainResultWithTrace<()> {
                    let def = graph
                        .get_stream(sub_stream.record_type())
                        .with_trace_element(trace_element!(DEBUG_TRACE_NAME))?
                        .borrow();
                    indent(depth);
                    eprintln!("--- Datum {}:", datum.name());
                    for d in def.get_current_data() {
                        indent(depth);
                        eprintln!("    {:?}", def.get_datum_definition(d).expect("datum"));
                    }
                    indent(depth);
                    eprintln!("    {:?}", sub_stream.facts());
                    for (&datum_id, sub_stream) in sub_stream.sub_streams().iter() {
                        debug_sub_stream(
                            depth + 1,
                            def.get_datum_definition(datum_id).expect("datum"),
                            sub_stream,
                            graph,
                        )?;
                    }
                    Ok(())
                }
                for (&datum_id, sub_stream) in builder.sub_streams().iter() {
                    debug_sub_stream(
                        1,
                        def.get_datum_definition(datum_id).expect("datum"),
                        sub_stream,
                        graph,
                    )?;
                }
                Ok(facts_proof.order_facts_updated().distinct_facts_updated())
            })?;
        let outputs = streams
            .build()
            .with_trace_element(trace_element!(DEBUG_TRACE_NAME))?;
        Ok(Self {
            name,
            inputs,
            outputs,
        })
    }
}

impl DynNode for Debug {
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
        chain.implement_inline_node(
            self,
            self.inputs.single(),
            self.outputs.single(),
            &quote! {
                #[allow(clippy::let_and_return)]
                input
            },
        );
    }
}

pub fn debug<R: TypeResolver + Copy>(
    graph: &mut GraphBuilder<R>,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    params: (),
) -> ChainResultWithTrace<Debug> {
    Debug::new(graph, name, inputs, params)
}
