use truc::record::{definition::DatumDefinition, type_resolver::TypeResolver};

use crate::prelude::*;

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
        _trace: Trace,
    ) -> ChainResult<Self> {
        let mut streams = StreamsBuilder::new(&name, &inputs);
        streams
            .output_from_input(0, true, graph)
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
                ) {
                    let def = graph
                        .get_stream(sub_stream.record_type())
                        .unwrap_or_else(|| panic!(r#"stream "{}""#, sub_stream.record_type()))
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
                        );
                    }
                }
                for (&datum_id, sub_stream) in builder.sub_streams().iter() {
                    debug_sub_stream(
                        1,
                        def.get_datum_definition(datum_id).expect("datum"),
                        sub_stream,
                        graph,
                    );
                }
                facts_proof.order_facts_updated().distinct_facts_updated()
            });
        let outputs = streams.build();
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

    fn all_nodes(&self) -> Box<dyn Iterator<Item = &dyn DynNode> + '_> {
        Box::new(Some(self as &dyn DynNode).into_iter())
    }
}

pub fn debug<R: TypeResolver + Copy>(
    graph: &mut GraphBuilder<R>,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    params: (),
    trace: Trace,
) -> ChainResult<Debug> {
    Debug::new(graph, name, inputs, params, trace)
}
