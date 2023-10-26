use crate::prelude::*;
use truc::record::type_resolver::TypeResolver;

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
    ) -> Self {
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
                facts_proof.order_facts_updated().distinct_facts_updated()
            });
        let outputs = streams.build();
        Self {
            name,
            inputs,
            outputs,
        }
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
) -> Debug {
    Debug::new(graph, name, inputs)
}
