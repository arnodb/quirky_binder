use crate::{prelude::*, trace_element};

const PIPE_TRACE_NAME: &str = "pipe";

#[derive(Debug, Getters)]
pub struct Pipe {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 1],
    #[getset(get = "pub")]
    outputs: [NodeStream; 1],
}

impl Pipe {
    fn new(
        graph: &mut GraphBuilder,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        _params: (),
    ) -> ChainResultWithTrace<Self> {
        let mut streams = StreamsBuilder::new(&name, &inputs);
        streams
            .output_from_input(0, true, graph)
            .with_trace_element(trace_element!(PIPE_TRACE_NAME))?
            .pass_through(|_, facts_proof| {
                Ok(facts_proof.order_facts_updated().distinct_facts_updated())
            })?;
        let outputs = streams
            .build()
            .with_trace_element(trace_element!(PIPE_TRACE_NAME))?;

        Ok(Self {
            name,
            inputs,
            outputs,
        })
    }
}

impl DynNode for Pipe {
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
        chain.pipe_inputs(&self.name, &self.inputs);

        let inline_body = quote! {
            input
        };

        chain.implement_inline_node(
            self,
            self.inputs.some_single(),
            self.outputs.single(),
            &inline_body,
        );
    }
}

pub fn pipe(
    graph: &mut GraphBuilder,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    params: (),
) -> ChainResultWithTrace<Pipe> {
    Pipe::new(graph, name, inputs, params)
}
