use truc::record::type_resolver::TypeResolver;

use crate::{prelude::*, trace_filter};

const ACCUMULATE_TRACE_NAME: &str = "accumulate";

#[derive(Getters)]
pub struct Accumulate {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 1],
    #[getset(get = "pub")]
    outputs: [NodeStream; 1],
}

impl Accumulate {
    fn new<R: TypeResolver + Copy>(
        graph: &mut GraphBuilder<R>,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        _params: (),
        trace: Trace,
    ) -> ChainResult<Self> {
        let mut streams = StreamsBuilder::new(&name, &inputs);
        streams
            .output_from_input(0, true, graph, || {
                trace_filter!(trace, ACCUMULATE_TRACE_NAME)
            })?
            .pass_through(|_, facts_proof| {
                Ok(facts_proof.order_facts_updated().distinct_facts_updated())
            })?;
        let outputs = streams.build(|| trace_filter!(trace, ACCUMULATE_TRACE_NAME))?;

        Ok(Self {
            name,
            inputs,
            outputs,
        })
    }
}

impl DynNode for Accumulate {
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
            quirky_binder_support::iterator::accumulate::Accumulate::new(input)
        };

        chain.implement_inline_node(
            self,
            self.inputs.single(),
            self.outputs.single(),
            &inline_body,
        );
    }
}

pub fn accumulate<R: TypeResolver + Copy>(
    graph: &mut GraphBuilder<R>,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    params: (),
    trace: Trace,
) -> ChainResult<Accumulate> {
    Accumulate::new(graph, name, inputs, params, trace)
}
