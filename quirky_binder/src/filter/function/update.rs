use proc_macro2::TokenStream;
use serde::Deserialize;
use truc::record::type_resolver::TypeResolver;

use crate::{prelude::*, trace_filter};

const FUNCTION_UPDATE_TRACE_NAME: &str = "function_update";

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct FunctionUpdateParams<'a> {
    body: &'a str,
}

#[derive(Getters)]
pub struct FunctionUpdate {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 1],
    #[getset(get = "pub")]
    outputs: [NodeStream; 1],
    body: TokenStream,
}

impl FunctionUpdate {
    fn new<R: TypeResolver + Copy>(
        graph: &mut GraphBuilder<R>,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        params: FunctionUpdateParams,
        trace: Trace,
    ) -> ChainResult<Self> {
        let valid_body =
            params
                .body
                .parse::<TokenStream>()
                .map_err(|err| ChainError::InvalidTokenStream {
                    name: "body".to_owned(),
                    msg: err.to_string(),
                    trace: trace_filter!(trace, FUNCTION_UPDATE_TRACE_NAME),
                })?;

        let mut streams = StreamsBuilder::new(&name, &inputs);
        streams
            .output_from_input(0, true, graph)
            .pass_through(|_, facts_proof| {
                facts_proof.order_facts_updated().distinct_facts_updated()
            });
        let outputs = streams.build();
        Ok(Self {
            name,
            inputs,
            outputs,
            body: valid_body,
        })
    }
}

impl DynNode for FunctionUpdate {
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
        let body = &self.body;

        chain.implement_inline_node(self, self.inputs.single(), self.outputs.single(), body);
    }

    fn all_nodes(&self) -> Box<dyn Iterator<Item = &dyn DynNode> + '_> {
        Box::new(Some(self as &dyn DynNode).into_iter())
    }
}

pub fn function_update<R: TypeResolver + Copy>(
    graph: &mut GraphBuilder<R>,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    params: FunctionUpdateParams,
    trace: Trace,
) -> ChainResult<FunctionUpdate> {
    FunctionUpdate::new(graph, name, inputs, params, trace)
}
