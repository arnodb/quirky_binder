use proc_macro2::TokenStream;
use serde::Deserialize;
use truc::record::type_resolver::TypeResolver;

use crate::{prelude::*, trace_filter};

const FUNCTION_EXECUTE_TRACE_NAME: &str = "function_execute";

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct FunctionExecuteParams<'a> {
    thread_type: Option<ChainThreadType>,
    body: &'a str,
}

#[derive(Getters)]
pub struct FunctionExecute {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 0],
    #[getset(get = "pub")]
    outputs: [NodeStream; 0],
    thread_type: ChainThreadType,
    body: TokenStream,
}

impl FunctionExecute {
    fn new<R: TypeResolver + Copy>(
        _graph: &mut GraphBuilder<R>,
        name: FullyQualifiedName,
        inputs: [NodeStream; 0],
        params: FunctionExecuteParams,
        trace: Trace,
    ) -> ChainResult<Self> {
        let valid_body =
            params
                .body
                .parse::<TokenStream>()
                .map_err(|err| ChainError::InvalidTokenStream {
                    name: "body".to_owned(),
                    msg: err.to_string(),
                    trace: trace_filter!(trace, FUNCTION_EXECUTE_TRACE_NAME),
                })?;

        Ok(Self {
            name,
            inputs,
            outputs: [],
            thread_type: params.thread_type.unwrap_or_default(),
            body: valid_body,
        })
    }
}

impl DynNode for FunctionExecute {
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
        let thread_id =
            chain.new_threaded_source(&self.name, self.thread_type, &self.inputs, &self.outputs);

        let body = &self.body;

        chain.implement_node_thread(
            self,
            thread_id,
            &quote! {
                move || {
                    #body
                }
            },
        );
    }
}

pub fn function_execute<R: TypeResolver + Copy>(
    graph: &mut GraphBuilder<R>,
    name: FullyQualifiedName,
    inputs: [NodeStream; 0],
    params: FunctionExecuteParams,
    trace: Trace,
) -> ChainResult<FunctionExecute> {
    FunctionExecute::new(graph, name, inputs, params, trace)
}
