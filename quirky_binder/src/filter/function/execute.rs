use proc_macro2::TokenStream;
use serde::Deserialize;

use crate::{prelude::*, trace_element};

const FUNCTION_EXECUTE_TRACE_NAME: &str = "function_execute";

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct FunctionExecuteParams<'a> {
    thread_type: Option<ChainThreadType>,
    body: &'a str,
}

#[derive(Debug, Getters)]
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
    fn new(
        _graph: &mut GraphBuilder,
        name: FullyQualifiedName,
        inputs: [NodeStream; 0],
        params: FunctionExecuteParams,
    ) -> ChainResultWithTrace<Self> {
        let valid_body = params
            .body
            .parse::<TokenStream>()
            .map_err(|err| ChainError::InvalidTokenStream {
                name: "body".to_owned(),
                msg: err.to_string(),
            })
            .with_trace_element(trace_element!())?;

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

pub fn function_execute(
    graph: &mut GraphBuilder,
    name: FullyQualifiedName,
    inputs: [NodeStream; 0],
    params: FunctionExecuteParams,
) -> ChainResultWithTrace<FunctionExecute> {
    let _trace_name = TraceName::push(FUNCTION_EXECUTE_TRACE_NAME);
    FunctionExecute::new(graph, name, inputs, params)
}
