use proc_macro2::TokenStream;
use serde::Deserialize;
use truc::record::type_resolver::TypeResolver;

use crate::{prelude::*, trace_filter};

const FUNCTION_TERMINATE_TRACE_NAME: &str = "function_terminate";

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct FunctionTerminateParams<'a> {
    body: &'a str,
}

#[derive(Getters)]
pub struct FunctionTerminate<const N: usize> {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; N],
    #[getset(get = "pub")]
    outputs: [NodeStream; 0],
    body: TokenStream,
}

impl<const N: usize> FunctionTerminate<N> {
    fn new<R: TypeResolver + Copy>(
        _graph: &mut GraphBuilder<R>,
        name: FullyQualifiedName,
        inputs: [NodeStream; N],
        params: FunctionTerminateParams,
        trace: Trace,
    ) -> ChainResult<Self> {
        let valid_body =
            params
                .body
                .parse::<TokenStream>()
                .map_err(|err| ChainError::InvalidTokenStream {
                    name: "body".to_owned(),
                    msg: err.to_string(),
                    trace: trace_filter!(trace, FUNCTION_TERMINATE_TRACE_NAME),
                })?;

        Ok(Self {
            name,
            inputs,
            outputs: [],
            body: valid_body,
        })
    }
}

impl<const N: usize> DynNode for FunctionTerminate<N> {
    fn name(&self) -> &FullyQualifiedName {
        &self.name
    }

    fn inputs(&self) -> &[NodeStream] {
        &self.inputs
    }

    fn outputs(&self) -> &[NodeStream] {
        &self.outputs
    }

    fn gen_chain(&self, graph: &Graph, chain: &mut Chain) {
        let (thread_id, inputs) = if self.inputs.len() == 1 {
            let thread =
                chain.get_thread_by_source(&self.inputs[0], &self.name, self.outputs.none());

            let input =
                thread.format_input(self.inputs[0].source(), graph.chain_customizer(), true);

            (thread.thread_id, vec![input])
        } else {
            let thread_id = chain.pipe_inputs(&self.name, &self.inputs, &self.outputs);

            let inputs = (0..self.inputs.len())
                .map(|input_index| {
                    let input = format_ident!("input_{}", input_index);
                    let expect = format!("input {}", input_index);
                    quote! { let #input = thread_control.#input.take().expect(#expect); }
                })
                .collect::<Vec<_>>();

            (thread_id, inputs)
        };

        let drop_thread_control = if inputs.is_empty() {
            Some(quote! { drop(thread_control); })
        } else {
            None
        };

        let body = &self.body;

        let thread_body = quote! {
            #(
                #inputs
            )*

            #drop_thread_control

            move || {
                #body
            }
        };

        chain.implement_node_thread(self, thread_id, &thread_body);

        chain.set_thread_main(thread_id, self.name.clone());
    }
}

pub fn function_terminate<const N: usize, R: TypeResolver + Copy>(
    graph: &mut GraphBuilder<R>,
    name: FullyQualifiedName,
    inputs: [NodeStream; N],
    params: FunctionTerminateParams,
    trace: Trace,
) -> ChainResult<FunctionTerminate<N>> {
    FunctionTerminate::new(graph, name, inputs, params, trace)
}
