use proc_macro2::TokenStream;
use serde::Deserialize;

use crate::{prelude::*, trace_element};

const FUNCTION_TERMINATE_TRACE_NAME: &str = "function_terminate";

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct FunctionTerminateParams<'a> {
    body: &'a str,
}

#[derive(Debug, Getters)]
pub struct FunctionTerminate<const N: usize> {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; N],
    #[getset(get = "pub")]
    outputs: [NodeStream; 0],
    body: TokenStream,
}

impl<const N: usize> FunctionTerminate<N> {
    fn new(
        _graph: &mut GraphBuilder,
        name: FullyQualifiedName,
        inputs: [NodeStream; N],
        params: FunctionTerminateParams,
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

    fn gen_chain(&self, _graph: &Graph, chain: &mut Chain) {
        let (thread_id, inputs) = if self.inputs.len() == 1 {
            let thread =
                chain.get_thread_by_source(&self.inputs[0], &self.name, self.outputs.none());

            let input = chain.format_source_thread_input(
                &thread,
                self.inputs[0].source(),
                true,
                NodeStatisticsOption::WithStatistics {
                    node_name: &self.name,
                },
            );

            (thread.thread_id, vec![input])
        } else {
            let thread_id = chain.pipe_inputs_with_thread(&self.name, &self.inputs, &self.outputs);

            let inputs = (0..self.inputs.len())
                .map(|input_index| {
                    let input_name = format_ident!("input_{}", input_index);
                    let input = chain.format_thread_input(
                        thread_id,
                        input_index,
                        NodeStatisticsOption::WithStatistics {
                            node_name: &self.name,
                        },
                    );
                    quote! { let #input_name = #input; }
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

pub fn function_terminate<const N: usize>(
    graph: &mut GraphBuilder,
    name: FullyQualifiedName,
    inputs: [NodeStream; N],
    params: FunctionTerminateParams,
) -> ChainResultWithTrace<FunctionTerminate<N>> {
    let _trace_name = TraceName::push(FUNCTION_TERMINATE_TRACE_NAME);
    FunctionTerminate::new(graph, name, inputs, params)
}
