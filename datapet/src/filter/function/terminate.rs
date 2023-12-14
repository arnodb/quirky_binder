use crate::prelude::*;
use proc_macro2::TokenStream;
use serde::Deserialize;
use truc::record::type_resolver::TypeResolver;

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct FunctionTerminateParams<'a> {
    body: &'a str,
}

#[derive(Getters)]
pub struct FunctionTerminate {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 1],
    #[getset(get = "pub")]
    outputs: [NodeStream; 0],
    body: String,
}

impl FunctionTerminate {
    fn new<R: TypeResolver + Copy>(
        _graph: &mut GraphBuilder<R>,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        params: FunctionTerminateParams,
    ) -> ChainResult<Self> {
        Ok(Self {
            name,
            inputs,
            outputs: [],
            body: params.body.to_owned(),
        })
    }
}

impl DynNode for FunctionTerminate {
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
        let thread = chain.get_thread_id_and_module_by_source(
            self.inputs.single(),
            &self.name,
            self.outputs.none(),
        );

        let input = thread.format_input(
            self.inputs.single().source(),
            graph.chain_customizer(),
            true,
        );

        let body: TokenStream = self.body.parse().expect("function body");

        let thread_body = quote! {
            #input
            move || {
                #body
            }
        };

        chain.implement_node_thread(self, thread.thread_id, &thread_body);

        chain.set_thread_main(thread.thread_id, self.name.clone());
    }

    fn all_nodes(&self) -> Box<dyn Iterator<Item = &dyn DynNode> + '_> {
        Box::new(Some(self as &dyn DynNode).into_iter())
    }
}

pub fn function_terminate<R: TypeResolver + Copy>(
    graph: &mut GraphBuilder<R>,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    params: FunctionTerminateParams,
) -> ChainResult<FunctionTerminate> {
    FunctionTerminate::new(graph, name, inputs, params)
}
