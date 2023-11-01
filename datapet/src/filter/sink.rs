use crate::prelude::*;
use proc_macro2::TokenStream;
use serde::Deserialize;
use truc::record::type_resolver::TypeResolver;

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct SinkParams<'a> {
    debug: Option<&'a str>,
}

#[derive(Getters)]
pub struct Sink {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 1],
    #[getset(get = "pub")]
    outputs: [NodeStream; 0],
    debug: Option<String>,
}

impl Sink {
    fn new<R: TypeResolver + Copy>(
        _graph: &mut GraphBuilder<R>,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        params: SinkParams,
    ) -> Self {
        Self {
            name,
            inputs,
            outputs: [],
            debug: params.debug.map(ToString::to_string),
        }
    }
}

impl DynNode for Sink {
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

        let input = thread.format_input(self.inputs.single().source(), graph.chain_customizer());

        let debug: Option<TokenStream> = self
            .debug
            .as_ref()
            .map(|debug| debug.parse().expect("function body"));

        let full_name = &self.name.to_string();

        let thread_body = quote! {
            #input
            move || {
                let mut input = input;
                let mut read = 0;
                while let Some(record) = input.next()? {
                    #debug
                    read += 1;
                }
                let full_name = #full_name;
                println!("read {} {}", full_name, read);
                Ok(())
            }
        };

        chain.implement_node_thread(self, thread.thread_id, &thread_body);

        chain.set_thread_main(thread.thread_id, self.name.clone());
    }

    fn all_nodes(&self) -> Box<dyn Iterator<Item = &dyn DynNode> + '_> {
        Box::new(Some(self as &dyn DynNode).into_iter())
    }
}

pub fn sink<R: TypeResolver + Copy>(
    graph: &mut GraphBuilder<R>,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    params: SinkParams,
) -> Sink {
    Sink::new(graph, name, inputs, params)
}
