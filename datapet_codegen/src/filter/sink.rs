use proc_macro2::TokenStream;

use crate::{
    chain::{Chain, ImportScope},
    dyn_node,
    graph::{DynNode, Graph, GraphBuilder, Node},
    stream::NodeStream,
    support::FullyQualifiedName,
};

pub struct Sink {
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    debug: Option<TokenStream>,
}

impl Node<1, 0> for Sink {
    fn inputs(&self) -> &[NodeStream; 1] {
        &self.inputs
    }

    fn outputs(&self) -> &[NodeStream; 0] {
        &[]
    }

    fn gen_chain(&self, graph: &Graph, chain: &mut Chain) {
        let thread = chain.get_thread_id_and_module_by_source(self.inputs[0].source(), &self.name);

        let scope = chain.get_or_new_module_scope(
            self.name.iter().take(self.name.len() - 1),
            graph.chain_customizer(),
            thread.thread_id,
        );
        let mut import_scope = ImportScope::default();

        {
            let fn_name = format_ident!("{}", **self.name.last().expect("local name"));
            let thread_module = format_ident!("thread_{}", thread.thread_id);
            let error_type = graph.chain_customizer().error_type.to_name();

            let input = thread.format_input(
                self.inputs[0].source(),
                graph.chain_customizer(),
                &mut import_scope,
            );

            let debug = &self.debug;

            let fn_def = quote! {
                  pub fn #fn_name(#[allow(unused_mut)] mut thread_control: #thread_module::ThreadControl) -> impl FnOnce() -> Result<(), #error_type> {
                      move || {
                          #input
                          let mut input = input;
                          let mut read = 0;
                          while let Some(record) = input.next()? {
                              #debug
                              read += 1;
                          }
                          println!("read #full_name {}", read);
                          Ok(())
                      }
                  }
            };
            scope.raw(&fn_def.to_string());
        }

        import_scope.import(scope, graph.chain_customizer());

        chain.set_thread_main(thread.thread_id, self.name.clone());
    }
}

dyn_node!(Sink);

pub fn sink(
    _graph: &mut GraphBuilder,
    name: FullyQualifiedName,
    input: NodeStream,
    debug: Option<TokenStream>,
) -> Sink {
    Sink {
        name,
        inputs: [input],
        debug,
    }
}
