use crate::prelude::*;
use proc_macro2::TokenStream;

#[derive(Getters)]
pub struct Sink {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 1],
    #[getset(get = "pub")]
    outputs: [NodeStream; 0],
    debug: Option<TokenStream>,
}

impl Sink {
    fn new(
        _graph: &mut GraphBuilder,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        debug: Option<TokenStream>,
    ) -> Self {
        Self {
            name,
            inputs,
            outputs: [],
            debug,
        }
    }
}

impl DynNode for Sink {
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

            let full_name = &self.name.to_string();

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
                          println!("read {} {}", #full_name, read);
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

pub fn sink(
    graph: &mut GraphBuilder,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    debug: Option<TokenStream>,
) -> Sink {
    Sink::new(graph, name, inputs, debug)
}
