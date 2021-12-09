use crate::{prelude::*, support::fields_eq};

#[datapet_node(in = "-", out = "-")]
pub struct Dedup {
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    outputs: [NodeStream; 1],
}

impl DynNode for Dedup {
    fn gen_chain(&self, graph: &Graph, chain: &mut Chain) {
        let thread = chain.get_thread_id_and_module_by_source(self.inputs[0].source(), &self.name);

        let scope = chain.get_or_new_module_scope(
            self.name.iter().take(self.name.len() - 1),
            graph.chain_customizer(),
            thread.thread_id,
        );
        let mut import_scope = ImportScope::default();
        import_scope.add_import_with_error_type("fallible_iterator", "FallibleIterator");

        {
            let fn_name = format_ident!("{}", **self.name.last().expect("local name"));
            let thread_module = format_ident!("thread_{}", thread.thread_id);
            let error_type = graph.chain_customizer().error_type.to_name();

            let def =
                self.outputs[0].definition_fragments(&graph.chain_customizer().streams_module_name);
            let record = def.record();

            let input = thread.format_input(
                self.inputs[0].source(),
                graph.chain_customizer(),
                &mut import_scope,
            );

            let record_definition = &graph.record_definitions()[self.inputs[0].record_type()];
            let variant = record_definition
                .get_variant(self.inputs[0].variant_id())
                .unwrap_or_else(|| panic!("variant #{}", self.inputs[0].variant_id()));
            let eq = fields_eq(variant.data().map(|d| {
                let datum = record_definition
                    .get_datum_definition(d)
                    .unwrap_or_else(|| panic!("datum #{}", d));
                datum.name()
            }));

            let fn_def = quote! {
                  pub fn #fn_name(#[allow(unused_mut)] mut thread_control: #thread_module::ThreadControl) -> impl FallibleIterator<Item = #record, Error = #error_type> {
                      #input
                      datapet_support::iterator::dedup::Dedup::new(
                          input,
                          #eq,
                      )
                  }
            };
            scope.raw(&fn_def.to_string());
        }

        import_scope.import(scope, graph.chain_customizer());

        chain.update_thread_single_stream(thread.thread_id, &self.outputs[0]);
    }
}

pub fn dedup(graph: &mut GraphBuilder, name: FullyQualifiedName, inputs: [NodeStream; 1]) -> Dedup {
    Dedup::new(graph, name, inputs)
}
