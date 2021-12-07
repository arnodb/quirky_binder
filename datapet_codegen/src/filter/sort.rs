use crate::{
    chain::{Chain, ImportScope},
    dyn_node,
    graph::{DynNode, Graph, GraphBuilder, Node},
    stream::{NodeStream, NodeStreamSource},
    support::{fields_cmp, FullyQualifiedName},
};

pub struct Sort {
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    outputs: [NodeStream; 1],
    fields: Vec<String>,
}

impl Node<1, 1> for Sort {
    fn inputs(&self) -> &[NodeStream; 1] {
        &self.inputs
    }

    fn outputs(&self) -> &[NodeStream; 1] {
        &self.outputs
    }

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

            let cmp = fields_cmp(self.fields.iter().map(String::as_str));

            let fn_def = quote! {
                  pub fn #fn_name(#[allow(unused_mut)] mut thread_control: #thread_module::ThreadControl) -> impl FallibleIterator<Item = #record, Error = #error_type> {
                      #input
                      datapet_support::iterator::sort::Sort::new(
                          input,
                          #cmp,
                      )
                  }
            };
            scope.raw(&fn_def.to_string());
        }

        import_scope.import(scope, graph.chain_customizer());

        chain.update_thread_single_stream(thread.thread_id, &self.outputs[0]);
    }
}

dyn_node!(Sort);

pub fn sort(
    _graph: &mut GraphBuilder,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    fields: &[&str],
) -> Sort {
    let [input] = inputs;
    let output = input.with_source(NodeStreamSource::from(name.clone()));
    Sort {
        name,
        inputs: [input],
        outputs: [output],
        fields: fields.iter().map(ToString::to_string).collect::<Vec<_>>(),
    }
}
