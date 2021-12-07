use crate::{
    chain::{Chain, ImportScope},
    dyn_node,
    graph::{DynNode, Graph, GraphBuilder, Node},
    stream::{NodeStream, NodeStreamSource},
    support::FullyQualifiedName,
};
use datapet_support::AnchorId;
use truc::record::definition::DatumDefinitionOverride;

pub struct Anchorize {
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    outputs: [NodeStream; 1],
    anchor_field: String,
}

impl Node<1, 1> for Anchorize {
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

            let def_output =
                self.outputs[0].definition_fragments(&graph.chain_customizer().streams_module_name);
            let output_record = def_output.record();
            let output_unpacked_record_in = def_output.unpacked_record_in();

            let input = thread.format_input(
                self.inputs[0].source(),
                graph.chain_customizer(),
                &mut import_scope,
            );

            let anchor_field = format_ident!("{}", self.anchor_field);

            let fn_def = quote! {
                  pub fn #fn_name(#[allow(unused_mut)] mut thread_control: #thread_module::ThreadControl) -> impl FallibleIterator<Item = #output_record, Error = #error_type> {
                      let mut seq: usize = 0;
                      #input
                      input
                          .map(move |record| {
                              let anchor = seq;
                              seq = anchor + 1;
                              Ok(#output_record::from((
                                  record,
                                  #output_unpacked_record_in { #anchor_field: datapet_support::AnchorId::new(anchor) },
                              )))
                          })
                  }
            };
            scope.raw(&fn_def.to_string());
        }

        import_scope.import(scope, graph.chain_customizer());

        chain.update_thread_single_stream(thread.thread_id, &self.outputs[0]);
    }
}

dyn_node!(Anchorize);

pub fn anchorize(
    graph: &mut GraphBuilder,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    anchor_field: &str,
) -> Anchorize {
    let [input] = inputs;

    let anchor_table_id = graph.new_anchor_table();

    let variant_id = {
        let mut stream = graph
            .get_stream(input.record_type())
            .unwrap_or_else(|| panic!(r#"stream "{}""#, input.record_type()))
            .borrow_mut();

        stream.add_datum_override::<AnchorId<0>, _>(
            anchor_field,
            DatumDefinitionOverride {
                type_name: Some(format!("datapet_support::AnchorId<{}>", anchor_table_id)),
                size: None,
                allow_uninit: Some(true),
            },
        );
        stream.close_record_variant()
    };

    let record_type = input.record_type().clone();
    Anchorize {
        name: name.clone(),
        inputs: [input],
        outputs: [NodeStream::new(
            record_type,
            variant_id,
            NodeStreamSource::from(name),
        )],
        anchor_field: anchor_field.to_string(),
    }
}
