use crate::prelude::*;
use datapet_support::AnchorId;
use std::cell::RefCell;
use truc::record::definition::{DatumDefinitionOverride, RecordDefinitionBuilder, RecordVariantId};

#[datapet_node(
    in = "-",
    out_mut = "-",
    init = "graph_and_streams",
    arg = "anchor_field: &str",
    fields = "anchor_field: anchor_field.to_string()"
)]
pub struct Anchorize {
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    outputs: [NodeStream; 1],
    anchor_field: String,
}

impl Anchorize {
    fn initialize_graph(graph: &mut GraphBuilder) -> usize {
        graph.new_anchor_table()
    }

    fn initialize_streams(
        (_, _): (&RefCell<RecordDefinitionBuilder>, RecordVariantId),
        output_stream: &RefCell<RecordDefinitionBuilder>,
        anchor_field: &str,
        anchor_table_id: usize,
    ) -> [RecordVariantId; 1] {
        let mut output_stream = output_stream.borrow_mut();
        output_stream.add_datum_override::<AnchorId<0>, _>(
            anchor_field,
            DatumDefinitionOverride {
                type_name: Some(format!("datapet_support::AnchorId<{}>", anchor_table_id)),
                size: None,
                allow_uninit: Some(true),
            },
        );
        [output_stream.close_record_variant()]
    }
}

impl DynNode for Anchorize {
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

pub fn anchorize(
    graph: &mut GraphBuilder,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    anchor_field: &str,
) -> Anchorize {
    Anchorize::new(graph, name, inputs, anchor_field)
}
