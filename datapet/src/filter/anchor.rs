use crate::{prelude::*, stream::UniqueNodeStream};
use datapet_support::AnchorId;
use truc::record::{definition::DatumDefinitionOverride, type_resolver::TypeResolver};

#[derive(Getters)]
pub struct Anchorize {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 1],
    #[getset(get = "pub")]
    outputs: [NodeStream; 1],
    anchor_field: String,
}

impl Anchorize {
    fn new<R: TypeResolver + Copy>(
        graph: &mut GraphBuilder<R>,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        anchor_field: &str,
    ) -> Self {
        let anchor_table_id = graph.new_anchor_table();

        let mut streams = StreamsBuilder::new(&name, &inputs);

        {
            let output_stream = streams.output_from_input(0, graph).for_update();
            let mut output_stream_def = output_stream.borrow_mut();
            output_stream_def.add_datum_override::<AnchorId<0>, _>(
                anchor_field,
                DatumDefinitionOverride {
                    type_name: Some(format!("datapet_support::AnchorId<{}>", anchor_table_id)),
                    size: None,
                    align: None,
                    allow_uninit: Some(true),
                },
            );
        }

        let outputs = streams.build();

        Self {
            name,
            inputs,
            outputs,
            anchor_field: anchor_field.to_string(),
        }
    }
}

impl DynNode for Anchorize {
    fn gen_chain(&self, graph: &Graph, chain: &mut Chain) {
        let thread = chain.get_thread_id_and_module_by_source(
            self.inputs.unique(),
            &self.name,
            self.outputs.some_unique(),
        );

        let def_output = chain.stream_definition_fragments(self.outputs.unique());

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

            let output_record = def_output.record();
            let output_unpacked_record_in = def_output.unpacked_record_in();

            let input = thread.format_input(
                self.inputs.unique().source(),
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
    }
}

pub fn anchorize<R: TypeResolver + Copy>(
    graph: &mut GraphBuilder<R>,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    anchor_field: &str,
) -> Anchorize {
    Anchorize::new(graph, name, inputs, anchor_field)
}
