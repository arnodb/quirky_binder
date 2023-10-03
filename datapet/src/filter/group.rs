use crate::{prelude::*, stream::UniqueNodeStream, support::fields_eq};
use truc::record::{definition::DatumDefinitionOverride, type_resolver::TypeResolver};

#[derive(Getters)]
pub struct Group {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 1],
    #[getset(get = "pub")]
    outputs: [NodeStream; 1],
    group_field: String,
    group_stream: NodeStream,
    fields: Vec<String>,
}

impl Group {
    pub fn new<R: TypeResolver + Copy>(
        graph: &mut GraphBuilder<R>,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        fields: &[&str],
        group_field: &str,
    ) -> Group {
        let mut streams = StreamsBuilder::new(&name, &inputs);
        streams.new_named_stream("group", graph);

        let group_stream = {
            let output_stream = streams.output_from_input(0, graph).for_update();
            let group_stream = output_stream.new_named_sub_stream("group", graph);
            let input_variant_id = output_stream.input_variant_id();
            let mut output_stream_def = output_stream.borrow_mut();

            {
                let mut group_stream_def = group_stream.borrow_mut();
                for &field in fields {
                    let datum = output_stream_def
                        .get_variant_datum_definition_by_name(input_variant_id, field)
                        .unwrap_or_else(|| panic!(r#"datum "{}""#, field));
                    group_stream_def.copy_datum(datum);
                    let datum_id = datum.id();
                    output_stream_def.remove_datum(datum_id);
                }
            }
            let group_stream = group_stream.close_record_variant();

            let module_name = graph
                .chain_customizer()
                .streams_module_name
                .sub_n(&***group_stream.record_type());
            output_stream_def.add_datum_override::<Vec<()>, _>(
                group_field,
                DatumDefinitionOverride {
                    type_name: Some(format!(
                        "Vec<{module_name}::Record{group_variant_id}>",
                        module_name = module_name,
                        group_variant_id = group_stream.variant_id(),
                    )),
                    size: None,
                    align: None,
                    allow_uninit: None,
                },
            );

            group_stream
        };

        let outputs = streams.build();

        Group {
            name: name.clone(),
            inputs,
            outputs,
            group_field: group_field.to_string(),
            group_stream,
            fields: fields.iter().map(ToString::to_string).collect::<Vec<_>>(),
        }
    }
}

impl DynNode for Group {
    fn gen_chain(&self, graph: &Graph, chain: &mut Chain) {
        let thread = chain.get_thread_id_and_module_by_source(
            self.inputs.unique(),
            &self.name,
            self.outputs.some_unique(),
        );

        let def_input = chain.stream_definition_fragments(self.inputs.unique());
        let def = chain.stream_definition_fragments(self.outputs.unique());
        let def_group = chain.stream_definition_fragments(&self.group_stream);

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

            let input_unpacked_record = def_input.unpacked_record();
            let record = def.record();
            let unpacked_record_in = def.unpacked_record_in();
            let record_and_unpacked_out = def.record_and_unpacked_out();
            let group_record = def_group.record();
            let group_unpacked_record = def_group.unpacked_record();

            let input = thread.format_input(
                self.inputs.unique().source(),
                graph.chain_customizer(),
                &mut import_scope,
            );

            let fields = {
                let names = self
                    .fields
                    .iter()
                    .map(|name| format_ident!("{}", name))
                    .collect::<Vec<_>>();
                quote!(#(#names),*)
            };

            let group_field = format_ident!("{}", self.group_field);
            let mut_group_field = format_ident!("{}_mut", self.group_field);

            let record_definition = &graph.record_definitions()[self.inputs.unique().record_type()];
            let variant = record_definition
                .get_variant(self.inputs.unique().variant_id())
                .unwrap_or_else(|| panic!("variant #{}", self.inputs.unique().variant_id()));
            let eq = fields_eq(variant.data().filter_map(|d| {
                let datum = &record_definition[d];
                if !self.fields.iter().any(|f| f == datum.name())
                    && datum.name() != self.group_field
                {
                    Some(datum.name())
                } else {
                    None
                }
            }));

            let fn_def = quote! {
                  pub fn #fn_name(#[allow(unused_mut)] mut thread_control: #thread_module::ThreadControl) -> impl FallibleIterator<Item = #record, Error = #error_type> {
                      #input
                      datapet_support::iterator::group::Group::new(
                          input,
                          |rec| {{
                              let #record_and_unpacked_out { mut record, #fields } = #record_and_unpacked_out::from((rec, #unpacked_record_in { #group_field: Vec::new() }));
                              let group_record = #group_record::new(#group_unpacked_record { #fields });
                              record.#mut_group_field().push(group_record);
                              record
                          }},
                          #eq,
                          |group, rec| {
                              let #input_unpacked_record{ #fields, .. } = rec.unpack();
                              let group_record = #group_record::new(#group_unpacked_record { #fields });
                              group.#mut_group_field().push(group_record);
                          },
                      )
                  }
            };
            scope.raw(&fn_def.to_string());
        }

        import_scope.import(scope, graph.chain_customizer());
    }
}

pub fn group<R: TypeResolver + Copy>(
    graph: &mut GraphBuilder<R>,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    fields: &[&str],
    group_field: &str,
) -> Group {
    Group::new(graph, name, inputs, fields, group_field)
}
