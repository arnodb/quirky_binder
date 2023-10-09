use crate::{prelude::*, stream::UniqueNodeStream, support::fields_eq_ab};
use truc::record::type_resolver::TypeResolver;

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
            let mut output_stream = streams.output_from_input(0, graph).for_update();
            let group_stream = output_stream.new_named_sub_stream("group", graph);
            let variant_id = output_stream.input_variant_id();

            {
                let mut output_stream_def = output_stream.borrow_mut();
                let mut group_stream_def = group_stream.borrow_mut();
                for &field in fields {
                    let datum = output_stream_def
                        .get_variant_datum_definition_by_name(variant_id, field)
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
            output_stream.add_vec_datum(
                group_field,
                &format!(
                    "{module_name}::Record{group_variant_id}",
                    module_name = module_name,
                    group_variant_id = group_stream.variant_id(),
                ),
                group_stream.clone(),
            );

            group_stream
        };

        let outputs = streams.build(graph);

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
    fn name(&self) -> &FullyQualifiedName {
        &self.name
    }

    fn gen_chain(&self, graph: &Graph, chain: &mut Chain) {
        let def_input = chain.stream_definition_fragments(self.inputs.unique());
        let def = chain.stream_definition_fragments(self.outputs.unique());
        let def_group = chain.stream_definition_fragments(&self.group_stream);

        let input_unpacked_record = def_input.unpacked_record();
        let unpacked_record_in = def.unpacked_record_in();
        let record_and_unpacked_out = def.record_and_unpacked_out();
        let group_record = def_group.record();
        let group_unpacked_record = def_group.unpacked_record();

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
        let variant = &record_definition[self.inputs.unique().variant_id()];
        let eq = {
            let fields = variant
                .data()
                .filter_map(|d| {
                    let datum = &record_definition[d];
                    if !self.fields.iter().any(|f| f == datum.name())
                        && datum.name() != self.group_field
                    {
                        Some(datum.name())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            fields_eq_ab(
                &def.record(),
                fields.iter(),
                &def_input.record(),
                fields.iter(),
            )
        };

        let inline_body = quote! {
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
        };

        chain.implement_inline_node(
            self,
            self.inputs.unique(),
            self.outputs.unique(),
            &inline_body,
        );
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
