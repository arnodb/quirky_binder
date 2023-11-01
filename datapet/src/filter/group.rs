use crate::{
    graph::builder::assert_undirected_order_starts_with,
    prelude::*,
    support::eq::{fields_eq, fields_eq_ab},
};
use serde::Deserialize;
use truc::record::type_resolver::TypeResolver;

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct GroupParams<'a> {
    fields: FieldsParam<'a>,
    group_field: &'a str,
}

#[derive(Getters)]
pub struct Group {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 1],
    #[getset(get = "pub")]
    outputs: [NodeStream; 1],
    group_field: String,
    group_stream: NodeSubStream,
    fields: Vec<String>,
}

impl Group {
    pub fn new<R: TypeResolver + Copy>(
        graph: &mut GraphBuilder<R>,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        params: GroupParams,
    ) -> Group {
        let mut streams = StreamsBuilder::new(&name, &inputs);
        streams.new_named_stream("group", graph);

        let group_stream =
            streams
                .output_from_input(0, true, graph)
                .update(|output_stream, facts_proof| {
                    let group_stream = output_stream.new_named_sub_stream("group", graph);
                    let variant_id = output_stream.input_variant_id();

                    let (group_by_datum_ids, group_datum_ids) = {
                        let mut output_stream_def = output_stream.record_definition().borrow_mut();
                        let mut group_stream_def = group_stream.record_definition().borrow_mut();

                        let variant = &output_stream_def[variant_id];
                        let mut group_by_datum_ids =
                            Vec::with_capacity(variant.data_len() - params.fields.len());
                        let mut group_data = Vec::with_capacity(params.fields.len());
                        let mut group_datum_ids = Vec::with_capacity(params.fields.len());
                        for datum_id in variant.data() {
                            let datum = &output_stream_def[datum_id];
                            if params.fields.iter().any(|field| *field == datum.name()) {
                                group_data.push(datum);
                                group_datum_ids.push(datum.id());
                            } else {
                                group_by_datum_ids.push(datum.id());
                            }
                        }

                        assert_undirected_order_starts_with(
                            &group_by_datum_ids,
                            output_stream.facts().order(),
                            &*output_stream_def,
                            &name,
                            "main stream",
                        );

                        for datum in &group_data {
                            group_stream_def.copy_datum(datum);
                        }
                        for datum_id in &group_datum_ids {
                            output_stream_def.remove_datum(*datum_id);
                        }

                        (group_by_datum_ids, group_datum_ids)
                    };
                    let group_stream = group_stream.close_record_variant();

                    let module_name = graph
                        .chain_customizer()
                        .streams_module_name
                        .sub_n(&***group_stream.record_type());
                    output_stream.add_vec_datum(
                        params.group_field,
                        &format!(
                            "{module_name}::Record{group_variant_id}",
                            module_name = module_name,
                            group_variant_id = group_stream.variant_id(),
                        ),
                        group_stream.clone(),
                    );

                    output_stream.break_order_fact_at_ids(group_datum_ids.iter().cloned());
                    output_stream.set_distinct_fact_ids(group_by_datum_ids);

                    facts_proof
                        .order_facts_updated()
                        .distinct_facts_updated()
                        .with_output(group_stream)
                });

        let outputs = streams.build();

        Group {
            name: name.clone(),
            inputs,
            outputs,
            group_field: params.group_field.to_string(),
            group_stream,
            fields: params
                .fields
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
        }
    }
}

impl DynNode for Group {
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
        let def_input = chain.stream_definition_fragments(self.inputs.single());
        let def = chain.stream_definition_fragments(self.outputs.single());
        let def_group = chain.sub_stream_definition_fragments(&self.group_stream);

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

        let record_definition = &graph.record_definitions()[self.inputs.single().record_type()];
        let variant = &record_definition[self.inputs.single().variant_id()];
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
            self.inputs.single(),
            self.outputs.single(),
            &inline_body,
        );
    }

    fn all_nodes(&self) -> Box<dyn Iterator<Item = &dyn DynNode> + '_> {
        Box::new(Some(self as &dyn DynNode).into_iter())
    }
}

pub fn group<R: TypeResolver + Copy>(
    graph: &mut GraphBuilder<R>,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    params: GroupParams,
) -> Group {
    Group::new(graph, name, inputs, params)
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct SubGroupParams<'a> {
    path_fields: FieldsParam<'a>,
    fields: FieldsParam<'a>,
    group_field: &'a str,
}

#[derive(Getters)]
pub struct SubGroup {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 1],
    #[getset(get = "pub")]
    outputs: [NodeStream; 1],
    path_streams: Vec<PathUpdateElement>,
    group_field: String,
    group_stream: NodeSubStream,
    fields: Vec<String>,
}

impl SubGroup {
    pub fn new<R: TypeResolver + Copy>(
        graph: &mut GraphBuilder<R>,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        params: SubGroupParams,
    ) -> SubGroup {
        let mut streams = StreamsBuilder::new(&name, &inputs);
        streams.new_named_stream("group", graph);

        let mut created_group_stream = None;

        let path_streams =
            streams
                .output_from_input(0, true, graph)
                .update(|output_stream, facts_proof| {
                    let path_streams = output_stream.update_path(
                        &params.path_fields,
                        |sub_input_stream, output_stream| {
                            output_stream.update_sub_stream(
                                sub_input_stream,
                                graph,
                                |output_stream, sub_output_stream| {
                                    let group_stream =
                                        output_stream.new_named_sub_stream("group", graph);
                                    let variant_id = sub_output_stream.input_variant_id();

                                    let (group_by_datum_ids, group_datum_ids) = {
                                        let mut output_stream_def =
                                            sub_output_stream.record_definition().borrow_mut();
                                        let mut group_stream_def =
                                            group_stream.record_definition().borrow_mut();

                                        let variant = &output_stream_def[variant_id];
                                        let mut group_by_datum_ids = Vec::with_capacity(
                                            variant.data_len() - params.fields.len(),
                                        );
                                        let mut group_data =
                                            Vec::with_capacity(params.fields.len());
                                        let mut group_datum_ids =
                                            Vec::with_capacity(params.fields.len());
                                        for datum_id in variant.data() {
                                            let datum = &output_stream_def[datum_id];
                                            if params
                                                .fields
                                                .iter()
                                                .any(|field| *field == datum.name())
                                            {
                                                group_data.push(datum);
                                                group_datum_ids.push(datum.id());
                                            } else {
                                                group_by_datum_ids.push(datum.id());
                                            }
                                        }

                                        assert_undirected_order_starts_with(
                                            &group_by_datum_ids,
                                            sub_output_stream.facts().order(),
                                            &output_stream_def,
                                            &name,
                                            "main sub stream",
                                        );

                                        for datum in &group_data {
                                            group_stream_def.copy_datum(datum);
                                        }
                                        for datum_id in &group_datum_ids {
                                            output_stream_def.remove_datum(*datum_id);
                                        }

                                        (group_by_datum_ids, group_datum_ids)
                                    };

                                    let group_stream = group_stream.close_record_variant();

                                    let module_name = graph
                                        .chain_customizer()
                                        .streams_module_name
                                        .sub_n(&***group_stream.record_type());
                                    sub_output_stream.add_vec_datum(
                                        params.group_field,
                                        &format!(
                                            "{module_name}::Record{group_variant_id}",
                                            module_name = module_name,
                                            group_variant_id = group_stream.variant_id(),
                                        ),
                                        group_stream.clone(),
                                    );

                                    created_group_stream = Some(group_stream);

                                    output_stream
                                        .break_order_fact_at_ids(group_datum_ids.iter().cloned());
                                    output_stream.set_distinct_fact_ids(group_by_datum_ids);
                                },
                            )
                        },
                        |field: &str, path_stream, sub_output_stream| {
                            let module_name = graph
                                .chain_customizer()
                                .streams_module_name
                                .sub_n(&***sub_output_stream.record_type());
                            let record = &format!(
                                "{module_name}::Record{group_variant_id}",
                                module_name = module_name,
                                group_variant_id = sub_output_stream.variant_id(),
                            );
                            path_stream.replace_vec_datum(field, record, sub_output_stream);
                        },
                        |field: &str, output_stream, sub_output_stream| {
                            let module_name = graph
                                .chain_customizer()
                                .streams_module_name
                                .sub_n(&***sub_output_stream.record_type());
                            let record = &format!(
                                "{module_name}::Record{group_variant_id}",
                                module_name = module_name,
                                group_variant_id = sub_output_stream.variant_id(),
                            );
                            output_stream.replace_vec_datum(field, record, sub_output_stream);
                        },
                        graph,
                    );

                    facts_proof
                        .order_facts_updated()
                        .distinct_facts_updated()
                        .with_output(path_streams)
                });

        let outputs = streams.build();

        SubGroup {
            name: name.clone(),
            inputs,
            outputs,
            path_streams,
            group_field: params.group_field.to_string(),
            group_stream: created_group_stream.expect("group stream"),
            fields: params
                .fields
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
        }
    }
}

impl DynNode for SubGroup {
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
        let def = chain.stream_definition_fragments(self.outputs.single());
        let def_group = chain.sub_stream_definition_fragments(&self.group_stream);

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

        let eq = {
            let path_stream = self.path_streams.last().expect("last path field");

            let leaf_record_definition =
                &graph.record_definitions()[path_stream.sub_input_stream.record_type()];
            let variant = &leaf_record_definition[path_stream.sub_input_stream.variant_id()];

            let record = chain
                .sub_stream_definition_fragments(&path_stream.sub_output_stream)
                .record();

            fields_eq(
                &record,
                variant.data().filter_map(|d| {
                    let datum = &leaf_record_definition[d];
                    if !self.fields.iter().any(|f| f == datum.name())
                        && datum.name() != self.group_field
                    {
                        Some(datum.name())
                    } else {
                        None
                    }
                }),
            )
        };

        let (group_loops, first_access) = self
            .path_streams
            .iter()
            .rev()
            .fold(None, |tail, path_stream| {
                let out_record_definition =
                    chain.sub_stream_definition_fragments(&path_stream.sub_output_stream);
                let in_record_definition =
                    chain.sub_stream_definition_fragments(&path_stream.sub_input_stream);
                let input_record = in_record_definition.record();
                let record = out_record_definition.record();
                let unpacked_record_in = out_record_definition.unpacked_record_in();
                let record_and_unpacked_out = out_record_definition.record_and_unpacked_out();

                let access = format_ident!("{}", path_stream.field);
                let access_mut = format_ident!("{}_mut", path_stream.field);
                Some(if let Some((tail, sub_access)) = tail {
                    (
                        quote! {
                            // TODO optimize this code in truc
                            let converted = convert_vec_in_place::<#input_record, #record, _>(
                                #access,
                                |rec, _| {
                                    let #record_and_unpacked_out {
                                        mut record,
                                        #sub_access,
                                    } = #record_and_unpacked_out::from((
                                        rec,
                                        #unpacked_record_in { #sub_access: Vec::new() },
                                    ));
                                    #tail
                                    VecElementConversionResult::Converted(record)
                                },
                            );
                            *record.#access_mut() = converted;
                        },
                        access,
                    )
                } else {
                    (
                        quote! {
                            // TODO optimize this code in truc
                            let converted = convert_vec_in_place::<#input_record, #record, _>(
                                #access,
                                |rec, prev_rec| {
                                    let #record_and_unpacked_out {
                                        mut record,
                                        #fields
                                    } = #record_and_unpacked_out::from((
                                        rec, #unpacked_record_in { #group_field: Vec::new() },
                                    ));
                                    if let Some(prev_rec) = prev_rec {
                                        if (eq)(&record, prev_rec) {
                                            // TODO optimize
                                            let group_record = #group_record::new(
                                                #group_unpacked_record { #fields }
                                            );
                                            prev_rec.#mut_group_field().push(group_record);
                                            return VecElementConversionResult::Abandonned;
                                        }
                                    }
                                    let group_record = #group_record::new(
                                        #group_unpacked_record { #fields }
                                    );
                                    record.#mut_group_field().push(group_record);
                                    VecElementConversionResult::Converted(record)
                                },
                            );
                            *record.#access_mut() = converted;
                        },
                        access,
                    )
                })
            })
            .expect("group loops");

        let unpacked_record_in = def.unpacked_record_in();
        let record_and_unpacked_out = def.record_and_unpacked_out();

        let inline_body = quote! {
            use truc_runtime::convert::{convert_vec_in_place, VecElementConversionResult};

            let eq = #eq;

            input.map(move |rec| {
                let #record_and_unpacked_out {
                    mut record,
                    #first_access,
                } = #record_and_unpacked_out::from((
                    rec, #unpacked_record_in { #first_access: Vec::new() },
                ));
                #group_loops
                Ok(record)
            })
        };

        chain.implement_inline_node(
            self,
            self.inputs.single(),
            self.outputs.single(),
            &inline_body,
        );
    }

    fn all_nodes(&self) -> Box<dyn Iterator<Item = &dyn DynNode> + '_> {
        Box::new(Some(self as &dyn DynNode).into_iter())
    }
}

pub fn sub_group<R: TypeResolver + Copy>(
    graph: &mut GraphBuilder<R>,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    params: SubGroupParams,
) -> SubGroup {
    SubGroup::new(graph, name, inputs, params)
}
