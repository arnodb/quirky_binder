use std::collections::BTreeMap;

use serde::Deserialize;
use truc::record::{definition::DatumId, type_resolver::TypeResolver};

use crate::{
    graph::builder::check_undirected_order_starts_with,
    prelude::*,
    support::eq::{fields_eq, fields_eq_ab},
    trace_filter,
};

const GROUP_TRACE_NAME: &str = "group";

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
    group_field: ValidFieldName,
    group_stream: NodeSubStream,
    fields: Vec<ValidFieldName>,
}

impl Group {
    pub fn new<R: TypeResolver + Copy>(
        graph: &mut GraphBuilder<R>,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        params: GroupParams,
        trace: Trace,
    ) -> ChainResult<Group> {
        let valid_fields = params
            .fields
            .validate_on_stream(inputs.single(), graph, || {
                trace_filter!(trace, GROUP_TRACE_NAME)
            })?;
        let valid_group_field = ValidFieldName::try_from(params.group_field).map_err(|_| {
            ChainError::InvalidFieldName {
                name: params.group_field.to_owned(),
                trace: trace_filter!(trace, GROUP_TRACE_NAME),
            }
        })?;

        let mut streams = StreamsBuilder::new(&name, &inputs);
        streams.new_named_stream("group", graph);

        let group_stream =
            streams
                .output_from_input(0, true, graph)
                .update(|output_stream, facts_proof| {
                    let mut group_stream = output_stream.new_named_sub_stream("group", graph);
                    let variant_id = output_stream.input_variant_id();

                    let (group_by_datum_ids, group_datum_ids) = {
                        let mut output_stream_def = output_stream.record_definition().borrow_mut();
                        let mut group_stream_def = group_stream.record_definition().borrow_mut();

                        let variant = &output_stream_def[variant_id];
                        let mut group_by_datum_ids =
                            Vec::with_capacity(variant.data_len() - valid_fields.len());
                        let mut group_data = Vec::with_capacity(valid_fields.len());
                        let mut group_datum_ids = Vec::with_capacity(valid_fields.len());
                        for datum_id in variant.data() {
                            let datum = &output_stream_def[datum_id];
                            if valid_fields
                                .iter()
                                .any(|field| field.name() == datum.name())
                            {
                                group_data.push(datum);
                                group_datum_ids.push(datum.id());
                            } else {
                                group_by_datum_ids.push(datum.id());
                            }
                        }

                        check_undirected_order_starts_with(
                            &group_by_datum_ids,
                            output_stream.facts().order(),
                            &*output_stream_def,
                            "main stream",
                            || trace_filter!(trace, GROUP_TRACE_NAME),
                        )?;

                        let mut map = BTreeMap::<DatumId, DatumId>::new();
                        for datum in &group_data {
                            let new_id = group_stream_def.copy_datum(datum);
                            map.insert(datum.id(), new_id);
                        }
                        for datum_id in &group_datum_ids {
                            output_stream_def.remove_datum(*datum_id);
                        }

                        let group_order = output_stream.facts().order()[group_by_datum_ids.len()..]
                            .iter()
                            .map(|d| d.map(|d| map[&d]))
                            .collect::<Vec<_>>();
                        group_stream.facts_mut().set_order(group_order);
                        let group_distinct = output_stream
                            .facts()
                            .distinct()
                            .iter()
                            .filter_map(|d| map.get(d).copied())
                            .collect::<Vec<_>>();
                        group_stream.facts_mut().set_distinct(group_distinct);

                        (group_by_datum_ids, group_datum_ids)
                    };

                    let group_stream = group_stream.close_record_variant(
                        facts_proof.order_facts_updated().distinct_facts_updated(),
                    );

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

                    Ok(facts_proof
                        .order_facts_updated()
                        .distinct_facts_updated()
                        .with_output(group_stream))
                })?;

        let outputs = streams.build();

        Ok(Group {
            name: name.clone(),
            inputs,
            outputs,
            group_field: valid_group_field,
            group_stream,
            fields: valid_fields,
        })
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
                .map(ValidFieldName::ident)
                .collect::<Vec<_>>();
            quote!(#(#names),*)
        };

        let group_field = self.group_field.ident();
        let mut_group_field = self.group_field.mut_ident();

        let record_definition = &graph.record_definitions()[self.inputs.single().record_type()];
        let variant = &record_definition[self.inputs.single().variant_id()];
        let eq = {
            let fields = variant
                .data()
                .filter_map(|d| {
                    let datum = &record_definition[d];
                    if !self.fields.iter().any(|field| field.name() == datum.name())
                        && datum.name() != self.group_field.name()
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

        let rec_ident = self.identifier_for("rec");
        let record_ident = self.identifier_for("record");
        let group_ident = self.identifier_for("group");
        let group_record_ident = self.identifier_for("group_record");
        let inline_body = quote! {
            quirky_binder_support::iterator::group::Group::new(
                input,
                |#rec_ident| {
                    let #record_and_unpacked_out { record: mut #record_ident, #fields } =
                        #record_and_unpacked_out::from((
                            #rec_ident,
                            #unpacked_record_in { #group_field: Vec::new() },
                        ));
                    let #group_record_ident = #group_record::new(#group_unpacked_record { #fields });
                    #record_ident.#mut_group_field().push(#group_record_ident);
                    #record_ident
                },
                #eq,
                |#group_ident, #rec_ident| {
                    let #input_unpacked_record{ #fields, .. } = #rec_ident.unpack();
                    let #group_record_ident = #group_record::new(#group_unpacked_record { #fields });
                    #group_ident.#mut_group_field().push(#group_record_ident);
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
}

pub fn group<R: TypeResolver + Copy>(
    graph: &mut GraphBuilder<R>,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    params: GroupParams,
    trace: Trace,
) -> ChainResult<Group> {
    Group::new(graph, name, inputs, params, trace)
}

const SUB_GROUP_TRACE_NAME: &str = "sub_group";

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
    group_field: ValidFieldName,
    group_stream: NodeSubStream,
    fields: Vec<ValidFieldName>,
}

impl SubGroup {
    pub fn new<R: TypeResolver + Copy>(
        graph: &mut GraphBuilder<R>,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        params: SubGroupParams,
        trace: Trace,
    ) -> ChainResult<SubGroup> {
        let (valid_path_fields, path_def) =
            params
                .path_fields
                .validate_path_on_stream(inputs.single(), graph, || {
                    trace_filter!(trace, SUB_GROUP_TRACE_NAME)
                })?;
        let valid_group_field = ValidFieldName::try_from(params.group_field).map_err(|_| {
            ChainError::InvalidFieldName {
                name: params.group_field.to_owned(),
                trace: trace_filter!(trace, GROUP_TRACE_NAME),
            }
        })?;
        let valid_fields = params.fields.validate_on_record_definition(&path_def, || {
            trace_filter!(trace, SUB_GROUP_TRACE_NAME)
        })?;
        drop(path_def);

        let mut streams = StreamsBuilder::new(&name, &inputs);
        streams.new_named_stream("group", graph);

        let mut created_group_stream = None;

        let path_streams = streams.output_from_input(0, true, graph).update_path(
            graph,
            &valid_path_fields,
            |output_stream, sub_output_stream, facts_proof| {
                let mut group_stream = output_stream.new_named_sub_stream("group", graph);
                let variant_id = sub_output_stream.input_variant_id();

                let (group_by_datum_ids, group_datum_ids) = {
                    let mut output_stream_def = sub_output_stream.record_definition().borrow_mut();
                    let mut group_stream_def = group_stream.record_definition().borrow_mut();

                    let variant = &output_stream_def[variant_id];
                    let mut group_by_datum_ids =
                        Vec::with_capacity(variant.data_len() - valid_fields.len());
                    let mut group_data = Vec::with_capacity(valid_fields.len());
                    let mut group_datum_ids = Vec::with_capacity(valid_fields.len());
                    for datum_id in variant.data() {
                        let datum = &output_stream_def[datum_id];
                        if valid_fields
                            .iter()
                            .any(|field| field.name() == datum.name())
                        {
                            group_data.push(datum);
                            group_datum_ids.push(datum.id());
                        } else {
                            group_by_datum_ids.push(datum.id());
                        }
                    }

                    check_undirected_order_starts_with(
                        &group_by_datum_ids,
                        sub_output_stream.facts().order(),
                        &output_stream_def,
                        "main sub stream",
                        || trace_filter!(trace, SUB_GROUP_TRACE_NAME),
                    )?;

                    let mut map = BTreeMap::<DatumId, DatumId>::new();
                    for datum in &group_data {
                        let new_id = group_stream_def.copy_datum(datum);
                        map.insert(datum.id(), new_id);
                    }
                    for datum_id in &group_datum_ids {
                        output_stream_def.remove_datum(*datum_id);
                    }

                    let group_order = sub_output_stream.facts().order()[group_by_datum_ids.len()..]
                        .iter()
                        .map(|d| d.map(|d| map[&d]))
                        .collect::<Vec<_>>();
                    group_stream.facts_mut().set_order(group_order);
                    let group_distinct = sub_output_stream
                        .facts()
                        .distinct()
                        .iter()
                        .filter_map(|d| map.get(d).copied())
                        .collect::<Vec<_>>();
                    group_stream.facts_mut().set_distinct(group_distinct);

                    (group_by_datum_ids, group_datum_ids)
                };

                let group_stream = group_stream.close_record_variant(
                    facts_proof.order_facts_updated().distinct_facts_updated(),
                );

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

                sub_output_stream.break_order_fact_at_ids(group_datum_ids.iter().cloned());
                sub_output_stream.set_distinct_fact_ids(group_by_datum_ids);

                Ok(facts_proof.order_facts_updated().distinct_facts_updated())
            },
        )?;

        let outputs = streams.build();

        Ok(SubGroup {
            name: name.clone(),
            inputs,
            outputs,
            path_streams,
            group_field: valid_group_field,
            group_stream: created_group_stream.expect("group stream"),
            fields: valid_fields,
        })
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
        let def_group = chain.sub_stream_definition_fragments(&self.group_stream);

        let group_record = def_group.record();
        let group_unpacked_record = def_group.unpacked_record();

        let fields = {
            let names = self
                .fields
                .iter()
                .map(ValidFieldName::ident)
                .collect::<Vec<_>>();
            quote!(#(#names),*)
        };

        let group_field = self.group_field.ident();
        let mut_group_field = self.group_field.mut_ident();

        let (eq_preamble, update_body) = {
            let path_stream = self.path_streams.last().expect("last path field");

            let leaf_record_definition =
                &graph.record_definitions()[path_stream.sub_input_stream.record_type()];
            let variant = &leaf_record_definition[path_stream.sub_input_stream.variant_id()];

            let out_record_definition =
                chain.sub_stream_definition_fragments(&path_stream.sub_output_stream);

            let eq = fields_eq(
                &out_record_definition.record(),
                variant.data().filter_map(|d| {
                    let datum = &leaf_record_definition[d];
                    if !self.fields.iter().any(|f| f.name() == datum.name())
                        && datum.name() != self.group_field.name()
                    {
                        Some(datum.name())
                    } else {
                        None
                    }
                }),
            );
            let eq_ident = self.identifier_for("eq");
            let eq_preamble = quote! { let #eq_ident = #eq; };

            let unpacked_record_in = out_record_definition.unpacked_record_in();
            let record_and_unpacked_out = out_record_definition.record_and_unpacked_out();

            let record_ident = self.identifier_for("record");
            let prev_record_ident = self.identifier_for("prev_record");
            let group_record_ident = self.identifier_for("group_record");
            let update_body = quote! {
                |#record_ident, #prev_record_ident| {
                    let #record_and_unpacked_out {
                        record: mut #record_ident,
                        #fields
                    } = #record_and_unpacked_out::from((
                        #record_ident, #unpacked_record_in { #group_field: Vec::new() },
                    ));
                    if let Some(#prev_record_ident) = #prev_record_ident {
                        if (#eq_ident)(&#record_ident, #prev_record_ident) {
                            // TODO optimize
                            let #group_record_ident = #group_record::new(
                                #group_unpacked_record { #fields }
                            );
                            #prev_record_ident.#mut_group_field().push(#group_record_ident);
                            return VecElementConversionResult::Abandonned;
                        }
                    }
                    let #group_record_ident = #group_record::new(
                        #group_unpacked_record { #fields }
                    );
                    #record_ident.#mut_group_field().push(#group_record_ident);
                    VecElementConversionResult::Converted(#record_ident)
                }
            };

            (eq_preamble, update_body)
        };

        chain.implement_path_update(
            self,
            self.inputs.single(),
            self.outputs.single(),
            &self.path_streams,
            Some(&eq_preamble),
            &update_body,
        );
    }
}

pub fn sub_group<R: TypeResolver + Copy>(
    graph: &mut GraphBuilder<R>,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    params: SubGroupParams,
    trace: Trace,
) -> ChainResult<SubGroup> {
    SubGroup::new(graph, name, inputs, params, trace)
}
