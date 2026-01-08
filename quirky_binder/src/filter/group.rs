use std::collections::BTreeMap;

use serde::Deserialize;
use truc::record::definition::DatumId;

use crate::{
    graph::builder::check_undirected_order_starts_with,
    prelude::*,
    support::eq::{fields_eq, fields_eq_ab},
    trace_element,
};

const GROUP_TRACE_NAME: &str = "group";

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct GroupParams<'a> {
    by_fields: FieldsParam<'a>,
    group_field: &'a str,
}

#[derive(Getters)]
pub struct Group {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 1],
    #[getset(get = "pub")]
    outputs: [NodeStream; 1],
    by_fields: Vec<ValidFieldName>,
    group_field: ValidFieldName,
    group_stream: NodeSubStream,
}

impl Group {
    pub fn new(
        graph: &mut GraphBuilder,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        params: GroupParams,
    ) -> ChainResultWithTrace<Group> {
        let valid_by_fields =
            params
                .by_fields
                .validate_on_stream(inputs.single(), graph, GROUP_TRACE_NAME)?;
        let valid_group_field = ValidFieldName::try_from(params.group_field)
            .map_err(|_| ChainError::InvalidFieldName {
                name: params.group_field.to_owned(),
            })
            .with_trace_element(trace_element!(GROUP_TRACE_NAME))?;

        let mut streams = StreamsBuilder::new(&name, &inputs);
        streams
            .new_named_stream("group", graph)
            .with_trace_element(trace_element!(GROUP_TRACE_NAME))?;

        let group_stream = streams
            .output_from_input(0, true, graph)
            .with_trace_element(trace_element!(GROUP_TRACE_NAME))?
            .update(|output_stream, facts_proof| {
                let mut group_stream = output_stream
                    .new_named_sub_stream("group", graph)
                    .with_trace_element(trace_element!(GROUP_TRACE_NAME))?;
                let variant_id = output_stream.input_variant_id();

                let (group_by_datum_ids, group_datum_ids) = {
                    let mut output_stream_def = output_stream.record_definition().borrow_mut();
                    let mut group_stream_def = group_stream.record_definition().borrow_mut();

                    let variant = &output_stream_def[variant_id];
                    let mut group_by_datum_ids = Vec::with_capacity(valid_by_fields.len());
                    let mut group_data =
                        Vec::with_capacity(variant.data_len() - valid_by_fields.len());
                    let mut group_datum_ids =
                        Vec::with_capacity(variant.data_len() - valid_by_fields.len());
                    for datum_id in variant.data() {
                        let datum = &output_stream_def[datum_id];
                        if !valid_by_fields
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
                        &output_stream_def,
                        "main stream",
                    )
                    .with_trace_element(trace_element!(GROUP_TRACE_NAME))?;

                    let mut map = BTreeMap::<DatumId, DatumId>::new();
                    for datum in &group_data {
                        let new_id = group_stream_def
                            .add_datum(datum.name(), datum.details().clone())
                            .map_err(|err| ChainError::Other { msg: err })
                            .with_trace_element(trace_element!(GROUP_TRACE_NAME))?;
                        map.insert(datum.id(), new_id);
                    }
                    for &datum_id in &group_datum_ids {
                        output_stream_def
                            .remove_datum(datum_id)
                            .map_err(|err| ChainError::Other { msg: err })
                            .with_trace_element(trace_element!(GROUP_TRACE_NAME))?;
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

                output_stream
                    .add_vec_datum(
                        params.group_field,
                        group_stream.record_type().clone(),
                        group_stream.variant_id(),
                        group_stream.clone(),
                    )
                    .with_trace_element(trace_element!(GROUP_TRACE_NAME))?;

                output_stream.break_order_fact_at_ids(group_datum_ids.iter().cloned());
                output_stream.set_distinct_fact_ids(group_by_datum_ids);

                Ok(facts_proof
                    .order_facts_updated()
                    .distinct_facts_updated()
                    .with_output(group_stream))
            })?;

        let outputs = streams
            .build()
            .with_trace_element(trace_element!(GROUP_TRACE_NAME))?;

        Ok(Group {
            name: name.clone(),
            inputs,
            outputs,
            by_fields: valid_by_fields,
            group_field: valid_group_field,
            group_stream,
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
        let def_group = chain.sub_stream_definition_fragments(&self.group_stream);

        let group_record = def_group.record();
        let group_unpacked_record = def_group.unpacked_record();

        let fields = {
            let record_definition = &graph.record_definitions()[self.inputs.single().record_type()];
            let variant = &record_definition[self.inputs.single().variant_id()];
            let idents = variant.data().filter_map(|d| {
                let datum = &record_definition[d];
                if !self
                    .by_fields
                    .iter()
                    .any(|field| field.name() == datum.name())
                {
                    Some(format_ident!("{}", datum.name()))
                } else {
                    None
                }
            });
            quote!(#(#idents,)*)
        };

        let group_field = self.group_field.ident();
        let mut_group_field = self.group_field.mut_ident();

        let eq = {
            let fields = self.by_fields.iter().map(ValidFieldName::name);
            fields_eq_ab(
                &syn::parse_str::<syn::Type>("Output0").unwrap(),
                fields.clone(),
                &syn::parse_str::<syn::Type>("Input0").unwrap(),
                fields,
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
                    let Output0AndUnpackedOut { record: mut #record_ident, #fields } =
                        Output0AndUnpackedOut::from((
                            #rec_ident,
                            UnpackedOutputIn0 { #group_field: Vec::new() },
                        ));
                    let #group_record_ident = #group_record::new(#group_unpacked_record { #fields });
                    #record_ident.#mut_group_field().push(#group_record_ident);
                    #record_ident
                },
                #eq,
                |#group_ident, #rec_ident| {
                    let UnpackedInput0 { #fields .. } = #rec_ident.unpack();
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

pub fn group(
    graph: &mut GraphBuilder,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    params: GroupParams,
) -> ChainResultWithTrace<Group> {
    Group::new(graph, name, inputs, params)
}

const SUB_GROUP_TRACE_NAME: &str = "sub_group";

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct SubGroupParams<'a> {
    path_fields: FieldsParam<'a>,
    by_fields: FieldsParam<'a>,
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
    by_fields: Vec<ValidFieldName>,
    group_field: ValidFieldName,
    group_stream: NodeSubStream,
}

impl SubGroup {
    pub fn new(
        graph: &mut GraphBuilder,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        params: SubGroupParams,
    ) -> ChainResultWithTrace<SubGroup> {
        let (valid_path_fields, path_def) = params
            .path_fields
            .validate_path_on_stream(inputs.single(), graph)
            .with_trace_element(trace_element!(SUB_GROUP_TRACE_NAME))?;
        let valid_group_field = ValidFieldName::try_from(params.group_field)
            .map_err(|_| ChainError::InvalidFieldName {
                name: params.group_field.to_owned(),
            })
            .with_trace_element(trace_element!(SUB_GROUP_TRACE_NAME))?;
        let valid_by_fields = params
            .by_fields
            .validate_on_record_definition(&path_def, SUB_GROUP_TRACE_NAME)?;
        drop(path_def);

        let mut streams = StreamsBuilder::new(&name, &inputs);
        streams
            .new_named_stream("group", graph)
            .with_trace_element(trace_element!(SUB_GROUP_TRACE_NAME))?;

        let mut created_group_stream = None;

        let path_streams = streams
            .output_from_input(0, true, graph)
            .with_trace_element(trace_element!(SUB_GROUP_TRACE_NAME))?
            .update_path(
                graph,
                &valid_path_fields,
                |output_stream, sub_output_stream, facts_proof| {
                    let mut group_stream = output_stream
                        .new_named_sub_stream("group", graph)
                        .with_trace_element(trace_element!(SUB_GROUP_TRACE_NAME))?;
                    let variant_id = sub_output_stream.input_variant_id();

                    let (group_by_datum_ids, group_datum_ids) = {
                        let mut output_stream_def =
                            sub_output_stream.record_definition().borrow_mut();
                        let mut group_stream_def = group_stream.record_definition().borrow_mut();

                        let variant = &output_stream_def[variant_id];
                        let mut group_by_datum_ids = Vec::with_capacity(valid_by_fields.len());
                        let mut group_data =
                            Vec::with_capacity(variant.data_len() - valid_by_fields.len());
                        let mut group_datum_ids =
                            Vec::with_capacity(variant.data_len() - valid_by_fields.len());
                        for datum_id in variant.data() {
                            let datum = &output_stream_def[datum_id];
                            if !valid_by_fields
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
                        )
                        .with_trace_element(trace_element!(SUB_GROUP_TRACE_NAME))?;

                        let mut map = BTreeMap::<DatumId, DatumId>::new();
                        for datum in &group_data {
                            let new_id = group_stream_def
                                .add_datum(datum.name(), datum.details().clone())
                                .map_err(|err| ChainError::Other { msg: err })
                                .with_trace_element(trace_element!(SUB_GROUP_TRACE_NAME))?;
                            map.insert(datum.id(), new_id);
                        }
                        for datum_id in &group_datum_ids {
                            output_stream_def
                                .remove_datum(*datum_id)
                                .map_err(|err| ChainError::Other { msg: err })
                                .with_trace_element(trace_element!(SUB_GROUP_TRACE_NAME))?;
                        }

                        let group_order = sub_output_stream.facts().order()
                            [group_by_datum_ids.len()..]
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

                    sub_output_stream
                        .add_vec_datum(
                            params.group_field,
                            group_stream.record_type().clone(),
                            group_stream.variant_id(),
                            group_stream.clone(),
                        )
                        .with_trace_element(trace_element!(SUB_GROUP_TRACE_NAME))?;

                    created_group_stream = Some(group_stream);

                    sub_output_stream.break_order_fact_at_ids(group_datum_ids.iter().cloned());
                    sub_output_stream.set_distinct_fact_ids(group_by_datum_ids);

                    Ok(facts_proof.order_facts_updated().distinct_facts_updated())
                },
                SUB_GROUP_TRACE_NAME,
            )?;

        let outputs = streams
            .build()
            .with_trace_element(trace_element!(SUB_GROUP_TRACE_NAME))?;

        Ok(SubGroup {
            name: name.clone(),
            inputs,
            outputs,
            path_streams,
            by_fields: valid_by_fields,
            group_field: valid_group_field,
            group_stream: created_group_stream.expect("group stream"),
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
        let error_type = graph.chain_customizer().error_type.to_full_name();

        let def_group = chain.sub_stream_definition_fragments(&self.group_stream);

        let group_record = def_group.record();
        let group_unpacked_record = def_group.unpacked_record();

        let group_field = self.group_field.ident();
        let mut_group_field = self.group_field.mut_ident();

        let (eq_preamble, update_body) = {
            let path_stream = self.path_streams.last().expect("last path field");

            let leaf_record_definition =
                &graph.record_definitions()[path_stream.sub_input_stream.record_type()];
            let variant = &leaf_record_definition[path_stream.sub_input_stream.variant_id()];

            let fields = {
                let idents = variant.data().filter_map(|d| {
                    let datum = &leaf_record_definition[d];
                    if !self.by_fields.iter().any(|f| f.name() == datum.name()) {
                        Some(format_ident!("{}", datum.name()))
                    } else {
                        None
                    }
                });
                quote!(#(#idents,)*)
            };

            let out_record_definition =
                chain.sub_stream_definition_fragments(&path_stream.sub_output_stream);

            let eq = fields_eq(
                &out_record_definition.record(),
                self.by_fields.iter().map(ValidFieldName::name),
            );
            let eq_ident = self.identifier_for("eq");
            let eq_preamble = quote! { let #eq_ident = #eq; };

            let unpacked_record_in = out_record_definition.unpacked_record_in();
            let record_and_unpacked_out = out_record_definition.record_and_unpacked_out();

            let record_ident = self.identifier_for("record");
            let prev_record_ident = self.identifier_for("prev_record");
            let group_record_ident = self.identifier_for("group_record");
            let update_body = quote! {
                |#record_ident, #prev_record_ident| -> Result<_, #error_type> {
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
                            return Ok(VecElementConversionResult::Abandonned);
                        }
                    }
                    let #group_record_ident = #group_record::new(
                        #group_unpacked_record { #fields }
                    );
                    #record_ident.#mut_group_field().push(#group_record_ident);
                    Ok(VecElementConversionResult::Converted(#record_ident))
                }
            };

            (eq_preamble, update_body)
        };

        let build_leaf_body = |input_record, record, access, mut_access| {
            quote! {
                // TODO optimize this code in truc
                let converted = try_convert_vec_in_place::<#input_record, #record, _, #error_type>(
                    #access,
                    #update_body,
                )?;
                *record.#mut_access() = converted;
            }
        };

        chain.implement_path_update(
            self,
            self.inputs.single(),
            self.outputs.single(),
            &self.path_streams,
            Some(&eq_preamble),
            build_leaf_body,
        );
    }
}

pub fn sub_group(
    graph: &mut GraphBuilder,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    params: SubGroupParams,
) -> ChainResultWithTrace<SubGroup> {
    SubGroup::new(graph, name, inputs, params)
}
