use serde::Deserialize;

use crate::{prelude::*, trace_element};

const UNGROUP_TRACE_NAME: &str = "ungroup";

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
#[serde(bound = "'de: 'a")]
pub struct UngroupParams<'a> {
    group_field: FieldParam<'a>,
}

#[derive(Debug, Getters)]
pub struct Ungroup {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 1],
    #[getset(get = "pub")]
    outputs: [NodeStream; 1],
    group_field: ValidFieldName,
    group_stream: NodeSubStream,
    grouped_fields: Vec<String>,
}

impl Ungroup {
    pub fn new(
        graph: &mut GraphBuilder,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        params: UngroupParams,
    ) -> ChainResultWithTrace<Ungroup> {
        let valid_group_field = params
            .group_field
            .validate_on_stream(inputs.single(), graph)
            .with_trace_element(trace_element!(UNGROUP_TRACE_NAME))?;

        let mut streams = StreamsBuilder::new(&name, &inputs);
        streams
            .new_named_stream("group", graph)
            .with_trace_element(trace_element!(UNGROUP_TRACE_NAME))?;

        let (group_stream, grouped_fields) = streams
            .output_from_input(0, true, graph)
            .with_trace_element(trace_element!(UNGROUP_TRACE_NAME))?
            .update(|output_stream, facts_proof| {
                let (group_stream, grouped_fields) = {
                    let mut output_stream_def = output_stream.record_definition().borrow_mut();

                    let group_datum_id = output_stream_def
                        .get_current_datum_definition_by_name(valid_group_field.name())
                        .ok_or_else(|| ChainError::FieldNotFound {
                            field: valid_group_field.name().to_owned(),
                        })
                        .with_trace_element(trace_element!(UNGROUP_TRACE_NAME))?
                        .id();

                    let group_stream = output_stream.sub_stream(group_datum_id).clone();

                    output_stream_def
                        .remove_datum(group_datum_id)
                        .map_err(|err| ChainError::Other { msg: err })
                        .with_trace_element(trace_element!(UNGROUP_TRACE_NAME))?;

                    let sub_stream_def = graph
                        .get_stream(group_stream.record_type())
                        .with_trace_element(trace_element!(UNGROUP_TRACE_NAME))?
                        .borrow();
                    let grouped_fields = sub_stream_def
                        .get_current_data()
                        .map(|d| {
                            let datum = &sub_stream_def[d];
                            output_stream_def
                                .add_datum(datum.name(), datum.details().clone())
                                .map_err(|err| ChainError::Other { msg: err })
                                .with_trace_element(trace_element!(UNGROUP_TRACE_NAME))?;
                            Ok(datum.name().to_owned())
                        })
                        .collect::<Result<Vec<_>, _>>()?;

                    (group_stream, grouped_fields)
                };

                // Reset facts
                output_stream
                    .set_order_fact::<_, &str>([])
                    .with_trace_element(trace_element!(UNGROUP_TRACE_NAME))?;
                output_stream
                    .set_distinct_fact::<_, &str>([])
                    .with_trace_element(trace_element!(UNGROUP_TRACE_NAME))?;

                Ok(facts_proof
                    .order_facts_updated()
                    .distinct_facts_updated()
                    .with_output((group_stream, grouped_fields)))
            })?;

        let outputs = streams
            .build()
            .with_trace_element(trace_element!(UNGROUP_TRACE_NAME))?;

        Ok(Ungroup {
            name: name.clone(),
            inputs,
            outputs,
            group_field: valid_group_field,
            group_stream,
            grouped_fields,
        })
    }
}

impl DynNode for Ungroup {
    fn name(&self) -> &FullyQualifiedName {
        &self.name
    }

    fn inputs(&self) -> &[NodeStream] {
        &self.inputs
    }

    fn outputs(&self) -> &[NodeStream] {
        &self.outputs
    }

    fn gen_chain(&self, _graph: &Graph, chain: &mut Chain) {
        let def_group = chain.sub_stream_definition_fragments(&self.group_stream);
        let group_unpacked_record = def_group.unpacked_record();

        let group_field_mut = format_ident!("{}_mut", self.group_field.name());

        let grouped_fields = {
            let names = self
                .grouped_fields
                .iter()
                .map(|name| format_ident!("{}", name))
                .collect::<Vec<_>>();
            quote!(#(#names),*)
        };

        let record_ident = self.identifier_for("record");

        let inline_body = quote! {
            use fallible_iterator::IteratorExt;

            input.flat_map(|mut #record_ident| {
                let group = std::mem::take(#record_ident.#group_field_mut());
                Ok(group.into_iter().map(move |item| {
                    let #group_unpacked_record { #grouped_fields } = item.unpack();
                    Ok((#record_ident.clone(), UnpackedOutputIn0 { #grouped_fields }).into())
                }).transpose_into_fallible())
            })
        };

        chain.implement_inline_node(
            self,
            self.inputs.single(),
            self.outputs.single(),
            &inline_body,
        );
    }
}

pub fn ungroup(
    graph: &mut GraphBuilder,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    params: UngroupParams,
) -> ChainResultWithTrace<Ungroup> {
    Ungroup::new(graph, name, inputs, params)
}

const SUB_UNGROUP_TRACE_NAME: &str = "sub_ungroup";

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
#[serde(bound = "'de: 'a")]
pub struct SubUngroupParams<'a> {
    path_fields: FieldsParam<'a>,
    group_field: FieldParam<'a>,
}

#[derive(Debug, Getters)]
pub struct SubUngroup {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 1],
    #[getset(get = "pub")]
    outputs: [NodeStream; 1],
    path_streams: Vec<PathUpdateElement>,
    group_field: ValidFieldName,
    group_stream: NodeSubStream,
    grouped_fields: Vec<String>,
}

impl SubUngroup {
    pub fn new(
        graph: &mut GraphBuilder,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        params: SubUngroupParams,
    ) -> ChainResultWithTrace<SubUngroup> {
        let (valid_path_fields, path_def) = params
            .path_fields
            .validate_path_on_stream(inputs.single(), graph)
            .with_trace_element(trace_element!(SUB_UNGROUP_TRACE_NAME))?;
        let valid_group_field = params
            .group_field
            .validate_on_record_definition(&path_def)
            .with_trace_element(trace_element!(SUB_UNGROUP_TRACE_NAME))?;
        drop(path_def);

        let mut streams = StreamsBuilder::new(&name, &inputs);
        streams
            .new_named_stream("group", graph)
            .with_trace_element(trace_element!(SUB_UNGROUP_TRACE_NAME))?;

        let mut group_stream_and_grouped_fields = None;

        let path_streams = streams
            .output_from_input(0, true, graph)
            .with_trace_element(trace_element!(SUB_UNGROUP_TRACE_NAME))?
            .update_path(
                graph,
                &valid_path_fields,
                |_output_stream, sub_output_stream, facts_proof| {
                    let (group_stream, grouped_fields) = {
                        let mut output_stream_def =
                            sub_output_stream.record_definition().borrow_mut();

                        let group_datum_id = output_stream_def
                            .get_current_datum_definition_by_name(valid_group_field.name())
                            .ok_or_else(|| ChainError::FieldNotFound {
                                field: valid_group_field.name().to_owned(),
                            })
                            .with_trace_element(trace_element!(SUB_UNGROUP_TRACE_NAME))?
                            .id();

                        let group_stream = sub_output_stream.sub_stream(group_datum_id).clone();

                        output_stream_def
                            .remove_datum(group_datum_id)
                            .map_err(|err| ChainError::Other { msg: err })
                            .with_trace_element(trace_element!(SUB_UNGROUP_TRACE_NAME))?;

                        let sub_stream_def = graph
                            .get_stream(group_stream.record_type())
                            .with_trace_element(trace_element!(SUB_UNGROUP_TRACE_NAME))?
                            .borrow();
                        let grouped_fields = sub_stream_def
                            .get_current_data()
                            .map(|d| {
                                let datum = &sub_stream_def[d];
                                output_stream_def
                                    .add_datum(datum.name(), datum.details().clone())
                                    .map_err(|err| ChainError::Other { msg: err })
                                    .with_trace_element(trace_element!(SUB_UNGROUP_TRACE_NAME))?;
                                Ok(datum.name().to_owned())
                            })
                            .collect::<Result<Vec<_>, _>>()?;

                        (group_stream, grouped_fields)
                    };

                    // Reset facts
                    sub_output_stream
                        .set_order_fact::<_, &str>([])
                        .with_trace_element(trace_element!(SUB_UNGROUP_TRACE_NAME))?;
                    sub_output_stream
                        .set_distinct_fact::<_, &str>([])
                        .with_trace_element(trace_element!(SUB_UNGROUP_TRACE_NAME))?;

                    group_stream_and_grouped_fields = Some((group_stream, grouped_fields));

                    Ok(facts_proof.order_facts_updated().distinct_facts_updated())
                },
                SUB_UNGROUP_TRACE_NAME,
            )?;

        let (group_stream, grouped_fields) =
            group_stream_and_grouped_fields.expect("group_stream_and_grouped_fields");

        let outputs = streams
            .build()
            .with_trace_element(trace_element!(SUB_UNGROUP_TRACE_NAME))?;

        Ok(SubUngroup {
            name: name.clone(),
            inputs,
            outputs,
            path_streams,
            group_field: valid_group_field,
            group_stream,
            grouped_fields,
        })
    }
}

impl DynNode for SubUngroup {
    fn name(&self) -> &FullyQualifiedName {
        &self.name
    }

    fn inputs(&self) -> &[NodeStream] {
        &self.inputs
    }

    fn outputs(&self) -> &[NodeStream] {
        &self.outputs
    }

    fn gen_chain(&self, _graph: &Graph, chain: &mut Chain) {
        let def_group = chain.sub_stream_definition_fragments(&self.group_stream);

        let path_stream = self.path_streams.last().expect("last path field");

        let out_record_definition =
            chain.sub_stream_definition_fragments(&path_stream.sub_output_stream);

        let group_unpacked_record = def_group.unpacked_record();
        let unpacked_record_in = out_record_definition.unpacked_record_in();

        let group_field_mut = format_ident!("{}_mut", self.group_field.name());

        let grouped_fields = {
            let names = self
                .grouped_fields
                .iter()
                .map(|name| format_ident!("{}", name))
                .collect::<Vec<_>>();
            quote!(#(#names),*)
        };

        let record_ident = self.identifier_for("record");
        let build_leaf_body = |_input_record, _record, access, mut_access| {
            quote! {
                let converted = #access
                    .into_iter()
                    .flat_map(|mut #record_ident| {
                        let group = std::mem::take(#record_ident.#group_field_mut());
                        group.into_iter().map(move |item| {
                            let #group_unpacked_record { #grouped_fields } = item.unpack();
                            (#record_ident.clone(), #unpacked_record_in { #grouped_fields }).into()
                        })
                    })
                    .collect::<Vec<_>>();
                *record.#mut_access() = converted;
            }
        };

        chain.implement_path_update(
            self,
            self.inputs.single(),
            self.outputs.single(),
            &self.path_streams,
            None,
            build_leaf_body,
        );
    }
}

pub fn sub_ungroup(
    graph: &mut GraphBuilder,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    params: SubUngroupParams,
) -> ChainResultWithTrace<SubUngroup> {
    SubUngroup::new(graph, name, inputs, params)
}
