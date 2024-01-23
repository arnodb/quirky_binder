use serde::Deserialize;
use truc::record::type_resolver::TypeResolver;

use crate::{
    graph::builder::{check_directed_order_starts_with, check_distinct_eq},
    prelude::*,
    support::cmp::fields_cmp_ab,
    trace_filter,
};

const JOIN_TRACE_NAME: &str = "join";

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct JoinParams<'a> {
    #[serde(borrow)]
    primary_fields: FieldsParam<'a>,
    secondary_fields: FieldsParam<'a>,
}

#[derive(Getters)]
pub struct Join {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 2],
    #[getset(get = "pub")]
    outputs: [NodeStream; 1],
    primary_fields: Vec<ValidFieldName>,
    secondary_fields: Vec<ValidFieldName>,
    joined_fields: Vec<String>,
}

impl Join {
    fn new<R: TypeResolver + Copy>(
        graph: &mut GraphBuilder<R>,
        name: FullyQualifiedName,
        inputs: [NodeStream; 2],
        params: JoinParams,
        trace: Trace,
    ) -> ChainResult<Self> {
        let mut streams = StreamsBuilder::new(&name, &inputs);

        let valid_primary_fields =
            params
                .primary_fields
                .validate_on_stream(&inputs[0], graph, || trace_filter!(trace, JOIN_TRACE_NAME))?;

        let valid_secondary_fields =
            params
                .secondary_fields
                .validate_on_stream(&inputs[1], graph, || trace_filter!(trace, JOIN_TRACE_NAME))?;

        let joined_fields =
            streams
                .output_from_input(0, true, graph)
                .update(|output_stream, facts_proof| {
                    for (stream_info, input, fields) in [
                        ("primary stream", &inputs[0], &valid_primary_fields),
                        ("secondary stream", &inputs[1], &valid_secondary_fields),
                    ] {
                        let input_stream_def = graph
                            .get_stream(input.record_type())
                            .expect("input_stream_def")
                            .borrow();

                        let variant = &input_stream_def[input.variant_id()];
                        let mut expected_fact_fields = Vec::with_capacity(fields.len());
                        for datum_id in variant.data() {
                            let datum = &input_stream_def[datum_id];
                            if fields.iter().any(|field| field.name() == datum.name()) {
                                expected_fact_fields.push(datum.id());
                            }
                        }

                        check_directed_order_starts_with(
                            &expected_fact_fields,
                            input.facts().order(),
                            &input_stream_def,
                            stream_info,
                            || trace_filter!(trace, JOIN_TRACE_NAME),
                        )?;
                        check_distinct_eq(
                            &expected_fact_fields,
                            input.facts().distinct(),
                            &input_stream_def,
                            stream_info,
                            || trace_filter!(trace, JOIN_TRACE_NAME),
                        )?;
                    }

                    let mut output_stream_def = output_stream.record_definition().borrow_mut();
                    let secondary_stream_def = graph
                        .get_stream(inputs[1].record_type())
                        .expect("secondary stream definition")
                        .borrow();
                    let variant = &secondary_stream_def[inputs[1].variant_id()];

                    let joined_fields = variant
                        .data()
                        .filter_map(|d| {
                            let datum = &secondary_stream_def[d];
                            if !valid_secondary_fields
                                .iter()
                                .any(|field| field.name() == datum.name())
                            {
                                output_stream_def.copy_datum(datum);
                                Some(datum.name().to_owned())
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<String>>();

                    // XXX That is actually not true, let's see what we can do later.
                    Ok(facts_proof
                        .order_facts_updated()
                        .distinct_facts_updated()
                        .with_output(joined_fields))
                })?;

        let outputs = streams.build();

        Ok(Self {
            name,
            inputs,
            outputs,
            primary_fields: valid_primary_fields,
            secondary_fields: valid_secondary_fields,
            joined_fields,
        })
    }
}

impl DynNode for Join {
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
        let thread_id = chain.pipe_inputs(&self.name, &self.inputs, &self.outputs);

        let primary_input_def = chain.stream_definition_fragments(&self.inputs[0]);
        let secondary_input_def = chain.stream_definition_fragments(&self.inputs[1]);
        let output_def = chain.stream_definition_fragments(self.outputs.single());

        let secondary_unpacked_record = secondary_input_def.unpacked_record();

        let cmp = fields_cmp_ab(
            &primary_input_def.record(),
            self.primary_fields.iter().map(ValidFieldName::name),
            &secondary_input_def.record(),
            self.secondary_fields.iter().map(ValidFieldName::name),
        );

        let has_joined_fields = !self.joined_fields.is_empty();
        let build_output = if has_joined_fields {
            let record_and_unpacked_out = output_def.record_and_unpacked_out();
            let unpacked_record_in = output_def.unpacked_record_in();

            let (joined_fields, joined_fields_defaults) = {
                let names = self
                    .joined_fields
                    .iter()
                    .map(|name| format_ident!("{}", name))
                    .collect::<Vec<_>>();
                (
                    quote!(#(#names,)*),
                    // Left join.
                    // TODO: support inner join.
                    quote!(#(#names: Default::default()),*),
                )
            };

            quote! {
                if equal {
                    let secondary_record = secondary_record.take().expect("secondary_record");
                    let #secondary_unpacked_record{ #joined_fields .. } = secondary_record.unpack();
                    let #record_and_unpacked_out { record } = #record_and_unpacked_out::from((primary_record, #unpacked_record_in { #joined_fields }));
                    tx.send(Some(record))?;
                } else {
                    let #record_and_unpacked_out { record } = #record_and_unpacked_out::from((primary_record, #unpacked_record_in { #joined_fields_defaults }));
                    tx.send(Some(record))?;
                }
            }
        } else {
            quote! {
                if equal {
                    secondary_record.take().expect("secondary_record");
                }
                tx.send(Some(primary_record))?;
            }
        };

        let thread_body = quote! {
            move || {
                use std::cmp::Ordering;

                let primary_rx = thread_control.input_0.take().expect("primary input");
                let secondary_rx = thread_control.input_1.take().expect("secondary input");
                let tx = thread_control.output_0.take().expect("output");

                let cmp = #cmp;

                let mut secondary_finished = false;
                let mut secondary_record = None;

                while let Some(primary_record) = primary_rx.recv()? {
                    let equal = loop {
                        if secondary_finished {
                            break false;
                        }
                        if let Some(secondary_record) = secondary_record.as_ref() {
                            match cmp(&primary_record, secondary_record) {
                                Ordering::Greater => {
                                    // TODO log
                                }
                                Ordering::Equal => {
                                    break true;
                                }
                                Ordering::Less => {
                                    break false;
                                }
                            }
                        }
                        secondary_record = secondary_rx.recv()?;
                        if secondary_record.is_none() {
                            secondary_finished = true;
                        }
                    };
                    #build_output
                }

                if !secondary_finished {
                    while secondary_rx.recv()?.is_some() {}
                    // TODO log
                }

                tx.send(None)?;
                Ok(())
            }
        };

        chain.implement_node_thread(self, thread_id, &thread_body);
    }

    fn all_nodes(&self) -> Box<dyn Iterator<Item = &dyn DynNode> + '_> {
        Box::new(Some(self as &dyn DynNode).into_iter())
    }
}

pub fn join<R: TypeResolver + Copy>(
    graph: &mut GraphBuilder<R>,
    name: FullyQualifiedName,
    inputs: [NodeStream; 2],
    params: JoinParams,
    trace: Trace,
) -> ChainResult<Join> {
    Join::new(graph, name, inputs, params, trace)
}
