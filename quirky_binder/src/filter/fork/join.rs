use serde::Deserialize;

use crate::{
    graph::builder::{check_directed_order_starts_with, check_distinct_eq},
    prelude::*,
    support::cmp::fields_cmp_ab,
    trace_element,
};

const JOIN_TRACE_NAME: &str = "join";

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct JoinParams<'a> {
    #[serde(borrow)]
    primary_fields: FieldsParam<'a>,
    secondary_fields: FieldsParam<'a>,
}

#[derive(Debug, Getters)]
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
    fn new(
        graph: &mut GraphBuilder,
        name: FullyQualifiedName,
        inputs: [NodeStream; 2],
        params: JoinParams,
    ) -> ChainResultWithTrace<Self> {
        let mut streams = StreamsBuilder::new(&name, &inputs);

        let valid_primary_fields = params
            .primary_fields
            .validate_on_stream(&inputs[0], graph)?;

        let valid_secondary_fields = params
            .secondary_fields
            .validate_on_stream(&inputs[1], graph)?;

        let joined_fields = streams
            .output_from_input(0, true, graph)
            .with_trace_element(trace_element!())?
            .update(&mut streams, |output_stream, facts_proof| {
                for (stream_info, input, fields) in [
                    ("primary stream", &inputs[0], &valid_primary_fields),
                    ("secondary stream", &inputs[1], &valid_secondary_fields),
                ] {
                    let input_stream_def = graph
                        .get_stream(input.record_type())
                        .with_trace_element(trace_element!())?
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
                    )
                    .with_trace_element(trace_element!())?;
                    check_distinct_eq(
                        &expected_fact_fields,
                        input.facts().distinct(),
                        &input_stream_def,
                        stream_info,
                    )
                    .with_trace_element(trace_element!())?;
                }

                let mut output_stream_def = output_stream.record_definition().borrow_mut();
                let secondary_stream_def = graph
                    .get_stream(inputs[1].record_type())
                    .with_trace_element(trace_element!())?
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
                            if let Err(err) = output_stream_def
                                .add_datum(datum.name(), datum.details().clone())
                                .map_err(|err| ChainError::Other { msg: err })
                                .with_trace_element(trace_element!())
                            {
                                return Some(Err(err));
                            };
                            Some(Ok(datum.name().to_owned()))
                        } else {
                            None
                        }
                    })
                    .collect::<Result<Vec<String>, _>>()?;

                // XXX That is actually not true, let's see what we can do later.
                Ok(facts_proof
                    .order_facts_updated()
                    .distinct_facts_updated()
                    .with_output(joined_fields))
            })?;

        let outputs = streams.build().with_trace_element(trace_element!())?;

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
        let thread_id = chain.pipe_inputs_with_thread(&self.name, &self.inputs, &self.outputs);

        let primary_input = chain.format_thread_input(
            thread_id,
            0,
            NodeStatisticsOption::WithStatistics {
                node_name: &self.name,
            },
        );
        let secondary_input = chain.format_thread_input(
            thread_id,
            1,
            NodeStatisticsOption::WithStatistics {
                node_name: &self.name,
            },
        );
        let output = chain.format_thread_output(
            thread_id,
            0,
            NodeStatisticsOption::WithStatistics {
                node_name: &self.name,
            },
        );

        let cmp = fields_cmp_ab(
            &syn::parse_str::<syn::Type>("Input0").unwrap(),
            self.primary_fields.iter().map(ValidFieldName::name),
            &syn::parse_str::<syn::Type>("Input1").unwrap(),
            self.secondary_fields.iter().map(ValidFieldName::name),
        );

        let has_joined_fields = !self.joined_fields.is_empty();
        let build_output = if has_joined_fields {
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
                    let UnpackedInput1 { #joined_fields .. } = secondary_record.unpack();
                    let Output0AndUnpackedOut { record } = Output0AndUnpackedOut::from((primary_record, UnpackedOutputIn0 { #joined_fields }));
                    output.send(Some(record))?;
                } else {
                    let Output0AndUnpackedOut { record } = Output0AndUnpackedOut::from((primary_record, UnpackedOutputIn0 { #joined_fields_defaults }));
                    output.send(Some(record))?;
                }
            }
        } else {
            quote! {
                if equal {
                    secondary_record.take().expect("secondary_record");
                }
                output.send(Some(primary_record))?;
            }
        };

        let thread_body = quote! {
            let thread_status = thread_status.clone();
            move || {
                use std::cmp::Ordering;

                let mut primary_input = #primary_input;
                let mut secondary_input = #secondary_input;
                let mut output = #output;

                let cmp = #cmp;

                let mut secondary_finished = false;
                let mut secondary_record = None;

                while let Some(primary_record) = primary_input.next()? {
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
                        secondary_record = secondary_input.next()?;
                        if secondary_record.is_none() {
                            secondary_finished = true;
                        }
                    };
                    #build_output
                }

                if !secondary_finished {
                    while secondary_input.next()?.is_some() {}
                    // TODO log
                }

                output.send(None)?;
                Ok(())
            }
        };

        chain.implement_node_thread(self, thread_id, &thread_body);
    }
}

pub fn join(
    graph: &mut GraphBuilder,
    name: FullyQualifiedName,
    inputs: [NodeStream; 2],
    params: JoinParams,
) -> ChainResultWithTrace<Join> {
    let _trace_name = TraceName::push(JOIN_TRACE_NAME);
    Join::new(graph, name, inputs, params)
}
