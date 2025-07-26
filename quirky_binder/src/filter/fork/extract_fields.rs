use serde::Deserialize;

use crate::{prelude::*, trace_element};

const EXTRACT_FIELDS_TRACE_NAME: &str = "extract_fields";

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct ExtractFieldsParams<'a> {
    #[serde(borrow)]
    fields: FieldsParam<'a>,
}

#[derive(Getters)]
pub struct ExtractFields {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 1],
    #[getset(get = "pub")]
    outputs: [NodeStream; 2],
}

impl ExtractFields {
    fn new(
        graph: &mut GraphBuilder,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        params: ExtractFieldsParams,
    ) -> ChainResultWithTrace<Self> {
        let valid_fields =
            params
                .fields
                .validate_on_stream(inputs.single(), graph, EXTRACT_FIELDS_TRACE_NAME)?;

        let mut streams = StreamsBuilder::new(&name, &inputs);
        streams
            .new_named_stream("extracted", graph)
            .with_trace_element(trace_element!(EXTRACT_FIELDS_TRACE_NAME))?;

        let output_stream_def = streams
            .output_from_input(0, true, graph)
            .with_trace_element(trace_element!(EXTRACT_FIELDS_TRACE_NAME))?
            .pass_through(|output_stream, facts_proof| {
                let record_definition = output_stream.record_definition();
                Ok(facts_proof
                    .order_facts_updated()
                    .distinct_facts_updated()
                    .with_output(record_definition))
            })?
            .borrow();

        streams
            .new_named_output("extracted", graph)
            .with_trace_element(trace_element!(EXTRACT_FIELDS_TRACE_NAME))?
            .update(|output_extracted_stream, facts_proof| {
                let mut output_extracted_stream_def =
                    output_extracted_stream.record_definition().borrow_mut();
                for field in valid_fields.iter() {
                    let datum = output_stream_def
                        .get_variant_datum_definition_by_name(
                            inputs.single().variant_id(),
                            field.name(),
                        )
                        .unwrap_or_else(|| panic!(r#"datum "{}""#, field.name()));
                    output_extracted_stream_def
                        .add_datum(datum.name(), datum.details().clone())
                        .map_err(|err| ChainError::Other { msg: err })
                        .with_trace_element(trace_element!(EXTRACT_FIELDS_TRACE_NAME))?;
                }
                // XXX That is actually not true, let's see what we can do later.
                Ok(facts_proof.order_facts_updated().distinct_facts_updated())
            })?;

        let outputs = streams
            .build()
            .with_trace_element(trace_element!(EXTRACT_FIELDS_TRACE_NAME))?;

        Ok(Self {
            name,
            inputs,
            outputs,
        })
    }
}

impl DynNode for ExtractFields {
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
        let thread_id = chain.pipe_inputs(&self.name, &self.inputs, &self.outputs);

        let input_0 = chain.format_thread_input(
            thread_id,
            0,
            NodeStatisticsOption::WithStatistics {
                node_name: &self.name,
            },
        );

        let def_output_1 = chain.stream_definition_fragments(&self.outputs[1]);

        let record_definition = &graph.record_definitions()[self.outputs[1].record_type()];
        let variant = &record_definition[self.outputs[1].variant_id()];
        let datum_clones = variant.data().map(|d| {
            let name = format_ident!("{}", record_definition[d].name());
            quote! {
                #[allow(clippy::clone_on_copy)]
                let #name = record.#name().clone();
            }
        });

        let output_record_1 = def_output_1.record();
        let output_unpacked_record_1 = def_output_1.unpacked_record();

        let fields = variant.data().map(|d| {
            let datum = &record_definition[d];
            format_ident!("{}", datum.name())
        });

        let thread_body = quote! {
            let thread_status = thread_status.clone();
            move || {
                let mut input_0 = #input_0;
                let output_0 = thread_control.output_0.take().expect("output 0");
                let output_1 = thread_control.output_1.take().expect("output 1");
                while let Some(record) = input_0.next()? {
                    #(#datum_clones)*
                    let record_1 = #output_record_1::new(
                        #output_unpacked_record_1 { #(#fields),* }
                    );
                    output_0.send(Some(record))?;
                    output_1.send(Some(record_1))?;
                }
                output_0.send(None)?;
                output_1.send(None)?;
                Ok(())
            }
        };

        chain.implement_node_thread(self, thread_id, &thread_body);
    }
}

pub fn extract_fields(
    graph: &mut GraphBuilder,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    params: ExtractFieldsParams,
) -> ChainResultWithTrace<ExtractFields> {
    ExtractFields::new(graph, name, inputs, params)
}
