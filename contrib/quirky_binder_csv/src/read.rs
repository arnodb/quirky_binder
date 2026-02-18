use quirky_binder::{prelude::*, trace_element};
use serde::Deserialize;

const READ_CSV_TRACE_NAME: &str = "read_csv";

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct ReadCsvParams<'a> {
    input_file: &'a str,
    fields: TypedFieldsParam<'a>,
    order_fields: Option<DirectedFieldsParam<'a>>,
    distinct_fields: Option<FieldsParam<'a>>,
    #[serde(default)]
    has_headers: bool,
}

#[derive(Debug, Getters)]
pub struct ReadCsv {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 0],
    #[getset(get = "pub")]
    outputs: [NodeStream; 1],
    input_file: String,
    fields: Vec<(ValidFieldName, ValidFieldType)>,
    has_headers: bool,
}

impl ReadCsv {
    fn new(
        graph: &mut GraphBuilder,
        name: FullyQualifiedName,
        inputs: [NodeStream; 0],
        params: ReadCsvParams,
    ) -> ChainResultWithTrace<Self> {
        let valid_fields = params
            .fields
            .validate_new()
            .with_trace_element(trace_element!())?;

        let valid_order_fields = params
            .order_fields
            .map(|order_fields| {
                order_fields
                    .validate(|name| valid_fields.iter().any(|vf| vf.0.name() == name))
                    .with_trace_element(trace_element!())
            })
            .transpose()?;

        let valid_distinct_fields = params
            .distinct_fields
            .map(|distinct_fields| {
                distinct_fields
                    .validate(|name| valid_fields.iter().any(|vf| vf.0.name() == name.name()))
            })
            .transpose()?;

        let mut streams = StreamsBuilder::new(&name, &inputs);
        streams
            .new_main_stream(graph)
            .with_trace_element(trace_element!())?;

        streams
            .new_main_output(graph)
            .with_trace_element(trace_element!())?
            .update(|output_stream, facts_proof| {
                {
                    let mut output_stream_def = output_stream.record_definition().borrow_mut();
                    for (name, r#type) in valid_fields.iter() {
                        output_stream_def
                            .add_datum(
                                name.name(),
                                QuirkyDatumType::Simple {
                                    type_name: r#type.type_name().to_owned(),
                                },
                            )
                            .map_err(|err| ChainError::Other { msg: err })
                            .with_trace_element(trace_element!())?;
                    }
                }
                if let Some(order_fields) = valid_order_fields.as_ref() {
                    output_stream
                        .set_order_fact(
                            order_fields
                                .iter()
                                .map(|field| field.as_ref().map(ValidFieldName::name)),
                        )
                        .with_trace_element(trace_element!())?;
                }
                if let Some(distinct_fields) = valid_distinct_fields.as_ref() {
                    output_stream
                        .set_distinct_fact(distinct_fields.iter().map(ValidFieldName::name))
                        .with_trace_element(trace_element!())?;
                }
                Ok(facts_proof.order_facts_updated().distinct_facts_updated())
            })?;

        let outputs = streams.build().with_trace_element(trace_element!())?;

        Ok(Self {
            name,
            inputs,
            outputs,
            input_file: params.input_file.to_owned(),
            fields: valid_fields,
            has_headers: params.has_headers,
        })
    }
}

impl DynNode for ReadCsv {
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
        let thread_id = chain.new_threaded_source(
            &self.name,
            ChainThreadType::Regular,
            &self.inputs,
            &self.outputs,
        );

        let output = chain.format_thread_output(
            thread_id,
            0,
            NodeStatisticsOption::WithStatistics {
                node_name: &self.name,
            },
        );

        let input_file = &self.input_file;

        let has_headers = self.has_headers;

        let error = graph.chain_customizer().error_macro.to_full_name();

        let read_headers =
            if has_headers {
                let header_checks = self.fields.iter().enumerate().map(
                    |(index, (field_name, _))| {
                        let field_name = field_name.name();
                        quote! {
                            let header = iter.next();
                            if let Some(header) = header {
                                if header != #field_name {
                                    return Err(#error!(
                                        "Header mismatch at position {}, expected {} but got {}",
                                        #index, #field_name, header
                                    ));
                                }
                            } else {
                                return Err(#error!(
                                    "Missing header at position {}, expected {}",
                                    #index, #field_name
                                ));
                            }
                        }
                    },
                );
                Some(quote! {{
                    let headers = reader.headers()?;
                    let mut iter = headers.into_iter();
                    #(#header_checks)*
                    if let Some(header) = iter.next() {
                        return Err(#error!("Unexpected extra header {}", header));
                    };
                }})
            } else {
                None
            };

        let thread_body = quote! {
            let mut output = #output;
            move || {
                use std::fs::File;
                use std::io::BufReader;
                use anyhow::Context;

                let file = File::open(#input_file)
                    .with_context(|| format!("Could not open {}", #input_file))?;

                let mut reader = csv::ReaderBuilder::new()
                    .has_headers(#has_headers)
                    .from_reader(BufReader::new(file));

                #read_headers

                for result in reader.deserialize() {
                    let record = result?;
                    output.send(Some(record))?;
                }
                output.send(None)?;

                Ok(())
            }
        };

        chain.implement_node_thread(self, thread_id, &thread_body);
    }
}

pub fn read_csv(
    graph: &mut GraphBuilder,
    name: FullyQualifiedName,
    inputs: [NodeStream; 0],
    params: ReadCsvParams,
) -> ChainResultWithTrace<ReadCsv> {
    let _trace_name = TraceName::push(READ_CSV_TRACE_NAME);
    ReadCsv::new(graph, name, inputs, params)
}
