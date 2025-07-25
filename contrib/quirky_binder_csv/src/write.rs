use quirky_binder::{prelude::*, trace_element};
use serde::Deserialize;

const WRITE_CSV_TRACE_NAME: &str = "write_csv";

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct WriteCsvParams<'a> {
    output_file: &'a str,
    #[serde(default)]
    has_headers: bool,
}

#[derive(Getters)]
pub struct WriteCsv {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 1],
    #[getset(get = "pub")]
    outputs: [NodeStream; 0],
    output_file: String,
    has_headers: bool,
}

impl WriteCsv {
    fn new(
        graph: &mut GraphBuilder,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        params: WriteCsvParams,
    ) -> ChainResultWithTrace<Self> {
        let input = inputs.single();
        let input_stream_def = graph
            .get_stream(input.record_type())
            .with_trace_element(trace_element!(WRITE_CSV_TRACE_NAME))?
            .borrow();
        for d in input_stream_def.get_current_data() {
            if input.sub_streams().contains_key(&d) {
                return Err(ChainError::Other {
                    msg: "Sub streams are not supported".to_owned(),
                })
                .with_trace_element(trace_element!(WRITE_CSV_TRACE_NAME));
            }
        }

        Ok(Self {
            name,
            inputs,
            outputs: [],
            output_file: params.output_file.to_owned(),
            has_headers: params.has_headers,
        })
    }
}

impl DynNode for WriteCsv {
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
        let (thread_id, inputs) = if self.inputs.len() == 1 {
            let thread =
                chain.get_thread_by_source(&self.inputs[0], &self.name, self.outputs.none());

            let input = chain.format_source_thread_input(
                &thread,
                self.inputs[0].source(),
                true,
                &self.name,
                true,
            );

            (thread.thread_id, vec![input])
        } else {
            let thread_id = chain.pipe_inputs(&self.name, &self.inputs, &self.outputs);

            let inputs = (0..self.inputs.len())
                .map(|input_index| {
                    let input_name = format_ident!("input_{}", input_index);
                    let input = chain.format_thread_input(thread_id, &self.name, input_index, true);
                    quote! { let #input_name = #input; }
                })
                .collect::<Vec<_>>();

            (thread_id, inputs)
        };

        let output_file = &self.output_file;

        let has_headers = self.has_headers;

        let write_headers = if has_headers {
            let record_definition = &graph.record_definitions()[self.inputs.single().record_type()];
            let variant = &record_definition[self.inputs.single().variant_id()];
            let headers = variant.data().map(|d| record_definition[d].name());
            Some(quote! {{
                writer.write_record([#(#headers),*])?;
            }})
        } else {
            None
        };

        let thread_body = quote! {
            #(
                #inputs
            )*

            move || {
                use std::fs::{create_dir_all, File};
                use std::path::Path;

                let file_path = Path::new(#output_file);
                if let Some(parent) = file_path.parent() {
                    if !parent.exists() {
                        create_dir_all(parent)?;
                    }
                }
                let file = File::create(file_path)?;

                let mut writer = csv::WriterBuilder::new()
                    .has_headers(false)
                    .from_writer(file);

                #write_headers

                while let Some(record) = input.next()? {
                    writer.serialize(record)?;
                }

                Ok(())
            }
        };

        chain.implement_node_thread(self, thread_id, &thread_body);

        chain.set_thread_main(thread_id, self.name.clone());
    }
}

pub fn write_csv(
    graph: &mut GraphBuilder,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    params: WriteCsvParams,
) -> ChainResultWithTrace<WriteCsv> {
    WriteCsv::new(graph, name, inputs, params)
}
