use quirky_binder::prelude::*;
use serde::Deserialize;
use truc::record::type_resolver::TypeResolver;

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct WriteCsvParams<'a> {
    output_file: &'a str,
}

#[derive(Getters)]
pub struct WriteCsv {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 1],
    #[getset(get = "pub")]
    outputs: [NodeStream; 0],
    output_file: String,
}

impl WriteCsv {
    fn new<R: TypeResolver + Copy>(
        _graph: &mut GraphBuilder<R>,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        params: WriteCsvParams,
        _trace: Trace,
    ) -> ChainResult<Self> {
        Ok(Self {
            name,
            inputs,
            outputs: [],
            output_file: params.output_file.to_owned(),
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

            let input =
                thread.format_input(self.inputs[0].source(), graph.chain_customizer(), true);

            (thread.thread_id, vec![input])
        } else {
            let thread_id = chain.pipe_inputs(&self.name, &self.inputs, &self.outputs);

            let inputs = (0..self.inputs.len())
                .map(|input_index| {
                    let input = format_ident!("input_{}", input_index);
                    let expect = format!("input {}", input_index);
                    quote! { let #input = thread_control.#input.take().expect(#expect); }
                })
                .collect::<Vec<_>>();

            (thread_id, inputs)
        };

        let output_file = &self.output_file;

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
                        create_dir_all(parent)
                            .map_err(|err| QuirkyBinderError::Custom(err.to_string()))?;
                    }
                }
                let file = File::create(file_path)
                    .map_err(|err| QuirkyBinderError::Custom(err.to_string()))?;
                let mut writer = csv::Writer::from_writer(file);
                while let Some(record) = input.next()? {
                    writer.serialize(record)
                        .map_err(|err| QuirkyBinderError::Custom(err.to_string()))?;
                }
                Ok(())
            }
        };

        chain.implement_node_thread(self, thread_id, &thread_body);

        chain.set_thread_main(thread_id, self.name.clone());
    }
}

pub fn write_csv<R: TypeResolver + Copy>(
    graph: &mut GraphBuilder<R>,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    params: WriteCsvParams,
    trace: Trace,
) -> ChainResult<WriteCsv> {
    WriteCsv::new(graph, name, inputs, params, trace)
}
