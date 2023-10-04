use crate::{prelude::*, stream::UniqueNodeStream};
use truc::record::type_resolver::TypeResolver;

#[derive(Getters)]
pub struct ExtractFields {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 1],
    #[getset(get = "pub")]
    outputs: [NodeStream; 2],
}

impl ExtractFields {
    fn new<R: TypeResolver + Copy>(
        graph: &mut GraphBuilder<R>,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        fields: &[&str],
    ) -> Self {
        let mut streams = StreamsBuilder::new(&name, &inputs);
        streams.new_named_stream("extracted", graph);

        let input_stream = streams.output_from_input(0, graph).pass_through();

        {
            let output_extracted_stream = streams.new_named_output("extracted", graph).for_update();
            let mut output_extracted_stream_def = output_extracted_stream.borrow_mut();
            for field in fields.iter() {
                output_extracted_stream_def.copy_datum(
                    input_stream
                        .borrow()
                        .get_variant_datum_definition_by_name(inputs.unique().variant_id(), field)
                        .unwrap_or_else(|| panic!(r#"datum "{}""#, field)),
                );
            }
        }

        let outputs = streams.build();

        Self {
            name,
            inputs,
            outputs,
        }
    }
}

impl DynNode for ExtractFields {
    fn name(&self) -> &FullyQualifiedName {
        &self.name
    }

    fn gen_chain(&self, graph: &Graph, chain: &mut Chain) {
        let thread_id = chain.pipe_inputs(&self.name, &self.inputs, &self.outputs);

        let def_output_1 = chain.stream_definition_fragments(&self.outputs[1]);

        let record_definition = &graph.record_definitions()[self.outputs[1].record_type()];
        let variant = &record_definition[self.outputs[1].variant_id()];
        let datum_clones = variant.data().map(|d| {
            let datum = &record_definition[d];
            syn::parse_str::<syn::Stmt>(&format!(
                "let {name} = {deref}record.{name}(){clone};",
                name = datum.name(),
                deref = if datum.allow_uninit() { "*" } else { "" },
                clone = if datum.allow_uninit() { "" } else { ".clone()" },
            ))
            .expect("clone")
        });

        let output_record_1 = def_output_1.record();
        let output_unpacked_record_1 = def_output_1.unpacked_record();

        let fields = variant.data().map(|d| {
            let datum = &record_definition[d];
            format_ident!("{}", datum.name())
        });

        let thread_body = quote! {
            move || {
                let rx = thread_control.input_0.take().expect("input 0");
                let tx_0 = thread_control.output_0.take().expect("output 0");
                let tx_1 = thread_control.output_1.take().expect("output 1");
                while let Some(record) = rx.recv()? {
                    #(#datum_clones)*
                    let record_1 = #output_record_1::new(
                        #output_unpacked_record_1 { #(#fields),* }
                    );
                    tx_0.send(Some(record))?;
                    tx_1.send(Some(record_1))?;
                }
                tx_0.send(None)?;
                tx_1.send(None)?;
                Ok(())
            }
        };

        chain.implement_node_thread(self, thread_id, &thread_body);
    }
}

pub fn extract_fields<R: TypeResolver + Copy>(
    graph: &mut GraphBuilder<R>,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    fields: &[&str],
) -> ExtractFields {
    ExtractFields::new(graph, name, inputs, fields)
}
