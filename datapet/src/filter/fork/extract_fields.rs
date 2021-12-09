use crate::prelude::*;
use std::cell::RefCell;
use truc::record::definition::{RecordDefinitionBuilder, RecordVariantId};

#[datapet_node(
    in = "-",
    out = "-",
    out = "extracted",
    init = "streams",
    arg = "fields: &[&str]"
)]
pub struct ExtractFields {
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    outputs: [NodeStream; 2],
}

impl ExtractFields {
    fn initialize_streams(
        (input_stream, input_variant_id): (&RefCell<RecordDefinitionBuilder>, RecordVariantId),
        output_extracted_stream: &RefCell<RecordDefinitionBuilder>,
        fields: &[&str],
    ) -> [RecordVariantId; 1] {
        let mut output_extracted_stream = output_extracted_stream.borrow_mut();
        for field in fields.iter() {
            output_extracted_stream.copy_datum(
                input_stream
                    .borrow()
                    .get_variant_datum_definition_by_name(input_variant_id, field)
                    .unwrap_or_else(|| panic!(r#"datum "{}""#, field)),
            );
        }
        [output_extracted_stream.close_record_variant()]
    }
}

impl DynNode for ExtractFields {
    fn gen_chain(&self, graph: &Graph, chain: &mut Chain) {
        let input_pipe = chain.pipe_single_thread(self.inputs[0].source());

        let thread_id = chain.new_thread(
            self.name.clone(),
            self.inputs.to_vec().into_boxed_slice(),
            self.outputs.to_vec().into_boxed_slice(),
            if let Some(pipe) = input_pipe {
                Some(Box::new([pipe]))
            } else {
                None
            },
            false,
            Some(self.name.clone()),
        );

        let scope = chain.get_or_new_module_scope(
            self.name.iter().take(self.name.len() - 1),
            graph.chain_customizer(),
            thread_id,
        );

        {
            let fn_name = format_ident!("{}", **self.name.last().expect("local name"));
            let thread_module = format_ident!("thread_{}", thread_id);
            let error_type = graph.chain_customizer().error_type.to_name();

            let record_definition = &graph.record_definitions()[self.outputs[1].record_type()];
            let variant = record_definition
                .get_variant(self.outputs[1].variant_id())
                .unwrap_or_else(|| panic!("variant #{}", self.outputs[1].variant_id()));
            let datum_clones = variant.data().map(|d| {
                let datum = record_definition
                    .get_datum_definition(d)
                    .unwrap_or_else(|| panic!("datum #{}", d));
                syn::parse_str::<syn::Stmt>(&format!(
                    "let {name} = {deref}record.{name}(){clone};",
                    name = datum.name(),
                    deref = if datum.allow_uninit() { "*" } else { "" },
                    clone = if datum.allow_uninit() { "" } else { ".clone()" },
                ))
                .expect("clone")
            });

            let def_output_1 =
                self.outputs[1].definition_fragments(&graph.chain_customizer().streams_module_name);
            let output_record_1 = def_output_1.record();
            let output_unpacked_record_1 = def_output_1.unpacked_record();

            let fields = variant.data().map(|d| {
                let datum = record_definition
                    .get_datum_definition(d)
                    .unwrap_or_else(|| panic!("datum #{}", d));
                format_ident!("{}", datum.name())
            });

            let fn_def = quote! {
                pub fn #fn_name(mut thread_control: #thread_module::ThreadControl) -> impl FnOnce() -> Result<(), #error_type> {
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
                }
            };
            scope.raw(&fn_def.to_string());
        }
    }
}

pub fn extract_fields(
    graph: &mut GraphBuilder,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    fields: &[&str],
) -> ExtractFields {
    ExtractFields::new(graph, name, inputs, fields)
}
