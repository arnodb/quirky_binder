use crate::{prelude::*, stream::UniqueNodeStream};
use proc_macro2::TokenStream;
use truc::record::type_resolver::TypeResolver;

#[derive(Getters)]
pub struct FunctionSource {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 0],
    #[getset(get = "pub")]
    outputs: [NodeStream; 1],
    fields: Vec<(String, String)>,
    func: String,
}

impl FunctionSource {
    fn new<R: TypeResolver + Copy>(
        graph: &mut GraphBuilder<R>,
        name: FullyQualifiedName,
        inputs: [NodeStream; 0],
        fields: &[(&str, &str)],
        func: &str,
    ) -> Self {
        let mut streams = StreamsBuilder::new(&name, &inputs);
        streams.new_main_stream(graph);

        {
            let output_stream = streams.new_main_output(graph).for_update();

            let mut output_stream_def = output_stream.borrow_mut();
            for (name, r#type) in fields.iter() {
                output_stream_def.add_dynamic_datum(*name, r#type);
            }
        }

        let outputs = streams.build();

        Self {
            name,
            inputs,
            outputs,
            fields: fields
                .iter()
                .map(|(name, r#type)| ((*name).to_owned(), (*r#type).to_owned()))
                .collect(),
            func: func.to_owned(),
        }
    }
}

impl DynNode for FunctionSource {
    fn name(&self) -> &FullyQualifiedName {
        &self.name
    }

    fn gen_chain(&self, _graph: &Graph, chain: &mut Chain) {
        let thread_id = chain.new_threaded_source(&self.name, &self.inputs, &self.outputs);

        let def = chain.stream_definition_fragments(self.outputs.unique());
        let record = def.record();
        let unpacked_record = def.unpacked_record();

        let (new_record_args, new_record_fields) = {
            let names = self
                .fields
                .iter()
                .map(|(name, _)| format_ident!("{}", name))
                .collect::<Vec<_>>();
            let types = self
                .fields
                .iter()
                .map(|(_, r#type)| syn::parse_str::<syn::Type>(r#type).expect("field type"))
                .collect::<Vec<_>>();
            (quote!(#(#names: #types),*), quote!(#(#names),*))
        };

        let func_body: TokenStream = self.func.parse().expect("function body");

        let thread_body = quote! {
            move || {
                let out = thread_control.output_0.take().expect("output 0");
                let new_record = |#new_record_args| {
                    #record::new(#unpacked_record { #new_record_fields })
                };
                #func_body
            }
        };

        chain.implement_node_thread(self, thread_id, &thread_body);
    }
}

pub fn function_source<R: TypeResolver + Copy>(
    graph: &mut GraphBuilder<R>,
    name: FullyQualifiedName,
    inputs: [NodeStream; 0],
    fields: &[(&str, &str)],
    func: &str,
) -> FunctionSource {
    FunctionSource::new(graph, name, inputs, fields, func)
}
