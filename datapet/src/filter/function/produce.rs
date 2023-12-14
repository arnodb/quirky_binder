use crate::prelude::*;
use proc_macro2::TokenStream;
use serde::Deserialize;
use truc::record::type_resolver::TypeResolver;

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct FunctionProduceParams<'a> {
    fields: TypedFieldsParam<'a>,
    order_fields: Option<DirectedFieldsParam<'a>>,
    distinct_fields: Option<FieldsParam<'a>>,
    body: &'a str,
}

#[derive(Getters)]
pub struct FunctionProduce {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 0],
    #[getset(get = "pub")]
    outputs: [NodeStream; 1],
    fields: Vec<(String, String)>,
    body: String,
}

impl FunctionProduce {
    fn new<R: TypeResolver + Copy>(
        graph: &mut GraphBuilder<R>,
        name: FullyQualifiedName,
        inputs: [NodeStream; 0],
        params: FunctionProduceParams,
    ) -> ChainResult<Self> {
        let mut streams = StreamsBuilder::new(&name, &inputs);
        streams.new_main_stream(graph);

        streams
            .new_main_output(graph)
            .update(|output_stream, facts_proof| {
                {
                    let mut output_stream_def = output_stream.record_definition().borrow_mut();
                    for (name, r#type) in params.fields.iter() {
                        output_stream_def.add_dynamic_datum(*name, r#type);
                    }
                }
                if let Some(order_fields) = params.order_fields.as_ref() {
                    output_stream.set_order_fact(order_fields);
                }
                if let Some(distinct_fields) = params.distinct_fields.as_ref() {
                    output_stream.set_distinct_fact(distinct_fields);
                }
                facts_proof.order_facts_updated().distinct_facts_updated()
            });

        let outputs = streams.build();

        Ok(Self {
            name,
            inputs,
            outputs,
            fields: params
                .fields
                .iter()
                .map(|(name, r#type)| ((*name).to_owned(), (*r#type).to_owned()))
                .collect(),
            body: params.body.to_owned(),
        })
    }
}

impl DynNode for FunctionProduce {
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
        let thread_id = chain.new_threaded_source(&self.name, &self.inputs, &self.outputs);

        let def = chain.stream_definition_fragments(self.outputs.single());
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

        let body: TokenStream = self.body.parse().expect("function body");

        let thread_body = quote! {
            let output = thread_control.output_0.take().expect("output 0");
            let new_record = |#new_record_args| {
                #record::new(#unpacked_record { #new_record_fields })
            };
            move || {
                #body
            }
        };

        chain.implement_node_thread(self, thread_id, &thread_body);
    }

    fn all_nodes(&self) -> Box<dyn Iterator<Item = &dyn DynNode> + '_> {
        Box::new(Some(self as &dyn DynNode).into_iter())
    }
}

pub fn function_produce<R: TypeResolver + Copy>(
    graph: &mut GraphBuilder<R>,
    name: FullyQualifiedName,
    inputs: [NodeStream; 0],
    params: FunctionProduceParams,
) -> ChainResult<FunctionProduce> {
    FunctionProduce::new(graph, name, inputs, params)
}
