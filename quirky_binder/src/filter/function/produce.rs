use proc_macro2::TokenStream;
use serde::Deserialize;
use truc::record::type_resolver::TypeResolver;

use crate::{prelude::*, trace_element};

const FUNCTION_PRODUCE_TRACE_NAME: &str = "function_produce";

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
    fields: Vec<(ValidFieldName, ValidFieldType)>,
    body: TokenStream,
}

impl FunctionProduce {
    fn new<R: TypeResolver + Copy>(
        graph: &mut GraphBuilder<R>,
        name: FullyQualifiedName,
        inputs: [NodeStream; 0],
        params: FunctionProduceParams,
    ) -> ChainResultWithTrace<Self> {
        let valid_fields = params
            .fields
            .validate_new()
            .with_trace_element(trace_element!(FUNCTION_PRODUCE_TRACE_NAME))?;

        let valid_order_fields = params
            .order_fields
            .map(|order_fields| {
                order_fields
                    .validate(|name| valid_fields.iter().any(|vf| vf.0.name() == name))
                    .with_trace_element(trace_element!(FUNCTION_PRODUCE_TRACE_NAME))
            })
            .transpose()?;

        let valid_distinct_fields = params
            .distinct_fields
            .map(|distinct_fields| {
                distinct_fields.validate(
                    |name| valid_fields.iter().any(|vf| vf.0.name() == name.name()),
                    FUNCTION_PRODUCE_TRACE_NAME,
                )
            })
            .transpose()?;

        let valid_body = params
            .body
            .parse::<TokenStream>()
            .map_err(|err| ChainError::InvalidTokenStream {
                name: "body".to_owned(),
                msg: err.to_string(),
            })
            .with_trace_element(trace_element!(FUNCTION_PRODUCE_TRACE_NAME))?;

        let mut streams = StreamsBuilder::new(&name, &inputs);
        streams
            .new_main_stream(graph)
            .with_trace_element(trace_element!(FUNCTION_PRODUCE_TRACE_NAME))?;

        streams
            .new_main_output(graph)
            .with_trace_element(trace_element!(FUNCTION_PRODUCE_TRACE_NAME))?
            .update(|output_stream, facts_proof| {
                {
                    let mut output_stream_def = output_stream.record_definition().borrow_mut();
                    for (name, r#type) in valid_fields.iter() {
                        output_stream_def.add_dynamic_datum(name.name(), r#type.type_name());
                    }
                }
                if let Some(order_fields) = valid_order_fields.as_ref() {
                    output_stream.set_order_fact(
                        order_fields
                            .iter()
                            .map(|field| field.as_ref().map(ValidFieldName::name)),
                    );
                }
                if let Some(distinct_fields) = valid_distinct_fields.as_ref() {
                    output_stream
                        .set_distinct_fact(distinct_fields.iter().map(ValidFieldName::name));
                }
                Ok(facts_proof.order_facts_updated().distinct_facts_updated())
            })?;

        let outputs = streams
            .build()
            .with_trace_element(trace_element!(FUNCTION_PRODUCE_TRACE_NAME))?;

        Ok(Self {
            name,
            inputs,
            outputs,
            fields: valid_fields,
            body: valid_body,
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
        let thread_id = chain.new_threaded_source(
            &self.name,
            ChainThreadType::Regular,
            &self.inputs,
            &self.outputs,
        );

        let def = chain.stream_definition_fragments(self.outputs.single());
        let record = def.record();
        let unpacked_record = def.unpacked_record();

        let (new_record_args, new_record_fields) = {
            let names = self
                .fields
                .iter()
                .map(|(name, _)| name.ident())
                .collect::<Vec<_>>();
            let types = self
                .fields
                .iter()
                .map(|(_, r#type)| r#type.r#type())
                .collect::<Vec<_>>();
            (quote!(#(#names: #types),*), quote!(#(#names),*))
        };

        let body = &self.body;

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
}

pub fn function_produce<R: TypeResolver + Copy>(
    graph: &mut GraphBuilder<R>,
    name: FullyQualifiedName,
    inputs: [NodeStream; 0],
    params: FunctionProduceParams,
) -> ChainResultWithTrace<FunctionProduce> {
    FunctionProduce::new(graph, name, inputs, params)
}
