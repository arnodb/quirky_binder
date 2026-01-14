use proc_macro2::TokenStream;
use serde::Deserialize;

use crate::{prelude::*, trace_element};

const FUNCTION_EMIT_TRACE_NAME: &str = "function_emit";

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct FunctionEmitParams<'a> {
    fields: TypedFieldsParam<'a>,
    order_fields: Option<DirectedFieldsParam<'a>>,
    distinct_fields: Option<FieldsParam<'a>>,
    body: &'a str,
}

#[derive(Debug, Getters)]
pub struct FunctionEmit {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 0],
    #[getset(get = "pub")]
    outputs: [NodeStream; 1],
    body: TokenStream,
}

impl FunctionEmit {
    fn new(
        graph: &mut GraphBuilder,
        name: FullyQualifiedName,
        inputs: [NodeStream; 0],
        params: FunctionEmitParams,
    ) -> ChainResultWithTrace<Self> {
        let valid_fields = params
            .fields
            .validate_new()
            .with_trace_element(trace_element!(FUNCTION_EMIT_TRACE_NAME))?;

        let valid_order_fields = params
            .order_fields
            .map(|order_fields| {
                order_fields
                    .validate(|name| valid_fields.iter().any(|vf| vf.0.name() == name))
                    .with_trace_element(trace_element!(FUNCTION_EMIT_TRACE_NAME))
            })
            .transpose()?;

        let valid_distinct_fields = params
            .distinct_fields
            .map(|distinct_fields| {
                distinct_fields.validate(
                    |name| valid_fields.iter().any(|vf| vf.0.name() == name.name()),
                    FUNCTION_EMIT_TRACE_NAME,
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
            .with_trace_element(trace_element!(FUNCTION_EMIT_TRACE_NAME))?;

        let mut streams = StreamsBuilder::new(&name, &inputs);
        streams
            .new_main_stream(graph)
            .with_trace_element(trace_element!(FUNCTION_EMIT_TRACE_NAME))?;

        streams
            .new_main_output(graph)
            .with_trace_element(trace_element!(FUNCTION_EMIT_TRACE_NAME))?
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
                            .with_trace_element(trace_element!(FUNCTION_EMIT_TRACE_NAME))?;
                    }
                }
                if let Some(order_fields) = valid_order_fields.as_ref() {
                    output_stream
                        .set_order_fact(
                            order_fields
                                .iter()
                                .map(|field| field.as_ref().map(ValidFieldName::name)),
                        )
                        .with_trace_element(trace_element!(FUNCTION_EMIT_TRACE_NAME))?;
                }
                if let Some(distinct_fields) = valid_distinct_fields.as_ref() {
                    output_stream
                        .set_distinct_fact(distinct_fields.iter().map(ValidFieldName::name))
                        .with_trace_element(trace_element!(FUNCTION_EMIT_TRACE_NAME))?;
                }
                Ok(facts_proof.order_facts_updated().distinct_facts_updated())
            })?;

        let outputs = streams
            .build()
            .with_trace_element(trace_element!(FUNCTION_EMIT_TRACE_NAME))?;

        Ok(Self {
            name,
            inputs,
            outputs,
            body: valid_body,
        })
    }
}

impl DynNode for FunctionEmit {
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
        let body = &self.body;

        chain.implement_inline_node(self, self.inputs.none(), self.outputs.single(), body);
    }
}

pub fn function_emit(
    graph: &mut GraphBuilder,
    name: FullyQualifiedName,
    inputs: [NodeStream; 0],
    params: FunctionEmitParams,
) -> ChainResultWithTrace<FunctionEmit> {
    FunctionEmit::new(graph, name, inputs, params)
}
