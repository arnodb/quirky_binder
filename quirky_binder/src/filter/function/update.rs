use proc_macro2::TokenStream;
use serde::Deserialize;

use crate::{prelude::*, trace_element};

const FUNCTION_UPDATE_TRACE_NAME: &str = "function_update";

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct FunctionUpdateParams<'a> {
    remove_fields: Option<FieldsParam<'a>>,
    add_fields: Option<TypedFieldsParam<'a>>,
    body: &'a str,
}

#[derive(Debug, Getters)]
pub struct FunctionUpdate {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 1],
    #[getset(get = "pub")]
    outputs: [NodeStream; 1],
    body: TokenStream,
}

impl FunctionUpdate {
    fn new(
        graph: &mut GraphBuilder,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        params: FunctionUpdateParams,
    ) -> ChainResultWithTrace<Self> {
        let valid_remove_fields = params
            .remove_fields
            .map(|fields| fields.validate_on_stream(inputs.single(), graph))
            .transpose()?;

        let valid_add_fields = params
            .add_fields
            .map(|fields| fields.validate_new())
            .transpose()
            .with_trace_element(trace_element!())?;

        let valid_body = params
            .body
            .parse::<TokenStream>()
            .map_err(|err| ChainError::InvalidTokenStream {
                name: "body".to_owned(),
                msg: err.to_string(),
            })
            .with_trace_element(trace_element!())?;

        let mut streams = StreamsBuilder::new(&name, &inputs);
        streams
            .output_from_input(0, true, graph)
            .with_trace_element(trace_element!())?
            .update(|output_stream, facts_proof| {
                if let Some(remove_fields) = valid_remove_fields {
                    let mut output_stream_def = output_stream.record_definition().borrow_mut();
                    for name in remove_fields.iter() {
                        let datum_id = output_stream_def
                            .get_current_datum_definition_by_name(name.name())
                            .expect("datum")
                            .id();
                        output_stream_def
                            .remove_datum(datum_id)
                            .map_err(|err| ChainError::Other { msg: err })
                            .with_trace_element(trace_element!())?;
                    }
                }
                if let Some(add_fields) = valid_add_fields {
                    let mut output_stream_def = output_stream.record_definition().borrow_mut();
                    for (name, r#type) in add_fields.iter() {
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
                Ok(facts_proof.order_facts_updated().distinct_facts_updated())
            })?;
        let outputs = streams.build().with_trace_element(trace_element!())?;
        Ok(Self {
            name,
            inputs,
            outputs,
            body: valid_body,
        })
    }
}

impl DynNode for FunctionUpdate {
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

        chain.implement_inline_node(self, self.inputs.some_single(), self.outputs.single(), body);
    }
}

pub fn function_update(
    graph: &mut GraphBuilder,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    params: FunctionUpdateParams,
) -> ChainResultWithTrace<FunctionUpdate> {
    let _trace_name = TraceName::push(FUNCTION_UPDATE_TRACE_NAME);
    FunctionUpdate::new(graph, name, inputs, params)
}
