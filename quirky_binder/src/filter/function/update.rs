use proc_macro2::TokenStream;
use serde::Deserialize;
use truc::record::type_resolver::TypeResolver;

use crate::{prelude::*, trace_element};

const FUNCTION_UPDATE_TRACE_NAME: &str = "function_update";

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct FunctionUpdateParams<'a> {
    remove_fields: Option<FieldsParam<'a>>,
    add_fields: Option<TypedFieldsParam<'a>>,
    body: &'a str,
}

#[derive(Getters)]
pub struct FunctionUpdate {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 1],
    #[getset(get = "pub")]
    outputs: [NodeStream; 1],
    body: TokenStream,
}

impl FunctionUpdate {
    fn new<R: TypeResolver + Copy>(
        graph: &mut GraphBuilder<R>,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        params: FunctionUpdateParams,
    ) -> ChainResultWithTrace<Self> {
        let valid_remove_fields = params
            .remove_fields
            .map(|fields| {
                fields.validate_on_stream(inputs.single(), graph, FUNCTION_UPDATE_TRACE_NAME)
            })
            .transpose()?;

        let valid_add_fields = params
            .add_fields
            .map(|fields| fields.validate_new())
            .transpose()
            .with_trace_element(trace_element!(FUNCTION_UPDATE_TRACE_NAME))?;

        let valid_body = params
            .body
            .parse::<TokenStream>()
            .map_err(|err| ChainError::InvalidTokenStream {
                name: "body".to_owned(),
                msg: err.to_string(),
            })
            .with_trace_element(trace_element!(FUNCTION_UPDATE_TRACE_NAME))?;

        let mut streams = StreamsBuilder::new(&name, &inputs);
        streams
            .output_from_input(0, true, graph)
            .with_trace_element(trace_element!(FUNCTION_UPDATE_TRACE_NAME))?
            .update(|output_stream, facts_proof| {
                if let Some(remove_fields) = valid_remove_fields {
                    let mut output_stream_def = output_stream.record_definition().borrow_mut();
                    for name in remove_fields.iter() {
                        let datum_id = output_stream_def
                            .get_current_datum_definition_by_name(name.name())
                            .expect("datum")
                            .id();
                        output_stream_def.remove_datum(datum_id);
                    }
                }
                if let Some(add_fields) = valid_add_fields {
                    let mut output_stream_def = output_stream.record_definition().borrow_mut();
                    for (name, r#type) in add_fields.iter() {
                        output_stream_def.add_dynamic_datum(name.name(), r#type.type_name());
                    }
                }
                Ok(facts_proof.order_facts_updated().distinct_facts_updated())
            })?;
        let outputs = streams
            .build()
            .with_trace_element(trace_element!(FUNCTION_UPDATE_TRACE_NAME))?;
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

        chain.implement_inline_node(self, self.inputs.single(), self.outputs.single(), body);
    }
}

pub fn function_update<R: TypeResolver + Copy>(
    graph: &mut GraphBuilder<R>,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    params: FunctionUpdateParams,
) -> ChainResultWithTrace<FunctionUpdate> {
    FunctionUpdate::new(graph, name, inputs, params)
}
