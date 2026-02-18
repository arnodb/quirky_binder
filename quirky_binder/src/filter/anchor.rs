use serde::Deserialize;

use crate::{prelude::*, trace_element};

const ANCHOR_TRACE_NAME: &str = "anchor";

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct AnchorParams<'a> {
    anchor_field: &'a str,
}

#[derive(Debug, Getters)]
pub struct Anchor {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 1],
    #[getset(get = "pub")]
    outputs: [NodeStream; 1],
    anchor_field: ValidFieldName,
}

impl Anchor {
    fn new(
        graph: &mut GraphBuilder,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        params: AnchorParams,
    ) -> ChainResultWithTrace<Self> {
        let valid_anchor_field = ValidFieldName::try_from(params.anchor_field)
            .map_err(|_| ChainError::InvalidFieldName {
                name: (params.anchor_field).to_owned(),
            })
            .with_trace_element(trace_element!())?;

        let anchor_table_id = graph.new_anchor_table();

        let mut streams = StreamsBuilder::new(&name, &inputs);

        streams
            .output_from_input(0, true, graph)
            .with_trace_element(trace_element!())?
            .update(|output_stream, facts_proof| {
                let mut output_stream_def = output_stream.record_definition().borrow_mut();
                output_stream_def
                    .add_datum(
                        valid_anchor_field.name(),
                        QuirkyDatumType::Simple {
                            type_name: format!(
                                "quirky_binder_support::AnchorId<{anchor_table_id}>"
                            ),
                        },
                    )
                    .map_err(|err| ChainError::Other { msg: err })
                    .with_trace_element(trace_element!())?;
                Ok(facts_proof.order_facts_updated().distinct_facts_updated())
            })?;

        let outputs = streams.build().with_trace_element(trace_element!())?;

        Ok(Self {
            name,
            inputs,
            outputs,
            anchor_field: valid_anchor_field,
        })
    }
}

impl DynNode for Anchor {
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
        let anchor_field = self.anchor_field.ident();

        let inline_body = quote! {
            let mut seq: usize = 0;
            input.map(move |record| {
                let anchor = seq;
                seq = anchor + 1;
                Ok(Output0::from((
                    record,
                    UnpackedOutputIn0 { #anchor_field: quirky_binder_support::AnchorId::new(anchor) },
                )))
            })
        };

        chain.implement_inline_node(
            self,
            self.inputs.some_single(),
            self.outputs.single(),
            &inline_body,
        );
    }
}

pub fn anchor(
    graph: &mut GraphBuilder,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    params: AnchorParams,
) -> ChainResultWithTrace<Anchor> {
    let _trace_name = TraceName::push(ANCHOR_TRACE_NAME);
    Anchor::new(graph, name, inputs, params)
}
