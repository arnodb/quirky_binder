use serde::Deserialize;

use crate::{prelude::*, support::eq::fields_eq, trace_element};

const DEDUP_TRACE_NAME: &str = "dedup";

#[derive(Debug, Getters)]
pub struct Dedup {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 1],
    #[getset(get = "pub")]
    outputs: [NodeStream; 1],
}

impl Dedup {
    fn new(
        graph: &mut GraphBuilder,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        _params: (),
    ) -> ChainResultWithTrace<Self> {
        let mut streams = StreamsBuilder::new(&name, &inputs);
        streams
            .output_from_input(0, true, graph)
            .with_trace_element(trace_element!(DEDUP_TRACE_NAME))?
            .pass_through(|output_stream, facts_proof| {
                output_stream.set_distinct_fact_all_fields();
                Ok(facts_proof.order_facts_updated().distinct_facts_updated())
            })?;
        let outputs = streams
            .build()
            .with_trace_element(trace_element!(DEDUP_TRACE_NAME))?;
        Ok(Self {
            name,
            inputs,
            outputs,
        })
    }
}

impl DynNode for Dedup {
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
        let record_definition = &graph.record_definitions()[self.inputs.single().record_type()];
        let variant = &record_definition[self.inputs.single().variant_id()];

        let eq = fields_eq(
            &syn::parse_str::<syn::Type>("Input0").unwrap(),
            variant.data().map(|d| record_definition[d].name()),
        );

        let inline_body = quote! {
            quirky_binder_support::iterator::dedup::Dedup::new(input, #eq)
        };

        chain.implement_inline_node(
            self,
            self.inputs.single(),
            self.outputs.single(),
            &inline_body,
        );
    }
}

pub fn dedup(
    graph: &mut GraphBuilder,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    params: (),
) -> ChainResultWithTrace<Dedup> {
    Dedup::new(graph, name, inputs, params)
}

const SUB_DEDUP_TRACE_NAME: &str = "sub_dedup";

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct SubDedupParams<'a> {
    #[serde(borrow)]
    path_fields: FieldsParam<'a>,
}

#[derive(Debug, Getters)]
pub struct SubDedup {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 1],
    #[getset(get = "pub")]
    outputs: [NodeStream; 1],
    path_fields: Vec<ValidFieldName>,
    path_sub_stream: NodeSubStream,
}

impl SubDedup {
    fn new(
        graph: &mut GraphBuilder,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        params: SubDedupParams,
    ) -> ChainResultWithTrace<Self> {
        let (valid_path_fields, _) = params
            .path_fields
            .validate_path_on_stream(inputs.single(), graph)
            .with_trace_element(trace_element!(SUB_DEDUP_TRACE_NAME))?;

        let mut streams = StreamsBuilder::new(&name, &inputs);
        let path_sub_stream = streams
            .output_from_input(0, true, graph)
            .with_trace_element(trace_element!(SUB_DEDUP_TRACE_NAME))?
            .pass_through(|output_stream, facts_proof| {
                let path_sub_stream = output_stream.pass_through_path(
                    &valid_path_fields,
                    |sub_input_stream, output_stream| {
                        output_stream
                            .pass_through_sub_stream(
                                sub_input_stream,
                                graph,
                                |sub_output_stream| {
                                    sub_output_stream.set_distinct_fact_all_fields();
                                    Ok(())
                                },
                                SUB_DEDUP_TRACE_NAME,
                            )
                            .with_trace_element(trace_element!(SUB_DEDUP_TRACE_NAME))
                    },
                    graph,
                    SUB_DEDUP_TRACE_NAME,
                )?;
                Ok(facts_proof
                    .order_facts_updated()
                    .distinct_facts_updated()
                    .with_output(path_sub_stream))
            })?;

        let outputs = streams
            .build()
            .with_trace_element(trace_element!(SUB_DEDUP_TRACE_NAME))?;

        Ok(Self {
            name,
            inputs,
            outputs,
            path_fields: valid_path_fields,
            path_sub_stream,
        })
    }
}

impl DynNode for SubDedup {
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
        let sub_record = chain
            .sub_stream_definition_fragments(&self.path_sub_stream)
            .record();
        let sub_record_definition = &graph.record_definitions()[self.path_sub_stream.record_type()];
        let sub_variant = &sub_record_definition[self.path_sub_stream.variant_id()];

        let flat_map = self.path_fields.iter().rev().fold(None, |tail, field| {
            let mut_access = field.mut_ident();
            Some(if let Some(tail) = tail {
                quote! {record.#mut_access().iter_mut().flat_map(|record| #tail)}
            } else {
                quote! {Some(record.#mut_access()).into_iter()}
            })
        });

        let eq = fields_eq(
            &sub_record,
            sub_variant.data().map(|d| sub_record_definition[d].name()),
        );

        let inline_body = quote! {
            fn ci_fn(record: &mut Input0) -> impl Iterator<Item = &mut Vec<#sub_record>> {
                #flat_map
            }
            quirky_binder_support::iterator::dedup::SubDedup::new(
                input,
                ci_fn,
                #eq,
            )
        };

        chain.implement_inline_node(
            self,
            self.inputs.single(),
            self.outputs.single(),
            &inline_body,
        );
    }
}

pub fn sub_dedup(
    graph: &mut GraphBuilder,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    params: SubDedupParams,
) -> ChainResultWithTrace<SubDedup> {
    SubDedup::new(graph, name, inputs, params)
}
