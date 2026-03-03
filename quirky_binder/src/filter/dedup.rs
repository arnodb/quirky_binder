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
            .with_trace_element(trace_element!())?
            .pass_through()
            .root(&mut streams, |stream, facts_proof| {
                let record_definition = graph
                    .get_stream(stream.record_type())
                    .with_trace_element(trace_element!())?
                    .borrow();

                set_distinct_fact_all_fields(stream.facts_mut(), &record_definition);

                Ok(facts_proof.order_facts_updated().distinct_facts_updated())
            })?;
        let outputs = streams.build().with_trace_element(trace_element!())?;
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
            self.inputs.some_single(),
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
    let _trace_name = TraceName::push(DEDUP_TRACE_NAME);
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
    path_streams: Vec<PassThroughPathElement>,
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
            .with_trace_element(trace_element!())?;

        let mut streams = StreamsBuilder::new(&name, &inputs);
        let path_streams = streams
            .output_from_input(0, true, graph)
            .with_trace_element(trace_element!())?
            .pass_through()
            .path(
                graph,
                &mut streams,
                valid_path_fields,
                |stream, facts_proof| {
                    let record_definition = graph
                        .get_stream(stream.record_type())
                        .with_trace_element(trace_element!())?
                        .borrow();

                    set_distinct_fact_all_fields(stream.facts_mut(), &record_definition);

                    Ok(facts_proof.order_facts_updated().distinct_facts_updated())
                },
            )?;

        let outputs = streams.build().with_trace_element(trace_element!())?;

        Ok(Self {
            name,
            inputs,
            outputs,
            path_streams,
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
        let path_stream = self.path_streams.last().expect("last path field");

        let sub_record = chain
            .customizer()
            .definition_fragments(&path_stream.sub_stream)
            .record();

        let sub_record_definition =
            &graph.record_definitions()[path_stream.sub_stream.record_type()];

        let sub_variant = &sub_record_definition[path_stream.sub_stream.variant_id()];

        let flat_map = self
            .path_streams
            .iter()
            .rev()
            .fold(None, |tail, path_stream| {
                let mut_access = path_stream.field.mut_ident();
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
            self.inputs.some_single(),
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
    let _trace_name = TraceName::push(SUB_DEDUP_TRACE_NAME);
    SubDedup::new(graph, name, inputs, params)
}
