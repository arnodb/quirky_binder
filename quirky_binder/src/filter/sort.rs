use serde::Deserialize;

use crate::{prelude::*, support::cmp::fields_cmp, trace_element};

const SORT_TRACE_NAME: &str = "sort";

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct SortParams<'a> {
    #[serde(borrow)]
    fields: DirectedFieldsParam<'a>,
}

#[derive(Getters)]
pub struct Sort {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 1],
    #[getset(get = "pub")]
    outputs: [NodeStream; 1],
    fields: Vec<Directed<ValidFieldName>>,
}

impl Sort {
    fn new(
        graph: &mut GraphBuilder,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        params: SortParams,
    ) -> ChainResultWithTrace<Self> {
        let valid_fields = params
            .fields
            .validate_on_stream(inputs.single(), graph)
            .with_trace_element(trace_element!(SORT_TRACE_NAME))?;

        let mut streams = StreamsBuilder::new(&name, &inputs);
        streams
            .output_from_input(0, true, graph)
            .with_trace_element(trace_element!(SORT_TRACE_NAME))?
            .pass_through(|builder, facts_proof| {
                builder
                    .set_order_fact(
                        valid_fields
                            .iter()
                            .map(|field| field.as_ref().map(ValidFieldName::name)),
                    )
                    .with_trace_element(trace_element!(SORT_TRACE_NAME))?;
                Ok(facts_proof.order_facts_updated().distinct_facts_updated())
            })?;
        let outputs = streams
            .build()
            .with_trace_element(trace_element!(SORT_TRACE_NAME))?;
        Ok(Self {
            name,
            inputs,
            outputs,
            fields: valid_fields,
        })
    }
}

impl DynNode for Sort {
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
        let record = chain
            .stream_definition_fragments(self.outputs.single())
            .record();

        let cmp = fields_cmp(
            &record,
            self.fields
                .iter()
                .map(|field| field.as_ref().map(ValidFieldName::name)),
        );

        let inline_body = quote! {
            quirky_binder_support::iterator::sort::Sort::new(input, #cmp)
        };

        chain.implement_inline_node(
            self,
            self.inputs.single(),
            self.outputs.single(),
            &inline_body,
        );
    }
}

pub fn sort(
    graph: &mut GraphBuilder,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    params: SortParams,
) -> ChainResultWithTrace<Sort> {
    Sort::new(graph, name, inputs, params)
}

const SUB_SORT_TRACE_NAME: &str = "sub_sort";

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct SubSortParams<'a> {
    #[serde(borrow)]
    path_fields: FieldsParam<'a>,
    fields: DirectedFieldsParam<'a>,
}

#[derive(Getters)]
pub struct SubSort {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 1],
    #[getset(get = "pub")]
    outputs: [NodeStream; 1],
    path_fields: Vec<ValidFieldName>,
    path_sub_stream: NodeSubStream,
    fields: Vec<Directed<ValidFieldName>>,
}

impl SubSort {
    fn new(
        graph: &mut GraphBuilder,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        params: SubSortParams,
    ) -> ChainResultWithTrace<Self> {
        let (valid_path_fields, path_def) = params
            .path_fields
            .validate_path_on_stream(inputs.single(), graph)
            .with_trace_element(trace_element!(SUB_SORT_TRACE_NAME))?;
        let valid_fields = params
            .fields
            .validate_on_record_definition(&path_def)
            .with_trace_element(trace_element!(SUB_SORT_TRACE_NAME))?;

        let mut streams = StreamsBuilder::new(&name, &inputs);
        let path_sub_stream = streams
            .output_from_input(0, true, graph)
            .with_trace_element(trace_element!(SUB_SORT_TRACE_NAME))?
            .pass_through_path(
                graph,
                &valid_path_fields,
                |sub_output_stream| {
                    sub_output_stream
                        .set_order_fact(
                            valid_fields
                                .iter()
                                .map(|field| field.as_ref().map(ValidFieldName::name)),
                        )
                        .with_trace_element(trace_element!(SUB_SORT_TRACE_NAME))?;
                    Ok(())
                },
                SUB_SORT_TRACE_NAME,
            )?;

        let outputs = streams
            .build()
            .with_trace_element(trace_element!(SUB_SORT_TRACE_NAME))?;

        Ok(Self {
            name,
            inputs,
            outputs,
            path_fields: valid_path_fields,
            path_sub_stream,
            fields: valid_fields,
        })
    }
}

impl DynNode for SubSort {
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
        let record = chain
            .stream_definition_fragments(self.inputs.single())
            .record();
        let sub_record = chain
            .sub_stream_definition_fragments(&self.path_sub_stream)
            .record();

        let flat_map = self.path_fields.iter().rev().fold(None, |tail, field| {
            let mut_access = field.mut_ident();
            Some(if let Some(tail) = tail {
                quote! {record.#mut_access().iter_mut().flat_map(|record| #tail)}
            } else {
                quote! {Some(record.#mut_access()).into_iter()}
            })
        });

        let cmp = fields_cmp(
            &sub_record,
            self.fields
                .iter()
                .map(|field| field.as_ref().map(ValidFieldName::name)),
        );

        let inline_body = quote! {
            fn ci_fn(record: &mut #record) -> impl Iterator<Item = &mut Vec<#sub_record>> {
                #flat_map
            }
            quirky_binder_support::iterator::sort::SubSort::new(
                input,
                ci_fn,
                #cmp,
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

pub fn sub_sort(
    graph: &mut GraphBuilder,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    params: SubSortParams,
) -> ChainResultWithTrace<SubSort> {
    SubSort::new(graph, name, inputs, params)
}
