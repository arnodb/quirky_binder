use crate::{prelude::*, support::fields_cmp};
use truc::record::type_resolver::TypeResolver;

#[derive(Getters)]
pub struct Sort {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 1],
    #[getset(get = "pub")]
    outputs: [NodeStream; 1],
    fields: Vec<String>,
}

impl Sort {
    fn new<R: TypeResolver + Copy>(
        graph: &mut GraphBuilder<R>,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        fields: &[&str],
    ) -> Self {
        let mut streams = StreamsBuilder::new(&name, &inputs);
        streams
            .output_from_input(0, true, graph)
            .pass_through(|builder, facts_proof| {
                builder.set_order_fact(fields);
                facts_proof.order_facts_updated().distinct_facts_updated()
            });
        let outputs = streams.build();
        Self {
            name,
            inputs,
            outputs,
            fields: fields.iter().map(ToString::to_string).collect::<Vec<_>>(),
        }
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

        let cmp = fields_cmp(&record, &self.fields);

        let inline_body = quote! {
            datapet_support::iterator::sort::Sort::new(input, #cmp)
        };

        chain.implement_inline_node(
            self,
            self.inputs.single(),
            self.outputs.single(),
            &inline_body,
        );
    }

    fn all_nodes(&self) -> Box<dyn Iterator<Item = &dyn DynNode> + '_> {
        Box::new(Some(self as &dyn DynNode).into_iter())
    }
}

pub fn sort<R: TypeResolver + Copy>(
    graph: &mut GraphBuilder<R>,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    fields: &[&str],
) -> Sort {
    Sort::new(graph, name, inputs, fields)
}

#[derive(Getters)]
pub struct SubSort {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 1],
    #[getset(get = "pub")]
    outputs: [NodeStream; 1],
    path_fields: Vec<String>,
    path_sub_stream: NodeSubStream,
    fields: Vec<String>,
}

impl SubSort {
    fn new<R: TypeResolver + Copy>(
        graph: &mut GraphBuilder<R>,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        path_fields: &[&str],
        fields: &[&str],
    ) -> Self {
        let mut streams = StreamsBuilder::new(&name, &inputs);
        let path_sub_stream =
            streams
                .output_from_input(0, true, graph)
                .pass_through(|output_stream, facts_proof| {
                    let path_sub_stream = output_stream.pass_through_path(
                        path_fields,
                        |sub_input_stream, output_stream| {
                            output_stream.pass_through_sub_stream(
                                sub_input_stream,
                                graph,
                                |sub_output_stream| sub_output_stream.set_order_fact(fields),
                            )
                        },
                        graph,
                    );
                    facts_proof
                        .order_facts_updated()
                        .distinct_facts_updated()
                        .with_output(path_sub_stream)
                });

        let outputs = streams.build();

        Self {
            name,
            inputs,
            outputs,
            path_fields: path_fields
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            path_sub_stream,
            fields: fields.iter().map(ToString::to_string).collect::<Vec<_>>(),
        }
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
            let access = format_ident!("{}_mut", field);
            Some(if let Some(tail) = tail {
                quote! {record.#access().iter_mut().flat_map(|record| #tail)}
            } else {
                quote! {Some(record.#access()).into_iter()}
            })
        });

        let cmp = fields_cmp(&sub_record, &self.fields);

        let inline_body = quote! {
            fn ci_fn(record: &mut #record) -> impl Iterator<Item = &mut Vec<#sub_record>> {
                #flat_map
            }
            datapet_support::iterator::sort::SubSort::new(
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

    fn all_nodes(&self) -> Box<dyn Iterator<Item = &dyn DynNode> + '_> {
        Box::new(Some(self as &dyn DynNode).into_iter())
    }
}

pub fn sub_sort<R: TypeResolver + Copy>(
    graph: &mut GraphBuilder<R>,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    path_fields: &[&str],
    fields: &[&str],
) -> SubSort {
    SubSort::new(graph, name, inputs, path_fields, fields)
}
