use crate::{prelude::*, stream::UniqueNodeStream, support::fields_cmp};
use truc::record::{definition::DatumDefinition, type_resolver::TypeResolver};

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
        streams.output_from_input(0, true, graph).pass_through();
        let outputs = streams.build(graph);
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
            .stream_definition_fragments(self.outputs.unique())
            .record();

        let cmp = fields_cmp(&record, &self.fields);

        let inline_body = quote! {
            datapet_support::iterator::sort::Sort::new(input, #cmp)
        };

        chain.implement_inline_node(
            self,
            self.inputs.unique(),
            self.outputs.unique(),
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
    path_sub_stream: NodeStream,
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
        let input_stream = streams.output_from_input(0, true, graph).pass_through();
        let path_sub_stream = path_fields
            .iter()
            .fold(
                (inputs.unique().clone(), input_stream),
                |(stream, def), field| {
                    let datum_id = def
                        .borrow()
                        .get_latest_variant_datum_definition_by_name(field)
                        .map(DatumDefinition::id);
                    if let Some(datum_id) = datum_id {
                        let sub_stream = graph
                            .get_sub_stream(stream.record_type().clone(), datum_id)
                            .expect("sub stream");
                        let sub_def = graph
                            .get_stream(sub_stream.record_type())
                            .expect("sub stream def");
                        (sub_stream.clone(), sub_def)
                    } else {
                        panic!("could not find datum `{}`", field);
                    }
                },
            )
            .0;
        let outputs = streams.build(graph);
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
            .stream_definition_fragments(self.inputs.unique())
            .record();
        let sub_record = chain
            .stream_definition_fragments(&self.path_sub_stream)
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
            self.inputs.unique(),
            self.outputs.unique(),
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
