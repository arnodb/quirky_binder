use crate::{prelude::*, stream::UniqueNodeStream, support::fields_cmp};
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
        streams.output_from_input(0, graph).pass_through();
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
}

pub fn sort<R: TypeResolver + Copy>(
    graph: &mut GraphBuilder<R>,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    fields: &[&str],
) -> Sort {
    Sort::new(graph, name, inputs, fields)
}
