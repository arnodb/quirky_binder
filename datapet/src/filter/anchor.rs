use crate::prelude::*;
use datapet_support::AnchorId;
use truc::record::{definition::DatumDefinitionOverride, type_resolver::TypeResolver};

#[derive(Getters)]
pub struct Anchor {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 1],
    #[getset(get = "pub")]
    outputs: [NodeStream; 1],
    anchor_field: String,
}

impl Anchor {
    fn new<R: TypeResolver + Copy>(
        graph: &mut GraphBuilder<R>,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        anchor_field: &str,
    ) -> Self {
        let anchor_table_id = graph.new_anchor_table();

        let mut streams = StreamsBuilder::new(&name, &inputs);

        {
            let output_stream = streams.output_from_input(0, true, graph).for_update();
            let mut output_stream_def = output_stream.borrow_mut();
            output_stream_def.add_datum_override::<AnchorId<0>, _>(
                anchor_field,
                DatumDefinitionOverride {
                    type_name: Some(format!("datapet_support::AnchorId<{}>", anchor_table_id)),
                    size: None,
                    align: None,
                    allow_uninit: Some(true),
                },
            );
        }

        let outputs = streams.build();

        Self {
            name,
            inputs,
            outputs,
            anchor_field: anchor_field.to_string(),
        }
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
        let def_output = chain.stream_definition_fragments(self.outputs.single());

        let output_record = def_output.record();
        let output_unpacked_record_in = def_output.unpacked_record_in();

        let anchor_field = format_ident!("{}", self.anchor_field);

        let inline_body = quote! {
            let mut seq: usize = 0;
            input.map(move |record| {
                let anchor = seq;
                seq = anchor + 1;
                Ok(#output_record::from((
                    record,
                    #output_unpacked_record_in { #anchor_field: datapet_support::AnchorId::new(anchor) },
                )))
            })
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

pub fn anchor<R: TypeResolver + Copy>(
    graph: &mut GraphBuilder<R>,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    anchor_field: &str,
) -> Anchor {
    Anchor::new(graph, name, inputs, anchor_field)
}
