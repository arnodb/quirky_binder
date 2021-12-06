use crate::{
    chain::{Chain, ImportScope},
    dyn_node,
    graph::{DynNode, Graph, GraphBuilder, Node},
    stream::{NodeStream, NodeStreamSource},
    support::FullyQualifiedName,
};
use datapet_support::AnchorId;
use truc::record::definition::DatumDefinitionOverride;

pub struct Anchorize {
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    outputs: [NodeStream; 1],
    anchor_field: String,
}

impl Node<1, 1> for Anchorize {
    fn inputs(&self) -> &[NodeStream; 1] {
        &self.inputs
    }

    fn outputs(&self) -> &[NodeStream; 1] {
        &self.outputs
    }

    fn gen_chain(&self, graph: &Graph, chain: &mut Chain) {
        let thread = chain.get_thread_id_and_module_by_source(self.inputs[0].source(), &self.name);

        let local_name = self.name.last().expect("local name");
        let def =
            self.outputs[0].definition_fragments(&graph.chain_customizer().streams_module_name);
        let scope = chain.get_or_new_module_scope(
            self.name.iter().take(self.name.len() - 1),
            graph.chain_customizer(),
            thread.thread_id,
        );
        let mut import_scope = ImportScope::default();
        import_scope.add_import_with_error_type("fallible_iterator", "FallibleIterator");
        let node_fn = scope
            .new_fn(local_name)
            .vis("pub")
            .arg(
                "#[allow(unused_mut)] mut thread_control",
                format!("thread_{}::ThreadControl", thread.thread_id),
            )
            .ret(def.impl_fallible_iterator);
        let input = thread.format_input(
            self.inputs[0].source(),
            graph.chain_customizer(),
            &mut import_scope,
        );
        crate::chain::fn_body(
            format!(
                r#"let mut seq: usize = 0;
{input}
input
    .map(move |record| {{
        let anchor = seq;
        seq = anchor + 1;
        Ok({record}::from((
            record,
            {unpacked_record_in} {{ {anchor_field}: datapet_support::AnchorId::new(anchor) }},
        )))
    }})"#,
                input = input,
                record = def.record,
                unpacked_record_in = def.unpacked_record_in,
                anchor_field = self.anchor_field,
            ),
            node_fn,
        );
        import_scope.import(scope, graph.chain_customizer());

        chain.update_thread_single_stream(thread.thread_id, &self.outputs[0]);
    }
}

dyn_node!(Anchorize);

pub fn anchorize(
    graph: &mut GraphBuilder,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    anchor_field: &str,
) -> Anchorize {
    let [input] = inputs;

    let anchor_table_id = graph.new_anchor_table();

    let variant_id = {
        let mut stream = graph
            .get_stream(input.record_type())
            .unwrap_or_else(|| panic!(r#"stream "{}""#, input.record_type()))
            .borrow_mut();

        stream.add_datum_override::<AnchorId<0>, _>(
            anchor_field,
            DatumDefinitionOverride {
                type_name: Some(format!("datapet_support::AnchorId<{}>", anchor_table_id)),
                size: None,
                allow_uninit: Some(true),
            },
        );
        stream.close_record_variant()
    };

    let record_type = input.record_type().clone();
    Anchorize {
        name: name.clone(),
        inputs: [input],
        outputs: [NodeStream::new(
            record_type,
            variant_id,
            NodeStreamSource::from(name),
        )],
        anchor_field: anchor_field.to_string(),
    }
}
