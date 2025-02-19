use super::{
    super::Graph,
    helper::{drawing_source_node_name, DrawingHelper, NodePortsBuilder},
};
use crate::{
    drawing::{Drawing, DrawingPortSize, DynNode},
    graph::visit::{visit_node, Visit},
    prelude::*,
    stream::StreamRecordType,
};

pub type RecordsPortKey<'a> = (&'a [Box<str>], &'a StreamRecordType, QuirkyDatumId);
pub type RecordsPathKey<'a> = (&'a StreamRecordType, QuirkyDatumId);

pub type RecordsDrawingHelper<'a> = DrawingHelper<'a, RecordsPortKey<'a>, RecordsPathKey<'a>>;

#[derive(new)]
struct RecordsDrawingVisitor<'a> {
    graph: &'a Graph,
    #[new(default)]
    helper: RecordsDrawingHelper<'a>,
}

impl<'a> RecordsDrawingVisitor<'a> {
    fn input_stream(
        &mut self,
        ports: &mut NodePortsBuilder<RecordsPortKey<'a>, RecordsPathKey<'a>>,
        stream: &'a NodeStream,
    ) {
        let source = drawing_source_node_name(stream);
        let record_definition = &self.graph.record_definitions()[stream.record_type()];
        let variant = &record_definition[stream.variant_id()];

        for d in variant.data() {
            ports.input(
                (source, stream.record_type(), d),
                (stream.record_type(), d),
                DrawingPortSize::Normal,
            );

            if let Some(sub_stream) = stream.sub_streams().get(&d) {
                self.input_sub_stream(ports, sub_stream, source);
            }
        }
    }

    fn input_sub_stream(
        &mut self,
        ports: &mut NodePortsBuilder<RecordsPortKey<'a>, RecordsPathKey<'a>>,
        stream: &'a NodeSubStream,
        source: &'a [Box<str>],
    ) {
        let record_definition = &self.graph.record_definitions()[stream.record_type()];
        let variant = &record_definition[stream.variant_id()];

        for d in variant.data() {
            ports.input(
                (source, stream.record_type(), d),
                (stream.record_type(), d),
                DrawingPortSize::Small,
            );

            if let Some(sub_stream) = stream.sub_streams().get(&d) {
                self.input_sub_stream(ports, sub_stream, source);
            }
        }
    }

    fn output_stream(
        &mut self,
        ports: &mut NodePortsBuilder<RecordsPortKey<'a>, RecordsPathKey<'a>>,
        stream: &'a NodeStream,
    ) {
        let source = drawing_source_node_name(stream);
        let record_definition = &self.graph.record_definitions()[stream.record_type()];
        let variant = &record_definition[stream.variant_id()];

        for d in variant.data() {
            ports.output(
                (source, stream.record_type(), d),
                (stream.record_type(), d),
                DrawingPortSize::Normal,
            );

            if let Some(sub_stream) = stream.sub_streams().get(&d) {
                self.output_sub_stream(ports, sub_stream, source);
            }
        }
    }

    fn output_sub_stream(
        &mut self,
        ports: &mut NodePortsBuilder<RecordsPortKey<'a>, RecordsPathKey<'a>>,
        stream: &'a NodeSubStream,
        source: &'a [Box<str>],
    ) {
        let record_definition = &self.graph.record_definitions()[stream.record_type()];
        let variant = &record_definition[stream.variant_id()];

        for d in variant.data() {
            ports.output(
                (source, stream.record_type(), d),
                (stream.record_type(), d),
                DrawingPortSize::Small,
            );

            if let Some(sub_stream) = stream.sub_streams().get(&d) {
                self.output_sub_stream(ports, sub_stream, source);
            }
        }
    }
}

impl<'a> Visit<'a> for RecordsDrawingVisitor<'a> {
    fn visit_node(&mut self, node: &'a dyn DynNode) {
        if node.is_cluster() {
            visit_node(self, node);
            return;
        }

        let (col, row) = self.helper.make_room_for_node(node);

        let mut ports = NodePortsBuilder::default();

        for input in node.inputs() {
            self.input_stream(&mut ports, input);
        }

        for output in node.outputs() {
            self.output_stream(&mut ports, output);
        }

        let port_columns = ports.build(&mut self.helper);

        self.helper
            .push_node_into_column(col, row, node, port_columns);

        visit_node(self, node);
    }
}

pub fn draw_records(graph: &Graph) -> Drawing {
    let mut visitor = RecordsDrawingVisitor::new(graph);
    visitor.visit_graph(graph);
    visitor.helper.into()
}
