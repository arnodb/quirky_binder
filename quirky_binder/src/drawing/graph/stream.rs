use truc::record::definition::RecordVariantId;

use super::helper::{drawing_source_node_name, DrawingHelper, NodePortsBuilder};
use crate::{
    drawing::{Drawing, DrawingPortSize},
    graph::visit::{visit_node, Visit},
    prelude::*,
};

pub type StreamsPortKey<'a> = (&'a [Box<str>], &'a StreamRecordType, RecordVariantId);
pub type StreamsPathKey<'a> = (&'a StreamRecordType, RecordVariantId);

pub type StreamsDrawingHelper<'a> = DrawingHelper<'a, StreamsPortKey<'a>, StreamsPathKey<'a>>;

#[derive(new)]
struct StreamsDrawingVisitor<'a> {
    graph: &'a Graph,
    #[new(default)]
    helper: StreamsDrawingHelper<'a>,
}

impl<'a> StreamsDrawingVisitor<'a> {
    fn input_stream(
        graph: &'a Graph,
        ports: &mut NodePortsBuilder<'a, '_, StreamsPortKey<'a>, StreamsPathKey<'a>>,
        stream: &'a NodeStream,
    ) {
        let source = drawing_source_node_name(stream);
        let record_definition = &graph.record_definitions()[stream.record_type()];
        let variant = &record_definition[stream.variant_id()];

        ports.input(
            (source, stream.record_type(), stream.variant_id()),
            (stream.record_type(), stream.variant_id()),
            DrawingPortSize::Normal,
        );

        for d in variant.data() {
            if let Some(sub_stream) = stream.sub_streams().get(&d) {
                Self::input_sub_stream(graph, ports, sub_stream, source);
            }
        }
    }

    fn input_sub_stream(
        graph: &'a Graph,
        ports: &mut NodePortsBuilder<'a, '_, StreamsPortKey<'a>, StreamsPathKey<'a>>,
        stream: &'a NodeSubStream,
        source: &'a [Box<str>],
    ) {
        let record_definition = &graph.record_definitions()[stream.record_type()];
        let variant = &record_definition[stream.variant_id()];

        ports.input(
            (source, stream.record_type(), stream.variant_id()),
            (stream.record_type(), stream.variant_id()),
            DrawingPortSize::Small,
        );

        for d in variant.data() {
            if let Some(sub_stream) = stream.sub_streams().get(&d) {
                Self::input_sub_stream(graph, ports, sub_stream, source);
            }
        }
    }

    fn output_stream(
        graph: &'a Graph,
        ports: &mut NodePortsBuilder<'a, '_, StreamsPortKey<'a>, StreamsPathKey<'a>>,
        stream: &'a NodeStream,
    ) {
        let source = drawing_source_node_name(stream);
        let record_definition = &graph.record_definitions()[stream.record_type()];
        let variant = &record_definition[stream.variant_id()];

        ports.output(
            (source, stream.record_type(), stream.variant_id()),
            (stream.record_type(), stream.variant_id()),
            DrawingPortSize::Normal,
        );

        for d in variant.data() {
            if let Some(sub_stream) = stream.sub_streams().get(&d) {
                Self::output_sub_stream(graph, ports, sub_stream, source);
            }
        }
    }

    fn output_sub_stream(
        graph: &'a Graph,
        ports: &mut NodePortsBuilder<'a, '_, StreamsPortKey<'a>, StreamsPathKey<'a>>,
        stream: &'a NodeSubStream,
        source: &'a [Box<str>],
    ) {
        let record_definition = &graph.record_definitions()[stream.record_type()];
        let variant = &record_definition[stream.variant_id()];

        ports.output(
            (source, stream.record_type(), stream.variant_id()),
            (stream.record_type(), stream.variant_id()),
            DrawingPortSize::Small,
        );

        for d in variant.data() {
            if let Some(sub_stream) = stream.sub_streams().get(&d) {
                Self::output_sub_stream(graph, ports, sub_stream, source);
            }
        }
    }
}

impl<'a> Visit<'a> for StreamsDrawingVisitor<'a> {
    fn visit_node(&mut self, node: &'a dyn DynNode) {
        if node.is_cluster() {
            visit_node(self, node);
            return;
        }

        let (col, row) = self.helper.make_room_for_node(node);

        let mut ports = self.helper.node_ports_builder();

        for input in node.inputs() {
            Self::input_stream(self.graph, &mut ports, input);
        }

        for output in node.outputs() {
            Self::output_stream(self.graph, &mut ports, output);
        }

        let port_columns = ports.build();

        self.helper
            .push_node_into_column(col, row, node, port_columns);

        visit_node(self, node);
    }
}

pub fn draw_streams(graph: &Graph) -> Drawing {
    let mut visitor = StreamsDrawingVisitor::new(graph);
    visitor.visit_graph(graph);
    visitor.helper.into()
}
