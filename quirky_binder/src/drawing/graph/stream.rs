use super::helper::{drawing_source_node_name, DrawingHelper, NodePortsBuilder};
use crate::{
    drawing::{Drawing, DrawingPortSize},
    graph::visit::{visit_node, Visit},
    prelude::*,
};

pub type StreamsPortKey<'a> = (&'a [Box<str>], &'a StreamRecordType, QuirkyRecordVariantId);
pub type StreamsPathKey<'a> = (&'a StreamRecordType, QuirkyRecordVariantId);

pub type StreamsDrawingHelper<'a> = DrawingHelper<'a, StreamsPortKey<'a>, StreamsPathKey<'a>>;

#[derive(new)]
struct StreamsDrawingVisitor<'a> {
    graph: &'a Graph,
    #[new(default)]
    helper: StreamsDrawingHelper<'a>,
}

impl<'a> StreamsDrawingVisitor<'a> {
    fn input_stream(
        &mut self,
        ports: &mut NodePortsBuilder<StreamsPortKey<'a>, StreamsPathKey<'a>>,
        stream: &'a NodeStream,
    ) {
        let source = drawing_source_node_name(stream);
        let record_definition = &self.graph.record_definitions()[stream.record_type()];
        let variant = &record_definition[stream.variant_id()];

        ports.input(
            (source, stream.record_type(), stream.variant_id()),
            (stream.record_type(), stream.variant_id()),
            DrawingPortSize::Normal,
        );

        for d in variant.data() {
            if let Some(sub_stream) = stream.sub_streams().get(&d) {
                self.input_sub_stream(ports, sub_stream, source);
            }
        }
    }

    fn input_sub_stream(
        &mut self,
        ports: &mut NodePortsBuilder<StreamsPortKey<'a>, StreamsPathKey<'a>>,
        stream: &'a NodeSubStream,
        source: &'a [Box<str>],
    ) {
        let record_definition = &self.graph.record_definitions()[stream.record_type()];
        let variant = &record_definition[stream.variant_id()];

        ports.input(
            (source, stream.record_type(), stream.variant_id()),
            (stream.record_type(), stream.variant_id()),
            DrawingPortSize::Small,
        );

        for d in variant.data() {
            if let Some(sub_stream) = stream.sub_streams().get(&d) {
                self.input_sub_stream(ports, sub_stream, source);
            }
        }
    }

    fn output_stream(
        &mut self,
        ports: &mut NodePortsBuilder<StreamsPortKey<'a>, StreamsPathKey<'a>>,
        stream: &'a NodeStream,
    ) {
        let source = drawing_source_node_name(stream);
        let record_definition = &self.graph.record_definitions()[stream.record_type()];
        let variant = &record_definition[stream.variant_id()];

        ports.output(
            (source, stream.record_type(), stream.variant_id()),
            (stream.record_type(), stream.variant_id()),
            DrawingPortSize::Normal,
        );

        for d in variant.data() {
            if let Some(sub_stream) = stream.sub_streams().get(&d) {
                self.output_sub_stream(ports, sub_stream, source);
            }
        }
    }

    fn output_sub_stream(
        &mut self,
        ports: &mut NodePortsBuilder<StreamsPortKey<'a>, StreamsPathKey<'a>>,
        stream: &'a NodeSubStream,
        source: &'a [Box<str>],
    ) {
        let record_definition = &self.graph.record_definitions()[stream.record_type()];
        let variant = &record_definition[stream.variant_id()];

        ports.output(
            (source, stream.record_type(), stream.variant_id()),
            (stream.record_type(), stream.variant_id()),
            DrawingPortSize::Small,
        );

        for d in variant.data() {
            if let Some(sub_stream) = stream.sub_streams().get(&d) {
                self.output_sub_stream(ports, sub_stream, source);
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

pub fn draw_streams(graph: &Graph) -> Drawing {
    let mut visitor = StreamsDrawingVisitor::new(graph);
    visitor.visit_graph(graph);
    visitor.helper.into()
}
