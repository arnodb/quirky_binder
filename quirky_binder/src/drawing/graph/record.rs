use itertools::{EitherOrBoth, Itertools};
use truc::record::definition::DatumId;

use super::{
    super::Graph,
    helper::{drawing_source_node_name, DrawingHelper},
};
use crate::{
    drawing::{
        Drawing, DrawingPort, DrawingPortAlign, DrawingPortSize, DrawingPortsColumn, DynNode,
    },
    graph::visit::{visit_node, Visit},
    prelude::*,
    stream::StreamRecordType,
};

pub type RecordsDrawingHelper<'a> = DrawingHelper<
    'a,
    (&'a [Box<str>], &'a StreamRecordType, DatumId),
    (&'a StreamRecordType, DatumId),
>;

impl<'a> RecordsDrawingHelper<'a> {
    pub fn push_input_sub_stream_port(
        &mut self,
        graph: &'a Graph,
        port_columns: &mut Vec<DrawingPortsColumn>,
        port_count: &mut usize,
        stream: &'a NodeSubStream,
        depth: usize,
    ) {
        let record_definition = &graph.record_definitions()[stream.record_type()];
        let variant = &record_definition[stream.variant_id()];

        for d in variant.data() {
            let input_port_id = *port_count;
            *port_count += 1;
            port_columns.push(DrawingPortsColumn {
                ports: vec![DrawingPort {
                    id: input_port_id,
                    size: if depth <= 1 {
                        DrawingPortSize::Small
                    } else {
                        DrawingPortSize::Dot
                    },
                    redundant: false,
                }],
                align: DrawingPortAlign::Top,
            });

            self.push_edge(
                &(&[], stream.record_type(), d),
                input_port_id,
                (stream.record_type(), d),
            );

            if let Some(sub_stream) = stream.sub_streams().get(&d) {
                self.push_input_sub_stream_port(
                    graph,
                    port_columns,
                    port_count,
                    sub_stream,
                    depth + 1,
                );
            }
        }
    }

    pub fn push_output_sub_stream_port(
        &mut self,
        graph: &'a Graph,
        port_columns: &mut Vec<DrawingPortsColumn>,
        port_count: &mut usize,
        stream: &'a NodeSubStream,
        depth: usize,
    ) {
        let record_definition = &graph.record_definitions()[stream.record_type()];
        let variant = &record_definition[stream.variant_id()];

        for d in variant.data() {
            let output_port_id = *port_count;
            *port_count += 1;
            port_columns.push(DrawingPortsColumn {
                ports: vec![DrawingPort {
                    id: output_port_id,
                    size: if depth <= 1 {
                        DrawingPortSize::Small
                    } else {
                        DrawingPortSize::Dot
                    },
                    redundant: false,
                }],
                align: DrawingPortAlign::Bottom,
            });

            self.output_ports
                .insert((&[], stream.record_type(), d), output_port_id);

            if let Some(sub_stream) = stream.sub_streams().get(&d) {
                self.push_output_sub_stream_port(
                    graph,
                    port_columns,
                    port_count,
                    sub_stream,
                    depth + 1,
                );
            }
        }
    }

    pub fn push_pass_through_sub_stream_ports(
        &mut self,
        graph: &'a Graph,
        port_columns: &mut Vec<DrawingPortsColumn>,
        port_count: &mut usize,
        stream: &'a NodeSubStream,
        depth: usize,
    ) {
        let record_definition = &graph.record_definitions()[stream.record_type()];
        let variant = &record_definition[stream.variant_id()];

        for d in variant.data() {
            let input_port_id = *port_count;
            let output_port_id = *port_count + 1;
            *port_count += 2;
            port_columns.push(DrawingPortsColumn {
                ports: vec![
                    DrawingPort {
                        id: input_port_id,
                        size: if depth <= 1 {
                            DrawingPortSize::Small
                        } else {
                            DrawingPortSize::Dot
                        },
                        redundant: false,
                    },
                    DrawingPort {
                        id: output_port_id,
                        size: if depth <= 1 {
                            DrawingPortSize::Small
                        } else {
                            DrawingPortSize::Dot
                        },
                        redundant: true,
                    },
                ],
                align: DrawingPortAlign::Middle,
            });

            self.push_edge(
                &(&[], stream.record_type(), d),
                input_port_id,
                (stream.record_type(), d),
            );

            self.output_ports
                .insert((&[], stream.record_type(), d), output_port_id);

            if let Some(sub_stream) = stream.sub_streams().get(&d) {
                self.push_pass_through_sub_stream_ports(
                    graph,
                    port_columns,
                    port_count,
                    sub_stream,
                    depth + 1,
                );
            }
        }
    }
}

#[derive(new)]
struct RecordsDrawingVisitor<'a> {
    graph: &'a Graph,
    #[new(default)]
    helper: RecordsDrawingHelper<'a>,
    #[new(value = "0")]
    port_count: usize,
}

impl<'a> RecordsDrawingVisitor<'a> {
    fn input_stream(
        &mut self,
        source: &'a [Box<str>],
        (input, in_d): (&'a NodeStream, DatumId),
        port_columns: &mut Vec<DrawingPortsColumn>,
    ) {
        self.helper.push_input_port(
            port_columns,
            &mut self.port_count,
            &(source, input.record_type(), in_d),
            (input.record_type(), in_d),
        );
        if let Some(sub_stream) = input.sub_streams().get(&in_d) {
            self.helper.push_input_sub_stream_port(
                self.graph,
                port_columns,
                &mut self.port_count,
                sub_stream,
                1,
            );
        }
    }

    fn output_stream(
        &mut self,
        node: &'a dyn DynNode,
        (output, out_d): (&'a NodeStream, DatumId),
        port_columns: &mut Vec<DrawingPortsColumn>,
    ) {
        self.helper.push_output_port(
            port_columns,
            &mut self.port_count,
            (node.name(), output.record_type(), out_d),
        );
        if let Some(sub_stream) = output.sub_streams().get(&out_d) {
            self.helper.push_output_sub_stream_port(
                self.graph,
                port_columns,
                &mut self.port_count,
                sub_stream,
                1,
            );
        }
    }

    fn merged_input_output_stream(
        &mut self,
        node: &'a dyn DynNode,
        source: &'a [Box<str>],
        (input, in_d): (&'a NodeStream, DatumId),
        (output, out_d): (&'a NodeStream, DatumId),
        port_columns: &mut Vec<DrawingPortsColumn>,
    ) {
        self.helper.push_connected_ports(
            port_columns,
            &mut self.port_count,
            &(source, input.record_type(), in_d),
            (input.record_type(), in_d),
            (node.name(), output.record_type(), out_d),
        );
        if let Some(sub_stream) = input.sub_streams().get(&in_d) {
            self.helper.push_pass_through_sub_stream_ports(
                self.graph,
                port_columns,
                &mut self.port_count,
                sub_stream,
                1,
            );
        }
    }

    fn input(&mut self, input: &'a NodeStream, port_columns: &mut Vec<DrawingPortsColumn>) {
        let source = drawing_source_node_name(input);
        let input_record_definition = &self.graph.record_definitions()[input.record_type()];
        let input_variant = &input_record_definition[input.variant_id()];

        for in_d in input_variant.data() {
            self.input_stream(source, (input, in_d), port_columns);
        }
    }

    fn output(
        &mut self,
        node: &'a dyn DynNode,
        output: &'a NodeStream,
        port_columns: &mut Vec<DrawingPortsColumn>,
    ) {
        let output_record_definition = &self.graph.record_definitions()[output.record_type()];
        let output_variant = &output_record_definition[output.variant_id()];

        for out_d in output_variant.data() {
            self.output_stream(node, (output, out_d), port_columns);
        }
    }

    fn merged_input_output(
        &mut self,
        node: &'a dyn DynNode,
        input: &'a NodeStream,
        output: &'a NodeStream,
        port_columns: &mut Vec<DrawingPortsColumn>,
    ) {
        let source = drawing_source_node_name(input);
        let input_record_definition = &self.graph.record_definitions()[input.record_type()];
        let input_variant = &input_record_definition[input.variant_id()];
        let output_record_definition = &self.graph.record_definitions()[output.record_type()];
        let output_variant = &output_record_definition[output.variant_id()];

        for input_output in input_variant
            .data()
            .merge_join_by(output_variant.data(), DatumId::cmp)
        {
            match input_output {
                EitherOrBoth::Both(in_d, out_d) => {
                    self.merged_input_output_stream(
                        node,
                        source,
                        (input, in_d),
                        (output, out_d),
                        port_columns,
                    );
                }
                EitherOrBoth::Left(in_d) => {
                    self.input_stream(source, (input, in_d), port_columns);
                }
                EitherOrBoth::Right(out_d) => {
                    self.output_stream(node, (output, out_d), port_columns);
                }
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

        let mut port_columns = Vec::<DrawingPortsColumn>::new();

        for input_output in node.inputs().iter().zip_longest(node.outputs().iter()) {
            match input_output {
                EitherOrBoth::Both(input, output) => {
                    let merge = input.record_type() == output.record_type();
                    if merge {
                        self.merged_input_output(node, input, output, &mut port_columns);
                    } else {
                        self.input(input, &mut port_columns);
                        self.output(node, output, &mut port_columns);
                    }
                }
                EitherOrBoth::Left(input) => {
                    self.input(input, &mut port_columns);
                }
                EitherOrBoth::Right(output) => {
                    self.output(node, output, &mut port_columns);
                }
            }
        }

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
