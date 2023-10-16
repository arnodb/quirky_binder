use super::{
    Drawing, DrawingColumn, DrawingEdge, DrawingNode, DrawingPort, DrawingPortAlign,
    DrawingPortSize, DrawingPortsColumn,
};
use crate::prelude::*;
use std::collections::{btree_map::Entry, BTreeMap};
use truc::record::definition::{DatumId, RecordVariantId};

const COLORS: [&str; 20] = [
    "#e6194B", "#3cb44b", "#ffe119", "#4363d8", "#f58231", "#911eb4", "#42d4f4", "#f032e6",
    "#bfef45", "#fabed4", "#469990", "#dcbeff", "#9A6324", "#fffac8", "#800000", "#aaffc3",
    "#808000", "#ffd8b1", "#000075", "#a9a9a9",
];

pub struct DrawingHelper<'a, PortKey: Ord, ColorKey: Ord> {
    columns: Vec<DrawingColumn<'a>>,
    node_column_and_index: BTreeMap<&'a [Box<str>], (usize, usize)>,
    output_ports: BTreeMap<PortKey, usize>,
    edges: Vec<DrawingEdge<'a>>,
    edges_color: BTreeMap<ColorKey, &'static str>,
    next_color: usize,
}

impl<'a, PortKey: Ord, ColorKey: Ord> Default for DrawingHelper<'a, PortKey, ColorKey> {
    fn default() -> Self {
        Self {
            columns: Default::default(),
            node_column_and_index: Default::default(),
            output_ports: Default::default(),
            edges: Default::default(),
            edges_color: Default::default(),
            next_color: Default::default(),
        }
    }
}

impl<'a, PortKey: Ord, ColorKey: Ord> DrawingHelper<'a, PortKey, ColorKey> {
    pub fn make_room_for_node(&mut self, node: &dyn DynNode) -> (usize, usize) {
        let col = if let Some(input) = node.inputs().first() {
            let main_source_node_name = Graph::drawing_source_node_name(input);
            let input_col = self
                .node_column_and_index
                .get(main_source_node_name)
                .map(|(col, _)| *col);
            if let Some(input_col) = input_col {
                if input.is_source_main_stream() {
                    input_col
                } else {
                    input_col + 1
                }
            } else {
                self.columns.len()
            }
        } else {
            self.columns.len()
        };
        if col == self.columns.len() {
            self.columns.push(DrawingColumn::default());
        }

        let row = node.inputs().iter().fold(0, |row, input| {
            let source_node_name = Graph::drawing_source_node_name(input);
            let source_col = self
                .node_column_and_index
                .get(source_node_name)
                .map(|(col, _)| *col);
            if let Some(source_col) = source_col {
                row.max(self.columns[source_col].nodes.last().unwrap().row + 1)
            } else {
                row
            }
        });

        if self.columns[col]
            .nodes
            .last()
            .map_or(false, |node| row <= node.row)
        {
            // move all the columns on the right 1 unit farther
            self.columns.insert(col, DrawingColumn::default());
            for (c, _) in self.node_column_and_index.values_mut() {
                if *c >= col {
                    *c += 1;
                }
            }
        }

        assert!(
            row >= self.columns[col]
                .nodes
                .last()
                .map_or(0, |node| node.row + 1)
        );

        (col, row)
    }

    pub fn push_node_into_column(
        &mut self,
        col: usize,
        row: usize,
        node: &'a dyn DynNode,
        port_columns: Vec<DrawingPortsColumn>,
    ) {
        let column = &mut self.columns[col];
        column.nodes.push(DrawingNode {
            label: node.name(),
            port_columns,
            row,
        });

        self.node_column_and_index
            .insert(node.name(), (col, column.nodes.len() - 1));
    }

    fn get_color_for(&mut self, key: ColorKey) -> &'static str {
        match self.edges_color.entry(key) {
            Entry::Vacant(vacant) => {
                let color = COLORS[self.next_color % COLORS.len()];
                self.next_color += 1;
                vacant.insert(color);
                color
            }
            Entry::Occupied(occupied) => occupied.get(),
        }
    }

    pub fn push_input_port(
        &mut self,
        port_columns: &mut Vec<DrawingPortsColumn>,
        port_count: &mut usize,
        from: &PortKey,
        from_color_key: ColorKey,
    ) {
        let input_port_id = *port_count;
        *port_count += 1;

        let mut index = port_columns.len();
        loop {
            if index > 0 && port_columns[index - 1].align == DrawingPortAlign::Bottom {
                index -= 1;
            } else {
                break;
            }
        }

        if index < port_columns.len() {
            let column = &mut port_columns[index];
            column.ports.insert(
                0,
                DrawingPort {
                    id: input_port_id,
                    size: DrawingPortSize::Normal,
                    redundant: false,
                },
            );
            column.align = DrawingPortAlign::Middle;
        } else {
            port_columns.push(DrawingPortsColumn {
                ports: vec![DrawingPort {
                    id: input_port_id,
                    size: DrawingPortSize::Normal,
                    redundant: false,
                }],
                align: DrawingPortAlign::Top,
            });
        }

        self.push_edge(from, input_port_id, from_color_key);
    }

    pub fn push_output_port(
        &mut self,
        port_columns: &mut Vec<DrawingPortsColumn>,
        port_count: &mut usize,
        to: PortKey,
    ) {
        let output_port_id = *port_count;
        *port_count += 1;

        let mut index = port_columns.len();
        loop {
            if index > 0 && port_columns[index - 1].align == DrawingPortAlign::Top {
                index -= 1;
            } else {
                break;
            }
        }

        if index < port_columns.len() {
            let column = &mut port_columns[index];
            column.ports.push(DrawingPort {
                id: output_port_id,
                size: DrawingPortSize::Normal,
                redundant: false,
            });
            column.align = DrawingPortAlign::Middle;
        } else {
            port_columns.push(DrawingPortsColumn {
                ports: vec![DrawingPort {
                    id: output_port_id,
                    size: DrawingPortSize::Normal,
                    redundant: false,
                }],
                align: DrawingPortAlign::Bottom,
            });
        }

        self.output_ports.insert(to, output_port_id);
    }

    pub fn push_connected_ports(
        &mut self,
        port_columns: &mut Vec<DrawingPortsColumn>,
        port_count: &mut usize,
        from: &PortKey,
        from_color_key: ColorKey,
        to: PortKey,
    ) {
        let input_port_id = *port_count;
        let output_port_id = *port_count + 1;
        *port_count += 2;

        port_columns.push(DrawingPortsColumn {
            ports: vec![
                DrawingPort {
                    id: input_port_id,
                    size: DrawingPortSize::Normal,
                    redundant: false,
                },
                DrawingPort {
                    id: output_port_id,
                    size: DrawingPortSize::Normal,
                    redundant: true,
                },
            ],
            align: DrawingPortAlign::Middle,
        });

        self.push_edge(from, input_port_id, from_color_key);

        self.output_ports.insert(to, output_port_id);
    }

    fn push_edge(&mut self, tail_key: &PortKey, head: usize, color_key: ColorKey) {
        let color = self.get_color_for(color_key);
        self.edges.push(DrawingEdge {
            tail: self.output_ports[tail_key],
            head,
            color,
        });
    }
}

pub type StreamsDrawingHelper<'a> = DrawingHelper<
    'a,
    (&'a [Box<str>], &'a StreamRecordType, RecordVariantId),
    (&'a StreamRecordType, RecordVariantId),
>;

impl<'a> StreamsDrawingHelper<'a> {
    pub fn push_input_sub_stream_port(
        &mut self,
        graph: &'a Graph,
        port_columns: &mut Vec<DrawingPortsColumn>,
        port_count: &mut usize,
        stream: &'a NodeStream,
        depth: usize,
    ) {
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
            &(
                &***stream.source(),
                stream.record_type(),
                stream.variant_id(),
            ),
            input_port_id,
            (stream.record_type(), stream.variant_id()),
        );

        let record_definition = &graph.record_definitions()[stream.record_type()];
        let variant = &record_definition[stream.variant_id()];

        for d in variant.data() {
            if let Some(sub_stream) = graph.get_sub_stream(stream.record_type().clone(), d) {
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
        stream: &'a NodeStream,
        depth: usize,
    ) {
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

        self.output_ports.insert(
            (stream.source(), stream.record_type(), stream.variant_id()),
            output_port_id,
        );

        let record_definition = &graph.record_definitions()[stream.record_type()];
        let variant = &record_definition[stream.variant_id()];

        for d in variant.data() {
            if let Some(sub_stream) = graph.get_sub_stream(stream.record_type().clone(), d) {
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
        stream: &'a NodeStream,
        depth: usize,
    ) {
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
            &(
                &***stream.source(),
                stream.record_type(),
                stream.variant_id(),
            ),
            input_port_id,
            (stream.record_type(), stream.variant_id()),
        );

        self.output_ports.insert(
            (stream.source(), stream.record_type(), stream.variant_id()),
            output_port_id,
        );

        let record_definition = &graph.record_definitions()[stream.record_type()];
        let variant = &record_definition[stream.variant_id()];

        for d in variant.data() {
            if let Some(sub_stream) = graph.get_sub_stream(stream.record_type().clone(), d) {
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
        stream: &'a NodeStream,
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
                &(&***stream.source(), stream.record_type(), d),
                input_port_id,
                (stream.record_type(), d),
            );

            if let Some(sub_stream) = graph.get_sub_stream(stream.record_type().clone(), d) {
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
        stream: &'a NodeStream,
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
                .insert((stream.source(), stream.record_type(), d), output_port_id);

            if let Some(sub_stream) = graph.get_sub_stream(stream.record_type().clone(), d) {
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
        stream: &'a NodeStream,
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
                &(&***stream.source(), stream.record_type(), d),
                input_port_id,
                (stream.record_type(), d),
            );

            self.output_ports
                .insert((stream.source(), stream.record_type(), d), output_port_id);

            if let Some(sub_stream) = graph.get_sub_stream(stream.record_type().clone(), d) {
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

#[allow(clippy::from_over_into)]
impl<'a, PortKey: Ord, ColorKey: Ord> Into<Drawing<'a>> for DrawingHelper<'a, PortKey, ColorKey> {
    fn into(self) -> Drawing<'a> {
        Drawing {
            columns: self.columns,
            edges: self.edges,
        }
    }
}
