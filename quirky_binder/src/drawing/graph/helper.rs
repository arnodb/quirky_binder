use std::{
    collections::{btree_map::Entry, BTreeMap},
    fmt::Debug,
};

use itertools::{EitherOrBoth, Itertools};

use crate::{
    drawing::{
        Drawing, DrawingColumn, DrawingEdge, DrawingNode, DrawingPort, DrawingPortAlign,
        DrawingPortId, DrawingPortSize, DrawingPortsColumn,
    },
    prelude::*,
};

pub fn drawing_source_node_name(input: &NodeStream) -> &[Box<str>] {
    let source = input.source();
    if input.is_source_main_stream() {
        source
    } else {
        &source[0..source.len() - 1]
    }
}

const COLORS: [&str; 20] = [
    "#e6194B", "#3cb44b", "#ffe119", "#4363d8", "#f58231", "#911eb4", "#42d4f4", "#f032e6",
    "#bfef45", "#fabed4", "#469990", "#dcbeff", "#9A6324", "#fffac8", "#800000", "#aaffc3",
    "#808000", "#ffd8b1", "#000075", "#a9a9a9",
];

pub struct DrawingHelper<'a, PortKey: Ord + Debug, PathKey: Ord + Debug> {
    pub columns: Vec<DrawingColumn<'a>>,
    pub node_column_and_index: BTreeMap<&'a [Box<str>], (usize, usize)>,
    pub port_count: usize,
    pub output_ports: BTreeMap<PortKey, DrawingPortId>,
    pub edges: Vec<DrawingEdge<'a>>,
    pub edges_color: BTreeMap<PathKey, &'static str>,
    pub next_color: usize,
}

impl<PortKey: Ord + Debug, PathKey: Ord + Debug> Default for DrawingHelper<'_, PortKey, PathKey> {
    fn default() -> Self {
        Self {
            columns: Default::default(),
            node_column_and_index: Default::default(),
            port_count: Default::default(),
            output_ports: Default::default(),
            edges: Default::default(),
            edges_color: Default::default(),
            next_color: Default::default(),
        }
    }
}

impl<'a, PortKey: Ord + Copy + Debug, PathKey: Ord + Copy + Debug>
    DrawingHelper<'a, PortKey, PathKey>
{
    pub fn make_room_for_node(&mut self, node: &dyn DynNode) -> (usize, usize) {
        let col = if let Some(input) = node.inputs().first() {
            let main_source_node_name = drawing_source_node_name(input);
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
            let source_node_name = drawing_source_node_name(input);
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
            .is_some_and(|node| row <= node.row)
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

    fn get_color_for(&mut self, key: PathKey) -> &'static str {
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

    pub fn push_edge(&mut self, tail_key: &PortKey, head: DrawingPortId, color_key: PathKey) {
        let color = self.get_color_for(color_key);
        self.edges.push(DrawingEdge {
            tail: self.output_ports[tail_key],
            head,
            color,
        });
    }
}

#[allow(clippy::from_over_into)]
impl<'a, PortKey: Ord + Debug, PathKey: Ord + Debug> Into<Drawing<'a>>
    for DrawingHelper<'a, PortKey, PathKey>
{
    fn into(self) -> Drawing<'a> {
        Drawing {
            columns: self.columns,
            edges: self.edges,
        }
    }
}

enum NodePortColumn<PortKey, PathKey> {
    Input {
        path: PathKey,
        port_size: DrawingPortSize,
        edge_tail: PortKey,
    },
    Output {
        port_key: PortKey,
        path: PathKey,
        port_size: DrawingPortSize,
    },
    ConnectedInputOutput {
        path: PathKey,
        port_size: DrawingPortSize,
        edge_tail: PortKey,
        output_port_key: PortKey,
    },
    DisconnectedInputOutput {
        input_path: PathKey,
        input_port_size: DrawingPortSize,
        edge_tail: PortKey,
        output_port_key: PortKey,
        output_path: PathKey,
        output_port_size: DrawingPortSize,
    },
    None,
}

pub struct NodePortsBuilder<PortKey: Ord + Copy + Debug, PathKey: Ord + Copy + Debug> {
    columns: Vec<NodePortColumn<PortKey, PathKey>>,
}

impl<PortKey: Ord + Copy + Debug, PathKey: Ord + Copy + Debug> Default
    for NodePortsBuilder<PortKey, PathKey>
{
    fn default() -> Self {
        NodePortsBuilder {
            columns: Default::default(),
        }
    }
}

impl<PortKey: Ord + Copy + Debug, PathKey: Eq + Ord + Copy + Debug>
    NodePortsBuilder<PortKey, PathKey>
{
    pub fn input(
        &mut self,
        edge_tail: PortKey,
        input_path: PathKey,
        input_port_size: DrawingPortSize,
    ) {
        let mut i = 0;
        while i < self.columns.len() {
            let col = &mut self.columns[i];
            match *col {
                NodePortColumn::Output {
                    port_key: output_port_key,
                    path: output_path,
                    port_size: output_port_size,
                } => {
                    if output_path == input_path {
                        *col = NodePortColumn::ConnectedInputOutput {
                            path: input_path,
                            port_size: input_port_size,
                            edge_tail,
                            output_port_key,
                        };
                    } else {
                        *col = NodePortColumn::DisconnectedInputOutput {
                            input_path,
                            input_port_size,
                            edge_tail,
                            output_port_key,
                            output_path,
                            output_port_size,
                        };
                    }
                    break;
                }
                NodePortColumn::Input { .. }
                | NodePortColumn::ConnectedInputOutput { .. }
                | NodePortColumn::DisconnectedInputOutput { .. }
                | NodePortColumn::None => {}
            }
            i += 1;
        }
        if i >= self.columns.len() {
            self.columns.push(NodePortColumn::Input {
                path: input_path,
                port_size: input_port_size,
                edge_tail,
            });
        }
    }

    pub fn output(
        &mut self,
        output_port_key: PortKey,
        output_path: PathKey,
        output_port_size: DrawingPortSize,
    ) {
        let mut i = 0;
        while i < self.columns.len() {
            let col = &mut self.columns[i];
            match *col {
                NodePortColumn::Input {
                    path: input_path,
                    port_size: input_port_size,
                    edge_tail,
                } => {
                    if input_path == output_path {
                        *col = NodePortColumn::ConnectedInputOutput {
                            path: input_path,
                            port_size: input_port_size,
                            edge_tail,
                            output_port_key,
                        };
                    } else {
                        *col = NodePortColumn::DisconnectedInputOutput {
                            input_path,
                            input_port_size,
                            edge_tail,
                            output_port_key,
                            output_path,
                            output_port_size,
                        };
                    }
                    break;
                }
                NodePortColumn::Output { .. }
                | NodePortColumn::ConnectedInputOutput { .. }
                | NodePortColumn::DisconnectedInputOutput { .. }
                | NodePortColumn::None => {}
            }
            i += 1;
        }
        if i >= self.columns.len() {
            self.columns.push(NodePortColumn::Output {
                port_key: output_port_key,
                path: output_path,
                port_size: output_port_size,
            });
        }
    }

    fn consolidate_slice_from(&mut self, from: usize) -> usize {
        let mut inputs = Vec::new();
        let mut outputs = Vec::new();
        let mut i = from;
        while i < self.columns.len() {
            match &self.columns[i] {
                NodePortColumn::Input { .. } => {
                    inputs.push(i);
                }
                NodePortColumn::Output { .. } => {
                    outputs.push(i);
                }
                NodePortColumn::ConnectedInputOutput { .. } => {
                    break;
                }
                NodePortColumn::DisconnectedInputOutput { .. } => {
                    // Should not happen but let's have a sensible behaviour
                    inputs.push(i);
                    outputs.push(i);
                }
                NodePortColumn::None => {}
            }
            i += 1;
        }
        let mut a = from;
        for input_output in inputs.into_iter().zip_longest(outputs.into_iter()) {
            match input_output {
                EitherOrBoth::Both(input, output) => {
                    let (input_path, input_port_size, edge_tail) = match &self.columns[input] {
                        NodePortColumn::Input {
                            path,
                            port_size,
                            edge_tail,
                        } => (*path, *port_size, *edge_tail),
                        NodePortColumn::DisconnectedInputOutput {
                            input_path,
                            input_port_size,
                            edge_tail,
                            ..
                        } => (*input_path, *input_port_size, *edge_tail),
                        _ => unreachable!(),
                    };
                    let (output_port_key, output_path, output_port_size) =
                        match &self.columns[output] {
                            NodePortColumn::Output {
                                port_key,
                                path,
                                port_size,
                            } => (*port_key, *path, *port_size),
                            NodePortColumn::DisconnectedInputOutput {
                                output_port_key,
                                output_path,
                                output_port_size,
                                ..
                            } => (*output_port_key, *output_path, *output_port_size),
                            _ => unreachable!(),
                        };
                    self.columns[a] = NodePortColumn::DisconnectedInputOutput {
                        input_path,
                        input_port_size,
                        edge_tail,
                        output_port_key,
                        output_path,
                        output_port_size,
                    };
                }
                EitherOrBoth::Left(input) => {
                    let (path, port_size, edge_tail) = match &self.columns[input] {
                        NodePortColumn::Input {
                            path,
                            port_size,
                            edge_tail,
                        } => (*path, *port_size, *edge_tail),
                        NodePortColumn::DisconnectedInputOutput {
                            input_path,
                            input_port_size,
                            edge_tail,
                            ..
                        } => (*input_path, *input_port_size, *edge_tail),
                        _ => unreachable!(),
                    };
                    self.columns[a] = NodePortColumn::Input {
                        path,
                        port_size,
                        edge_tail,
                    };
                }
                EitherOrBoth::Right(output) => {
                    let (port_key, path, port_size) = match &self.columns[output] {
                        NodePortColumn::Output {
                            port_key,
                            path,
                            port_size,
                        } => (*port_key, *path, *port_size),
                        NodePortColumn::DisconnectedInputOutput {
                            output_port_key,
                            output_path,
                            output_port_size,
                            ..
                        } => (*output_port_key, *output_path, *output_port_size),
                        _ => unreachable!(),
                    };
                    self.columns[a] = NodePortColumn::Output {
                        port_key,
                        path,
                        port_size,
                    };
                }
            }
            a += 1;
        }
        while a < i {
            self.columns[a] = NodePortColumn::None;
            a += 1;
        }
        i
    }

    fn consolidate(&mut self) {
        let mut i = 0;
        while i < self.columns.len() {
            match &self.columns[i] {
                NodePortColumn::Input { .. }
                | NodePortColumn::Output { .. }
                | NodePortColumn::None => {
                    i = self.consolidate_slice_from(i);
                }
                NodePortColumn::ConnectedInputOutput { .. }
                | NodePortColumn::DisconnectedInputOutput { .. } => {
                    i += 1;
                }
            }
        }
    }

    pub fn build(
        mut self,
        helper: &mut DrawingHelper<PortKey, PathKey>,
    ) -> Vec<DrawingPortsColumn> {
        self.consolidate();
        self.columns
            .into_iter()
            .filter_map(|col| match col {
                NodePortColumn::Input {
                    path,
                    port_size,
                    edge_tail,
                } => {
                    let port_id = DrawingPortId::from(helper.port_count);
                    helper.port_count += 1;
                    helper.push_edge(&edge_tail, port_id, path);
                    Some(DrawingPortsColumn {
                        ports: vec![DrawingPort {
                            id: port_id,
                            size: port_size,
                        }],
                        align: DrawingPortAlign::Top,
                    })
                }
                NodePortColumn::Output {
                    port_key,
                    path: _,
                    port_size,
                } => {
                    let port_id = DrawingPortId::from(helper.port_count);
                    helper.port_count += 1;
                    let old = helper.output_ports.insert(port_key, port_id);
                    assert!(old.is_none());
                    Some(DrawingPortsColumn {
                        ports: vec![DrawingPort {
                            id: port_id,
                            size: port_size,
                        }],
                        align: DrawingPortAlign::Bottom,
                    })
                }
                NodePortColumn::ConnectedInputOutput {
                    path,
                    port_size,
                    edge_tail,
                    output_port_key,
                } => {
                    let port_id = DrawingPortId::from(helper.port_count);
                    helper.port_count += 1;
                    helper.push_edge(&edge_tail, port_id, path);
                    let old = helper.output_ports.insert(output_port_key, port_id);
                    assert!(old.is_none());
                    Some(DrawingPortsColumn {
                        ports: vec![DrawingPort {
                            id: port_id,
                            size: port_size,
                        }],
                        align: DrawingPortAlign::Middle,
                    })
                }
                NodePortColumn::DisconnectedInputOutput {
                    input_path,
                    input_port_size,
                    edge_tail,
                    output_port_key,
                    output_path: _,
                    output_port_size,
                } => {
                    let input_port_id = DrawingPortId::from(helper.port_count);
                    let output_port_id = DrawingPortId::from(helper.port_count + 1);
                    helper.port_count += 2;
                    helper.push_edge(&edge_tail, input_port_id, input_path);
                    let old = helper.output_ports.insert(output_port_key, output_port_id);
                    assert!(old.is_none());
                    Some(DrawingPortsColumn {
                        ports: vec![
                            DrawingPort {
                                id: input_port_id,
                                size: input_port_size,
                            },
                            DrawingPort {
                                id: output_port_id,
                                size: output_port_size,
                            },
                        ],
                        align: DrawingPortAlign::Middle,
                    })
                }
                NodePortColumn::None => None,
            })
            .collect()
    }
}
