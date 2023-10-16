use crate::prelude::*;
use std::collections::BTreeMap;

pub mod graph;

#[derive(Debug)]
pub struct Drawing<'a> {
    pub columns: Vec<DrawingColumn<'a>>,
    pub edges: Vec<DrawingEdge<'a>>,
}

#[derive(Default, Debug)]
pub struct DrawingColumn<'a> {
    pub nodes: Vec<DrawingNode<'a>>,
}

#[derive(Debug)]
pub struct DrawingNode<'a> {
    pub label: &'a FullyQualifiedName,
    pub row: usize,
    pub port_columns: Vec<DrawingPortsColumn>,
}

#[derive(Debug)]
pub struct DrawingPortsColumn {
    pub ports: Vec<DrawingPort>,
    pub align: DrawingPortAlign,
}

#[derive(Debug)]
pub struct DrawingPort {
    pub id: usize,
    pub size: DrawingPortSize,
    pub redundant: bool,
}

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum DrawingPortSize {
    Normal,
    Small,
    Dot,
}

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum DrawingPortAlign {
    Top,
    Middle,
    Bottom,
}

#[derive(Debug)]
pub struct DrawingEdge<'a> {
    pub tail: usize,
    pub head: usize,
    pub color: &'a str,
}

pub fn format_text<W>(mut w: W, drawing: &Drawing) -> std::fmt::Result
where
    W: std::fmt::Write,
{
    let Drawing { columns, edges: _ } = drawing;

    let mut iterators = columns
        .iter()
        .map(|column| Some(column.nodes.iter().peekable()))
        .collect::<Vec<_>>();

    let columns_max_port_count = columns
        .iter()
        .map(|column| {
            column
                .nodes
                .iter()
                .map(|node| node.port_columns.len())
                .reduce(usize::max)
                .unwrap_or(0)
        })
        .collect::<Vec<usize>>();

    #[allow(clippy::manual_flatten)]
    for maybe_iter in &mut iterators {
        if let Some(iter) = maybe_iter {
            if iter.peek().is_none() {
                *maybe_iter = None;
            }
        }
    }

    loop {
        let next_row = iterators
            .iter_mut()
            .fold(None::<usize>, |next_row, maybe_iter| {
                if let Some(iter) = maybe_iter {
                    if let Some(node) = iter.peek() {
                        next_row.map_or(Some(node.row), |next_row| Some(next_row.min(node.row)))
                    } else {
                        *maybe_iter = None;
                        next_row
                    }
                } else {
                    next_row
                }
            });
        let next_row = if let Some(next_row) = next_row {
            next_row
        } else {
            break;
        };
        let mut max_port_rows = 1;
        let mut nodes = iterators
            .iter_mut()
            .map(|maybe_iter| {
                if let Some(iter) = maybe_iter {
                    if let Some(&node) = iter.peek() {
                        if node.row == next_row {
                            iter.next();
                            let port_rows = node
                                .port_columns
                                .iter()
                                .map(|column| column.ports.len())
                                .reduce(usize::max)
                                .unwrap_or(0);
                            if port_rows > max_port_rows {
                                max_port_rows = port_rows;
                            }
                            return Some(node);
                        }
                    } else {
                        *maybe_iter = None;
                    }
                }
                None
            })
            .collect::<Vec<Option<&DrawingNode>>>();
        while let Some(None) = nodes.last() {
            nodes.pop();
        }
        for (col, node) in nodes.iter().enumerate() {
            if let Some(node) = node {
                write!(w, "{: >20}", node.label.last().unwrap())?;
                write!(w, " ")?;
                let count = node.port_columns.len();
                for c in 0..count {
                    if !node.port_columns[c].ports.is_empty() {
                        write!(w, "*")?;
                    } else {
                        write!(w, " ")?;
                    }
                }
                for _ in count..(columns_max_port_count[col] + 1) {
                    write!(w, " ")?;
                }
            } else {
                write!(w, "{: >20}", "")?;
                for _ in 0..(columns_max_port_count[col] + 2) {
                    write!(w, " ")?;
                }
            }
        }
        writeln!(w)?;
        for port_row in 1..max_port_rows {
            for (col, node) in nodes.iter().enumerate() {
                write!(w, "{: >20}", "")?;
                if let Some(node) = node {
                    write!(w, " ")?;
                    let count = node.port_columns.len();
                    for c in 0..count {
                        if node.port_columns[c].ports.len() > port_row {
                            write!(w, "*")?;
                        } else {
                            write!(w, " ")?;
                        }
                    }
                    for _ in count..(columns_max_port_count[col] + 1) {
                        write!(w, " ")?;
                    }
                } else {
                    for _ in 0..(columns_max_port_count[col] + 2) {
                        write!(w, " ")?;
                    }
                }
            }
            writeln!(w)?;
        }
        writeln!(w)?;
    }

    Ok(())
}

pub fn format_svg<W>(mut w: W, drawing: &Drawing) -> std::fmt::Result
where
    W: std::fmt::Write,
{
    let Drawing { columns, edges } = drawing;

    let columns_max_port_count = columns
        .iter()
        .map(|column| {
            column
                .nodes
                .iter()
                .map(|node| node.port_columns.len())
                .reduce(usize::max)
                .unwrap_or(0)
        })
        .collect::<Vec<usize>>();

    writeln!(
        w,
        "<svg xmlns=\"http://www.w3.org/2000/svg\" xmlns:xlink=\"http://www.w3.org/1999/xlink\">"
    )?;

    const NODE_HEIGHT_WITH_MARGINS: usize = 50;
    const NODE_HEIGHT: usize = 30;
    const LABEL_MAX_WIDTH: usize = 200;
    const BLANK_WIDTH: usize = 0;
    const PORT_RADIUS: usize = 4;
    const PORT_STROKE_WIDTH: usize = 2;
    const SMALL_PORT_RADIUS: usize = 2;
    const DOT_PORT_RADIUS: usize = 1;
    const PORT_X_DISTANCE: usize = 12;
    const PORT_Y_DISTANCE: usize = 12;
    const ARC_RADIUS: usize = 8;

    #[derive(Debug)]
    struct SvgPort {
        row: usize,
        cx: usize,
        cy: usize,
        size: DrawingPortSize,
        redundant: bool,
    }

    impl SvgPort {
        fn radius(&self) -> usize {
            match self.size {
                DrawingPortSize::Normal => PORT_RADIUS,
                DrawingPortSize::Small => SMALL_PORT_RADIUS,
                DrawingPortSize::Dot => DOT_PORT_RADIUS,
            }
        }
    }

    let mut svg_ports = BTreeMap::<usize, SvgPort>::new();

    let mut ports_count = 0;

    for (col, column) in columns.iter().enumerate() {
        for DrawingNode {
            label: _,
            row,
            port_columns,
        } in &column.nodes
        {
            let columns_real_rows = port_columns
                .iter()
                .map(|DrawingPortsColumn { ports, align: _ }| {
                    ports
                        .iter()
                        .fold(0, |count, port| count + if port.redundant { 0 } else { 1 })
                })
                .collect::<Vec<usize>>();
            let max_real_rows = columns_real_rows
                .iter()
                .copied()
                .reduce(usize::max)
                .unwrap_or(0);
            for (port_col, DrawingPortsColumn { ports, align }) in port_columns.iter().enumerate() {
                let mut top = match align {
                    DrawingPortAlign::Top => {
                        row * NODE_HEIGHT_WITH_MARGINS + NODE_HEIGHT_WITH_MARGINS / 2
                            - (max_real_rows - 1) * PORT_Y_DISTANCE / 2
                    }
                    DrawingPortAlign::Middle => {
                        row * NODE_HEIGHT_WITH_MARGINS + NODE_HEIGHT_WITH_MARGINS / 2
                            - (columns_real_rows[port_col] - 1) * PORT_Y_DISTANCE / 2
                    }
                    DrawingPortAlign::Bottom => {
                        row * NODE_HEIGHT_WITH_MARGINS
                            + NODE_HEIGHT_WITH_MARGINS / 2
                            + (max_real_rows - 1) * PORT_Y_DISTANCE / 2
                            - (columns_real_rows[port_col] - 1) * PORT_Y_DISTANCE
                    }
                };
                let mut go_down_next = false;
                for DrawingPort {
                    id,
                    size,
                    redundant,
                } in ports
                {
                    if !redundant {
                        if go_down_next {
                            top += PORT_Y_DISTANCE;
                        }
                        go_down_next = true;
                    } else {
                        go_down_next = false;
                    }
                    svg_ports.insert(
                        *id,
                        SvgPort {
                            row: *row,
                            cx: LABEL_MAX_WIDTH
                                + col * (LABEL_MAX_WIDTH + BLANK_WIDTH)
                                + (ports_count + port_col) * PORT_X_DISTANCE,
                            cy: top,
                            size: *size,
                            redundant: *redundant,
                        },
                    );
                }
            }
        }

        ports_count += columns_max_port_count[col];
    }

    for DrawingEdge { tail, head, color } in edges {
        let tail = &svg_ports[tail];
        let head = &svg_ports[head];
        write!(w, "<path d=\"M{},{} ", tail.cx, tail.cy + tail.radius())?;
        if tail.cx != head.cx {
            let to_right = tail.cx < head.cx;
            let arc_radius = if to_right {
                (head.cx - tail.cx) / 2
            } else {
                (tail.cx - head.cx) / 2
            }
            .min(ARC_RADIUS);
            write!(
                w,
                "V{} ",
                (tail.row + head.row + 1) * NODE_HEIGHT_WITH_MARGINS / 2 - arc_radius
            )?;
            write!(
                w,
                "A{},{} 0 0,{} {},{}",
                arc_radius,
                arc_radius,
                if to_right { 0 } else { 1 },
                if to_right {
                    tail.cx + arc_radius
                } else {
                    tail.cx - arc_radius
                },
                (tail.row + head.row + 1) * NODE_HEIGHT_WITH_MARGINS / 2
            )?;
            write!(
                w,
                "H{} ",
                if to_right {
                    head.cx - arc_radius
                } else {
                    head.cx + arc_radius
                }
            )?;
            write!(
                w,
                "A{},{} 0 0,{} {},{}",
                arc_radius,
                arc_radius,
                if to_right { 1 } else { 0 },
                head.cx,
                (tail.row + head.row + 1) * NODE_HEIGHT_WITH_MARGINS / 2 + arc_radius
            )?;
        }
        write!(w, "V{}", head.cy - head.radius())?;
        writeln!(
            w,
            "\" style=\"stroke:{};stroke-width:3;fill:none;\" />",
            color
        )?;
    }

    fn write_stream_port<W>(mut w: W, port: &SvgPort) -> std::fmt::Result
    where
        W: std::fmt::Write,
    {
        write!(w, "<circle cx=\"{}\" cy=\"{}\" ", port.cx, port.cy)?;
        write!(w, "r=\"{}\" ", port.radius())?;
        writeln!(
            w,
            "style=\"stroke:#000000;stroke-width:{};fill:none;\" />",
            PORT_STROKE_WIDTH
        )?;
        Ok(())
    }

    for port in svg_ports.values().filter(|port| !port.redundant) {
        write_stream_port(&mut w, port)?;
    }

    let mut ports_count = 0;

    for (col, column) in columns.iter().enumerate() {
        for DrawingNode {
            label,
            row,
            port_columns: _,
        } in &column.nodes
        {
            write!(
                w,
                "<rect x=\"{}\" y=\"{}\" width=\"{}\" height=\"{}\" ",
                10 + col * (LABEL_MAX_WIDTH + BLANK_WIDTH) + ports_count * PORT_X_DISTANCE,
                row * NODE_HEIGHT_WITH_MARGINS + NODE_HEIGHT_WITH_MARGINS / 2 - NODE_HEIGHT / 2,
                LABEL_MAX_WIDTH - 10 + columns_max_port_count[col] * PORT_X_DISTANCE,
                NODE_HEIGHT
            )?;
            writeln!(w, "style=\"stroke:#000000;stroke-width:1;fill:none;\" />")?;

            write!(
                w,
                "<text x=\"{}\" y=\"{}\" ",
                LABEL_MAX_WIDTH
                    + col * (LABEL_MAX_WIDTH + BLANK_WIDTH)
                    + ports_count * PORT_X_DISTANCE
                    - 10,
                row * NODE_HEIGHT_WITH_MARGINS + NODE_HEIGHT_WITH_MARGINS / 2 + 4
            )?;
            writeln!(w, "style=\"text-anchor:end;font-size:12px;")?;
            writeln!(w, "font-family:helvetica;\">")?;
            writeln!(w, "{}", label.last().unwrap())?;
            writeln!(w, "</text>",)?;
        }

        ports_count += columns_max_port_count[col];
    }

    write!(w, "</svg>")?;

    Ok(())
}
