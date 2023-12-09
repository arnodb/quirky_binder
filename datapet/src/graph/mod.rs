use crate::{
    drawing::{
        format_svg,
        graph::{RecordsDrawingHelper, StreamsDrawingHelper},
        Drawing, DrawingPortsColumn,
    },
    prelude::*,
};
use codegen::Scope;
use itertools::{EitherOrBoth, Itertools};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    path::Path,
};
use truc::record::definition::{DatumId, RecordDefinition};

pub mod builder;
pub mod node;

pub struct Graph {
    chain_customizer: ChainCustomizer,
    record_definitions: BTreeMap<StreamRecordType, RecordDefinition>,
    entry_nodes: Vec<Box<dyn DynNode>>,
}

impl Graph {
    pub fn chain_customizer(&self) -> &ChainCustomizer {
        &self.chain_customizer
    }

    pub fn record_definitions(&self) -> &BTreeMap<StreamRecordType, RecordDefinition> {
        &self.record_definitions
    }

    pub fn generate(&self, output: &Path) -> Result<(), std::io::Error> {
        use std::io::Write;

        {
            let mut file = File::create(&output.join("streams.rs")).unwrap();
            let mut scope = Scope::new();
            for (record_type, definition) in &self.record_definitions {
                let module = scope.get_or_new_module(&record_type[0]).vis("pub");
                let module = record_type
                    .iter()
                    .skip(1)
                    .fold(module, |m, n| m.get_or_new_module(n).vis("pub"))
                    .scope();
                module.raw(&truc::generator::generate(definition));
            }
            write!(file, "{}", scope.to_string()).unwrap();
        }
        rustfmt_generated_file(output.join("streams.rs").as_path());

        {
            let mut scope = Scope::new();
            scope.import("fallible_iterator", "FallibleIterator");
            scope.import(
                &self.chain_customizer.error_type_path(),
                &self.chain_customizer.error_type_name(),
            );
            for (path, ty) in &self.chain_customizer.custom_module_imports {
                scope.import(path, ty);
            }

            scope.raw("mod streams;");

            let mut chain = Chain::new(&self.chain_customizer, &mut scope);

            for node in &self.entry_nodes {
                node.gen_chain(self, &mut chain);
            }

            chain.gen_chain();

            let mut file = File::create(output.join("chain.rs")).unwrap();
            write!(file, "{}", scope.to_string()).unwrap();
        }
        rustfmt_generated_file(output.join("chain.rs").as_path());

        Ok(())
    }

    fn streams_to_abstract_drawing(&self) -> Drawing {
        let mut helper = StreamsDrawingHelper::default();

        let mut port_count = 0;

        for node in self
            .entry_nodes
            .iter()
            .flat_map(|entry_node| entry_node.all_nodes())
        {
            let (col, row) = helper.make_room_for_node(node);

            let mut port_columns = Vec::new();

            for input_output in node.inputs().iter().zip_longest(node.outputs().iter()) {
                match input_output {
                    EitherOrBoth::Both(input, output) => {
                        let source = Self::drawing_source_node_name(input);
                        let input_record_definition = &self.record_definitions[input.record_type()];
                        let input_variant = &input_record_definition[input.variant_id()];
                        let output_record_definition =
                            &self.record_definitions[output.record_type()];
                        let output_variant = &output_record_definition[output.variant_id()];

                        let merge = input.record_type() == output.record_type()
                            && input.variant_id() == output.variant_id();

                        if merge {
                            helper.push_connected_ports(
                                &mut port_columns,
                                &mut port_count,
                                &(source, input.record_type(), input.variant_id()),
                                (input.record_type(), input.variant_id()),
                                (node.name(), output.record_type(), output.variant_id()),
                            );
                            for input_output in input_variant
                                .data()
                                .merge_join_by(output_variant.data(), DatumId::cmp)
                            {
                                match input_output {
                                    EitherOrBoth::Both(in_d, _out_d) => {
                                        if let Some(sub_stream) = input.sub_streams().get(&in_d) {
                                            helper.push_pass_through_sub_stream_ports(
                                                self,
                                                &mut port_columns,
                                                &mut port_count,
                                                sub_stream,
                                                1,
                                            );
                                        }
                                    }
                                    EitherOrBoth::Left(in_d) => {
                                        if let Some(sub_stream) = input.sub_streams().get(&in_d) {
                                            helper.push_input_sub_stream_port(
                                                self,
                                                &mut port_columns,
                                                &mut port_count,
                                                sub_stream,
                                                1,
                                            );
                                        }
                                    }
                                    EitherOrBoth::Right(out_d) => {
                                        if let Some(sub_stream) = output.sub_streams().get(&out_d) {
                                            helper.push_output_sub_stream_port(
                                                self,
                                                &mut port_columns,
                                                &mut port_count,
                                                sub_stream,
                                                1,
                                            );
                                        }
                                    }
                                }
                            }
                        } else {
                            helper.push_input_port(
                                &mut port_columns,
                                &mut port_count,
                                &(source, input.record_type(), input.variant_id()),
                                (input.record_type(), input.variant_id()),
                            );

                            helper.push_output_port(
                                &mut port_columns,
                                &mut port_count,
                                (node.name(), output.record_type(), output.variant_id()),
                            );

                            for in_d in input_variant.data() {
                                if let Some(sub_stream) = input.sub_streams().get(&in_d) {
                                    helper.push_input_sub_stream_port(
                                        self,
                                        &mut port_columns,
                                        &mut port_count,
                                        sub_stream,
                                        1,
                                    );
                                }
                            }

                            for out_d in output_variant.data() {
                                if let Some(sub_stream) = output.sub_streams().get(&out_d) {
                                    helper.push_output_sub_stream_port(
                                        self,
                                        &mut port_columns,
                                        &mut port_count,
                                        sub_stream,
                                        1,
                                    );
                                }
                            }
                        };
                    }
                    EitherOrBoth::Left(input) => {
                        let source = Self::drawing_source_node_name(input);
                        let input_record_definition = &self.record_definitions[input.record_type()];
                        let input_variant = &input_record_definition[input.variant_id()];

                        helper.push_input_port(
                            &mut port_columns,
                            &mut port_count,
                            &(source, input.record_type(), input.variant_id()),
                            (input.record_type(), input.variant_id()),
                        );

                        for in_d in input_variant.data() {
                            if let Some(sub_stream) = input.sub_streams().get(&in_d) {
                                helper.push_input_sub_stream_port(
                                    self,
                                    &mut port_columns,
                                    &mut port_count,
                                    sub_stream,
                                    1,
                                );
                            }
                        }
                    }
                    EitherOrBoth::Right(output) => {
                        let output_record_definition =
                            &self.record_definitions[output.record_type()];
                        let output_variant = &output_record_definition[output.variant_id()];

                        helper.push_output_port(
                            &mut port_columns,
                            &mut port_count,
                            (node.name(), output.record_type(), output.variant_id()),
                        );

                        for out_d in output_variant.data() {
                            if let Some(sub_stream) = output.sub_streams().get(&out_d) {
                                helper.push_output_sub_stream_port(
                                    self,
                                    &mut port_columns,
                                    &mut port_count,
                                    sub_stream,
                                    1,
                                );
                            }
                        }
                    }
                }
            }

            helper.push_node_into_column(col, row, node, port_columns)
        }

        helper.into()
    }

    fn records_to_abstract_drawing(&self) -> Drawing {
        let mut helper = RecordsDrawingHelper::default();

        let mut port_count = 0;

        for node in self
            .entry_nodes
            .iter()
            .flat_map(|entry_node| entry_node.all_nodes())
        {
            let (col, row) = helper.make_room_for_node(node);

            let mut port_columns = Vec::<DrawingPortsColumn>::new();

            for input_output in node.inputs().iter().zip_longest(node.outputs().iter()) {
                match input_output {
                    EitherOrBoth::Both(input, output) => {
                        let source = Self::drawing_source_node_name(input);

                        let input_record_definition = &self.record_definitions[input.record_type()];
                        let input_variant = &input_record_definition[input.variant_id()];
                        let output_record_definition =
                            &self.record_definitions[output.record_type()];
                        let output_variant = &output_record_definition[output.variant_id()];
                        let merge = input.record_type() == output.record_type();

                        let mut merged = BTreeSet::<DatumId>::new();

                        if merge {
                            for input_output in input_variant
                                .data()
                                .merge_join_by(output_variant.data(), DatumId::cmp)
                            {
                                match input_output {
                                    EitherOrBoth::Both(in_d, out_d) => {
                                        helper.push_connected_ports(
                                            &mut port_columns,
                                            &mut port_count,
                                            &(source, input.record_type(), in_d),
                                            (input.record_type(), in_d),
                                            (node.name(), output.record_type(), out_d),
                                        );
                                        merged.insert(in_d);
                                        if let Some(sub_stream) = input.sub_streams().get(&in_d) {
                                            helper.push_pass_through_sub_stream_ports(
                                                self,
                                                &mut port_columns,
                                                &mut port_count,
                                                sub_stream,
                                                1,
                                            );
                                        }
                                    }
                                    EitherOrBoth::Left(in_d) => {
                                        helper.push_input_port(
                                            &mut port_columns,
                                            &mut port_count,
                                            &(source, input.record_type(), in_d),
                                            (input.record_type(), in_d),
                                        );
                                        if let Some(sub_stream) = input.sub_streams().get(&in_d) {
                                            helper.push_input_sub_stream_port(
                                                self,
                                                &mut port_columns,
                                                &mut port_count,
                                                sub_stream,
                                                1,
                                            );
                                        }
                                    }
                                    EitherOrBoth::Right(out_d) => {
                                        helper.push_output_port(
                                            &mut port_columns,
                                            &mut port_count,
                                            (node.name(), output.record_type(), out_d),
                                        );
                                        if let Some(sub_stream) = output.sub_streams().get(&out_d) {
                                            helper.push_output_sub_stream_port(
                                                self,
                                                &mut port_columns,
                                                &mut port_count,
                                                sub_stream,
                                                1,
                                            );
                                        }
                                    }
                                }
                            }
                        } else {
                            for in_d in input_variant.data() {
                                helper.push_input_port(
                                    &mut port_columns,
                                    &mut port_count,
                                    &(source, input.record_type(), in_d),
                                    (input.record_type(), in_d),
                                );
                                if let Some(sub_stream) = input.sub_streams().get(&in_d) {
                                    helper.push_input_sub_stream_port(
                                        self,
                                        &mut port_columns,
                                        &mut port_count,
                                        sub_stream,
                                        1,
                                    );
                                }
                            }

                            for out_d in output_variant.data() {
                                helper.push_output_port(
                                    &mut port_columns,
                                    &mut port_count,
                                    (node.name(), output.record_type(), out_d),
                                );
                                if let Some(sub_stream) = output.sub_streams().get(&out_d) {
                                    helper.push_output_sub_stream_port(
                                        self,
                                        &mut port_columns,
                                        &mut port_count,
                                        sub_stream,
                                        1,
                                    );
                                }
                            }
                        }
                    }
                    EitherOrBoth::Left(input) => {
                        let source = Self::drawing_source_node_name(input);

                        let input_record_definition = &self.record_definitions[input.record_type()];
                        let input_variant = &input_record_definition[input.variant_id()];

                        for in_d in input_variant.data() {
                            helper.push_input_port(
                                &mut port_columns,
                                &mut port_count,
                                &(source, input.record_type(), in_d),
                                (input.record_type(), in_d),
                            );
                            if let Some(sub_stream) = input.sub_streams().get(&in_d) {
                                helper.push_input_sub_stream_port(
                                    self,
                                    &mut port_columns,
                                    &mut port_count,
                                    sub_stream,
                                    1,
                                );
                            }
                        }
                    }
                    EitherOrBoth::Right(output) => {
                        let output_record_definition =
                            &self.record_definitions[output.record_type()];
                        let output_variant = &output_record_definition[output.variant_id()];

                        for out_d in output_variant.data() {
                            helper.push_output_port(
                                &mut port_columns,
                                &mut port_count,
                                (node.name(), output.record_type(), out_d),
                            );
                            if let Some(sub_stream) = output.sub_streams().get(&out_d) {
                                helper.push_output_sub_stream_port(
                                    self,
                                    &mut port_columns,
                                    &mut port_count,
                                    sub_stream,
                                    1,
                                );
                            }
                        }
                    }
                }
            }

            helper.push_node_into_column(col, row, node, port_columns);
        }

        helper.into()
    }

    pub fn drawing_source_node_name(input: &NodeStream) -> &[Box<str>] {
        let source = input.source();
        if input.is_source_main_stream() {
            source
        } else {
            &source[0..source.len() - 1]
        }
    }

    pub fn streams_to_svg(&self) -> Result<String, std::fmt::Error> {
        let drawing = self.streams_to_abstract_drawing();
        let mut svg = String::new();
        format_svg(&mut svg, &drawing).unwrap();
        Ok(svg)
    }

    pub fn records_to_svg(&self) -> Result<String, std::fmt::Error> {
        let drawing = self.records_to_abstract_drawing();
        let mut svg = String::new();
        format_svg(&mut svg, &drawing).unwrap();
        Ok(svg)
    }
}

fn rustfmt_generated_file(file: &Path) {
    use std::process::Command;

    let rustfmt = if let Ok(rustfmt) = which::which("rustfmt") {
        rustfmt
    } else {
        eprintln!("Rustfmt activated, but it could not be found in global path.");
        return;
    };

    let mut cmd = Command::new(rustfmt);

    if let Ok(output) = cmd.arg(file).output() {
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            match output.status.code() {
                Some(2) => {
                    eprintln!("Rustfmt parsing errors:\n{}", stderr);
                }
                Some(3) => {
                    eprintln!("Rustfmt could not format some lines:\n{}", stderr);
                }
                _ => {
                    eprintln!("Internal rustfmt error:\n{}", stderr);
                }
            }
        }
    } else {
        eprintln!("Error executing rustfmt!");
    }
}
