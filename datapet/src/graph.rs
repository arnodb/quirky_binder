use crate::{
    drawing::{
        format_svg,
        graph::{RecordsDrawingHelper, StreamsDrawingHelper},
        Drawing, DrawingPortsColumn,
    },
    prelude::*,
    stream::NodeSubStream,
};
use codegen::Scope;
use itertools::{EitherOrBoth, Itertools};
use std::{
    cell::RefCell,
    collections::{btree_map::Entry, BTreeMap, BTreeSet},
    fs::File,
    ops::Deref,
    path::Path,
};
use truc::record::{
    definition::{
        DatumDefinition, DatumDefinitionOverride, DatumId, RecordDefinition,
        RecordDefinitionBuilder, RecordVariantId,
    },
    type_resolver::TypeResolver,
};

pub trait DynNode {
    fn name(&self) -> &FullyQualifiedName;

    fn inputs(&self) -> &[NodeStream];

    fn outputs(&self) -> &[NodeStream];

    fn gen_chain(&self, graph: &Graph, chain: &mut Chain);

    fn all_nodes(&self) -> Box<dyn Iterator<Item = &dyn DynNode> + '_>;
}

#[derive(new)]
pub struct NodeCluster<const IN: usize, const OUT: usize> {
    name: FullyQualifiedName,
    ordered_nodes: Vec<Box<dyn DynNode>>,
    inputs: [NodeStream; IN],
    outputs: [NodeStream; OUT],
}

impl<const IN: usize, const OUT: usize> NodeCluster<IN, OUT> {
    pub fn name(&self) -> &FullyQualifiedName {
        &self.name
    }

    pub fn inputs(&self) -> &[NodeStream; IN] {
        &self.inputs
    }

    pub fn outputs(&self) -> &[NodeStream; OUT] {
        &self.outputs
    }
}

impl<const IN: usize, const OUT: usize> DynNode for NodeCluster<IN, OUT> {
    fn name(&self) -> &FullyQualifiedName {
        &self.name
    }

    fn inputs(&self) -> &[NodeStream] {
        &self.inputs
    }

    fn outputs(&self) -> &[NodeStream] {
        &self.outputs
    }

    fn gen_chain(&self, graph: &Graph, chain: &mut Chain) {
        for node in &self.ordered_nodes {
            node.gen_chain(graph, chain);
        }
    }

    fn all_nodes(&self) -> Box<dyn Iterator<Item = &dyn DynNode> + '_> {
        Box::new(self.ordered_nodes.iter().flat_map(|node| node.all_nodes()))
    }
}

#[derive(new)]
pub struct GraphBuilder<R: TypeResolver + Copy> {
    type_resolver: R,
    chain_customizer: ChainCustomizer,
    #[new(default)]
    record_definitions: BTreeMap<StreamRecordType, RefCell<RecordDefinitionBuilder<R>>>,
    #[new(default)]
    anchor_table_count: usize,
}

impl<R: TypeResolver + Copy> GraphBuilder<R> {
    pub fn new_stream(&mut self, record_type: StreamRecordType) {
        match self.record_definitions.entry(record_type) {
            Entry::Vacant(entry) => {
                let record_definition_builder = RecordDefinitionBuilder::new(self.type_resolver);
                entry.insert(record_definition_builder.into());
            }
            Entry::Occupied(entry) => {
                panic!(r#"Stream "{}" already exists"#, entry.key())
            }
        }
    }

    pub fn new_anchor_table(&mut self) -> usize {
        let anchor_table_id = self.anchor_table_count;
        self.anchor_table_count = anchor_table_id + 1;
        anchor_table_id
    }

    pub fn get_stream(
        &self,
        record_type: &StreamRecordType,
    ) -> Option<&RefCell<RecordDefinitionBuilder<R>>> {
        self.record_definitions.get(record_type)
    }

    pub fn build(self, entry_nodes: Vec<Box<dyn DynNode>>) -> Graph {
        Graph {
            chain_customizer: self.chain_customizer,
            record_definitions: self
                .record_definitions
                .into_iter()
                .map(|(name, builder)| (name, builder.into_inner().build()))
                .collect(),
            entry_nodes,
        }
    }

    pub fn chain_customizer(&self) -> &ChainCustomizer {
        &self.chain_customizer
    }
}

pub struct StreamsBuilder<'a> {
    name: &'a FullyQualifiedName,
    inputs: &'a [NodeStream],
    outputs: Vec<NodeStream>,
    in_out_links: Vec<Option<usize>>,
}

impl<'a> StreamsBuilder<'a> {
    pub fn new(name: &'a FullyQualifiedName, inputs: &'a [NodeStream]) -> Self {
        let mut in_out_links = Vec::with_capacity(inputs.len());
        for _ in 0..inputs.len() {
            in_out_links.push(None);
        }
        Self {
            name,
            inputs,
            outputs: Vec::new(),
            in_out_links,
        }
    }

    pub fn new_main_stream<R: TypeResolver + Copy>(&mut self, graph: &mut GraphBuilder<R>) {
        let full_name = self.name.clone();
        let record_type = StreamRecordType::from(full_name);
        graph.new_stream(record_type);
    }

    pub fn new_named_stream<R: TypeResolver + Copy>(
        &mut self,
        name: &str,
        graph: &mut GraphBuilder<R>,
    ) {
        let full_name = self.name.sub(name);
        let record_type = StreamRecordType::from(full_name);
        graph.new_stream(record_type);
    }

    pub fn new_main_output<'b, 'g, R: TypeResolver + Copy>(
        &'b mut self,
        graph: &'g GraphBuilder<R>,
    ) -> OutputBuilder<'a, 'b, 'g, R> {
        let full_name = self.name.clone();
        self.new_output_internal(graph, full_name, true)
    }

    pub fn new_named_output<'b, 'g, R: TypeResolver + Copy>(
        &'b mut self,
        name: &str,
        graph: &'g GraphBuilder<R>,
    ) -> OutputBuilder<'a, 'b, 'g, R> {
        let full_name = self.name.sub(name);
        self.new_output_internal(graph, full_name, false)
    }

    fn new_output_internal<'b, 'g, R: TypeResolver + Copy>(
        &'b mut self,
        graph: &'g GraphBuilder<R>,
        full_name: FullyQualifiedName,
        is_output_main_stream: bool,
    ) -> OutputBuilder<'a, 'b, 'g, R> {
        let record_type = StreamRecordType::from(full_name.clone());
        let record_definition = graph
            .get_stream(&record_type)
            .unwrap_or_else(|| panic!(r#"stream "{}""#, &record_type));
        OutputBuilder {
            streams: self,
            record_type,
            record_definition,
            input_variant_id: None,
            input_sub_streams: BTreeMap::new(),
            source: full_name.into(),
            is_output_main_stream,
        }
    }

    pub fn output_from_input<'b, 'g, R: TypeResolver + Copy>(
        &'b mut self,
        input_index: usize,
        is_output_main_stream: bool,
        graph: &'g GraphBuilder<R>,
    ) -> OutputBuilder<'a, 'b, 'g, R> {
        let input = &self.inputs[input_index];
        if let Some(output_index) = self.in_out_links[input_index] {
            panic!(
                "Output {} is already derived from input {}",
                output_index, input_index
            );
        }
        let record_definition = graph
            .get_stream(input.record_type())
            .unwrap_or_else(|| panic!(r#"stream "{}""#, input.record_type()));
        let output_index = self.outputs.len();
        self.in_out_links[input_index] = Some(output_index);
        let source = self.name.clone().into();
        OutputBuilder {
            streams: self,
            record_type: input.record_type().clone(),
            record_definition,
            input_variant_id: Some(input.variant_id()),
            input_sub_streams: input.sub_streams().clone(),
            source,
            is_output_main_stream,
        }
    }

    pub fn build<const OUT: usize>(self) -> [NodeStream; OUT] {
        self.outputs.try_into().unwrap_or_else(|v: Vec<_>| {
            panic!("Expected a Vec of length {} but it was {}", OUT, v.len())
        })
    }
}

#[must_use]
#[derive(Getters)]
pub struct OutputBuilder<'a, 'b, 'g, R: TypeResolver> {
    streams: &'b mut StreamsBuilder<'a>,
    #[getset(get = "pub")]
    record_type: StreamRecordType,
    record_definition: &'g RefCell<RecordDefinitionBuilder<R>>,
    input_variant_id: Option<RecordVariantId>,
    input_sub_streams: BTreeMap<DatumId, NodeSubStream>,
    source: NodeStreamSource,
    is_output_main_stream: bool,
}

impl<'a, 'b, 'g, R: TypeResolver + Copy> OutputBuilder<'a, 'b, 'g, R> {
    pub fn for_update(self) -> OutputBuilderForUpdate<'a, 'b, 'g, R> {
        OutputBuilderForUpdate {
            streams: self.streams,
            record_type: self.record_type,
            record_definition: self.record_definition,
            input_variant_id: self.input_variant_id,
            sub_streams: self.input_sub_streams,
            source: self.source,
            is_output_main_stream: self.is_output_main_stream,
        }
    }

    pub fn pass_through(self) -> &'g RefCell<RecordDefinitionBuilder<R>> {
        if let Some(input_variant_id) = self.input_variant_id {
            self.streams.outputs.push(NodeStream::new(
                self.record_type,
                input_variant_id,
                self.input_sub_streams,
                self.source,
                self.is_output_main_stream,
            ));
            self.record_definition
        } else {
            panic! {"output not derived from input"}
        }
    }
}

#[derive(Getters)]
pub struct OutputBuilderForUpdate<'a, 'b, 'g, R: TypeResolver + Copy> {
    streams: &'b mut StreamsBuilder<'a>,
    #[getset(get = "pub")]
    record_type: StreamRecordType,
    record_definition: &'g RefCell<RecordDefinitionBuilder<R>>,
    input_variant_id: Option<RecordVariantId>,
    sub_streams: BTreeMap<DatumId, NodeSubStream>,
    source: NodeStreamSource,
    is_output_main_stream: bool,
}

impl<'a, 'b, 'g, R: TypeResolver + Copy> Deref for OutputBuilderForUpdate<'a, 'b, 'g, R> {
    type Target = RefCell<RecordDefinitionBuilder<R>>;

    fn deref(&self) -> &Self::Target {
        self.record_definition
    }
}

impl<'a, 'b, 'g, R: TypeResolver + Copy> OutputBuilderForUpdate<'a, 'b, 'g, R> {
    pub fn input_variant_id(&self) -> RecordVariantId {
        self.input_variant_id
            .expect("output not derived from input")
    }

    pub fn new_named_sub_stream(
        &self,
        name: &str,
        graph: &'g GraphBuilder<R>,
    ) -> SubStreamBuilder<'g, R> {
        let full_name = self.streams.name.sub(name);
        self.new_sub_stream_internal(graph, full_name)
    }

    fn new_sub_stream_internal(
        &self,
        graph: &'g GraphBuilder<R>,
        full_name: FullyQualifiedName,
    ) -> SubStreamBuilder<'g, R> {
        let record_type = StreamRecordType::from(full_name);
        let record_definition = graph
            .get_stream(&record_type)
            .unwrap_or_else(|| panic!(r#"stream "{}""#, &record_type));
        SubStreamBuilder {
            record_type,
            record_definition,
            input_variant_id: None,
            sub_streams: BTreeMap::new(),
        }
    }

    pub fn sub_stream_for_update(
        &mut self,
        sub_stream: NodeSubStream,
        graph: &'g GraphBuilder<R>,
    ) -> SubStreamBuilder<'g, R> {
        let record_definition = graph
            .get_stream(sub_stream.record_type())
            .unwrap_or_else(|| panic!(r#"stream "{}""#, sub_stream.record_type()));
        let (record_type, variant_id, sub_streams) = sub_stream.destructure();
        SubStreamBuilder {
            record_type,
            record_definition,
            input_variant_id: Some(variant_id),
            sub_streams,
        }
    }

    pub fn update_path<UpdateLeafSubStream, UpdatePathStream, UpdateRootStream>(
        &mut self,
        path_fields: &[&str],
        update_leaf_sub_stream: UpdateLeafSubStream,
        update_path_stream: UpdatePathStream,
        update_root_stream: UpdateRootStream,
        graph: &'g GraphBuilder<R>,
    ) -> Vec<PathUpdateElement>
    where
        UpdateLeafSubStream: for<'c, 'd> FnOnce(
            NodeSubStream,
            &mut OutputBuilderForUpdate<'c, 'd, 'g, R>,
        ) -> NodeSubStream,
        UpdatePathStream: Fn(&str, &mut SubStreamBuilder<'g, R>, NodeSubStream),
        UpdateRootStream:
            for<'c, 'd> FnOnce(&str, &mut OutputBuilderForUpdate<'c, 'd, 'g, R>, NodeSubStream),
    {
        struct PathFieldDetails<'a> {
            stream: NodeSubStream,
            field: &'a str,
            datum_id: DatumId,
        }

        // Find the sub stream of the first path field
        let (root_field, root_datum_id, root_sub_stream, root_sub_stream_record_definition) = {
            let field = path_fields.first().expect("first path field");
            let datum_id = self
                .record_definition
                .borrow()
                .get_latest_variant_datum_definition_by_name(field)
                .map(DatumDefinition::id);
            if let Some(datum_id) = datum_id {
                let sub_stream = self.sub_streams.remove(&datum_id).expect("root sub stream");
                let sub_record_definition = graph
                    .get_stream(sub_stream.record_type())
                    .unwrap_or_else(|| panic!(r#"stream "{}""#, sub_stream.record_type()));
                (*field, datum_id, sub_stream, sub_record_definition)
            } else {
                panic!("could not find datum `{}`", field);
            }
        };

        // Then find path input streams
        let (mut path_details, leaf_sub_input_stream, _) = path_fields[1..].iter().fold(
            (
                Vec::new(),
                root_sub_stream,
                root_sub_stream_record_definition,
            ),
            |(mut path_data, mut stream, record_definition), field| {
                let datum_id = record_definition
                    .borrow()
                    .get_latest_variant_datum_definition_by_name(field)
                    .map(DatumDefinition::id);
                if let Some(datum_id) = datum_id {
                    let sub_stream = stream
                        .sub_streams_mut()
                        .remove(&datum_id)
                        .expect("sub stream");
                    let sub_record_definition = graph
                        .get_stream(sub_stream.record_type())
                        .unwrap_or_else(|| panic!(r#"stream "{}""#, sub_stream.record_type()));
                    path_data.push(PathFieldDetails {
                        stream,
                        field,
                        datum_id,
                    });
                    (path_data, sub_stream, sub_record_definition)
                } else {
                    panic!("could not find datum `{}`", field);
                }
            },
        );

        // Then update the leaf sub stream
        let mut sub_input_stream = Some(leaf_sub_input_stream.clone());
        let mut sub_output_stream = Some(update_leaf_sub_stream(leaf_sub_input_stream, self));

        // Then all streams up to the root
        let mut path_update_streams = Vec::<PathUpdateElement>::with_capacity(path_fields.len());

        while !path_details.is_empty() {
            let sub_stream = sub_output_stream.take().expect("sub_output_stream");
            if let Some(mut field_details) = path_details.pop() {
                let old = field_details
                    .stream
                    .sub_streams_mut()
                    .insert(field_details.datum_id, sub_stream.clone());
                if old.is_some() {
                    panic!("sub stream should have been removed");
                }
                path_update_streams.push(PathUpdateElement {
                    field: field_details.field.to_owned(),
                    sub_input_stream: sub_input_stream.take().expect("sub_input_stream"),
                    sub_output_stream: sub_stream.clone(),
                });
                let mut path_stream =
                    self.sub_stream_for_update(field_details.stream.clone(), graph);
                update_path_stream(field_details.field, &mut path_stream, sub_stream);
                sub_input_stream = Some(field_details.stream);
                sub_output_stream = Some(path_stream.close_record_variant());
            }
        }

        {
            let sub_stream = sub_output_stream.take().expect("sub_output_stream");
            let old = self.sub_streams.insert(root_datum_id, sub_stream.clone());
            if old.is_some() {
                panic!("sub stream should have been removed");
            }
            path_update_streams.push(PathUpdateElement {
                field: root_field.to_owned(),
                sub_input_stream: sub_input_stream.take().expect("sub_input_stream"),
                sub_output_stream: sub_stream.clone(),
            });
            update_root_stream(root_field, self, sub_stream);
        }

        assert_eq!(path_details.len(), 0);

        // They were pushed in reverse order
        path_update_streams.reverse();

        path_update_streams
    }

    pub fn add_vec_datum(&mut self, field: &str, record: &str, sub_stream: NodeSubStream) {
        let datum_id = add_vec_datum_to_record_definition(
            &mut self.record_definition.borrow_mut(),
            field,
            record,
        );
        let old = self.sub_streams.insert(datum_id, sub_stream);
        if old.is_some() {
            panic!("the datum should not be registered yet");
        }
    }

    pub fn replace_vec_datum(&mut self, field: &str, record: &str, sub_stream: NodeSubStream) {
        let (old_datum_id, new_datum_id) = replace_vec_datum_in_record_definition(
            &mut self.record_definition.borrow_mut(),
            field,
            record,
        );
        let old = self.sub_streams.remove(&old_datum_id);
        if old.is_none() {
            panic!("the replaced datum should be registered");
        }
        let old = self.sub_streams.insert(new_datum_id, sub_stream);
        if old.is_some() {
            panic!("the datum should not be registered yet");
        }
    }
}

impl<'a, 'b, 'g, R: TypeResolver + Copy> Drop for OutputBuilderForUpdate<'a, 'b, 'g, R> {
    fn drop(&mut self) {
        let variant_id = self.record_definition.borrow_mut().close_record_variant();
        let mut sub_streams = BTreeMap::new();
        std::mem::swap(&mut sub_streams, &mut self.sub_streams);
        self.streams.outputs.push(NodeStream::new(
            self.record_type.clone(),
            variant_id,
            sub_streams,
            self.source.clone(),
            self.is_output_main_stream,
        ));
    }
}

pub struct PathUpdateElement {
    pub field: String,
    pub sub_input_stream: NodeSubStream,
    pub sub_output_stream: NodeSubStream,
}

#[derive(Getters)]
pub struct SubStreamBuilder<'g, R: TypeResolver> {
    #[getset(get = "pub")]
    record_type: StreamRecordType,
    record_definition: &'g RefCell<RecordDefinitionBuilder<R>>,
    input_variant_id: Option<RecordVariantId>,
    sub_streams: BTreeMap<DatumId, NodeSubStream>,
}

impl<'g, R: TypeResolver> SubStreamBuilder<'g, R> {
    pub fn input_variant_id(&self) -> RecordVariantId {
        if let Some(input_variant_id) = self.input_variant_id {
            input_variant_id
        } else {
            panic! {"output not derived from input"}
        }
    }

    pub fn add_vec_datum(&mut self, field: &str, record: &str, sub_stream: NodeSubStream) {
        let datum_id = add_vec_datum_to_record_definition(
            &mut self.record_definition.borrow_mut(),
            field,
            record,
        );
        let old = self.sub_streams.insert(datum_id, sub_stream);
        if old.is_some() {
            panic!("the datum should not be registered yet");
        }
    }

    pub fn replace_vec_datum(&mut self, field: &str, record: &str, sub_stream: NodeSubStream) {
        let (old_datum_id, new_datum_id) = replace_vec_datum_in_record_definition(
            &mut self.record_definition.borrow_mut(),
            field,
            record,
        );
        let old = self.sub_streams.remove(&old_datum_id);
        if old.is_none() {
            panic!("the replaced datum should be registered");
        }
        let old = self.sub_streams.insert(new_datum_id, sub_stream);
        if old.is_some() {
            panic!("the datum should not be registered yet");
        }
    }

    pub fn close_record_variant(self) -> NodeSubStream {
        let variant_id = self.record_definition.borrow_mut().close_record_variant();
        NodeSubStream::new(self.record_type, variant_id, self.sub_streams)
    }
}

impl<'g, R: TypeResolver> Deref for SubStreamBuilder<'g, R> {
    type Target = RefCell<RecordDefinitionBuilder<R>>;

    fn deref(&self) -> &Self::Target {
        self.record_definition
    }
}

fn add_vec_datum_to_record_definition<R: TypeResolver>(
    record_definition: &mut RecordDefinitionBuilder<R>,
    field: &str,
    record: &str,
) -> DatumId {
    record_definition.add_datum_override::<Vec<()>, _>(
        field,
        DatumDefinitionOverride {
            type_name: Some(format!("Vec<{}>", record)),
            size: None,
            align: None,
            allow_uninit: None,
        },
    )
}

fn replace_vec_datum_in_record_definition<R: TypeResolver>(
    record_definition: &mut RecordDefinitionBuilder<R>,
    field: &str,
    record: &str,
) -> (DatumId, DatumId) {
    let old_datum = record_definition
        .get_latest_variant_datum_definition_by_name(field)
        .unwrap_or_else(|| panic!(r#"datum "{}""#, field));
    let old_datum_id = old_datum.id();
    record_definition.remove_datum(old_datum_id);
    (
        old_datum_id,
        record_definition.add_datum_override::<Vec<()>, _>(
            field,
            DatumDefinitionOverride {
                type_name: Some(format!("Vec<{}>", record)),
                size: None,
                align: None,
                allow_uninit: None,
            },
        ),
    )
}

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
            let mut file = File::create(&output.join("chain_streams.rs")).unwrap();
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
        rustfmt_generated_file(output.join("chain_streams.rs").as_path()).unwrap();
        {
            let mut scope = Scope::new();
            scope.import("fallible_iterator", "FallibleIterator");
            for (path, ty) in &self.chain_customizer.custom_module_imports {
                scope.import(path, ty);
            }

            let mut chain = Chain::new(&self.chain_customizer, &mut scope);

            for node in &self.entry_nodes {
                node.gen_chain(self, &mut chain);
            }

            chain.gen_chain();

            let mut file = File::create(output.join("chain.rs")).unwrap();
            write!(file, "{}", scope.to_string()).unwrap();
        }
        rustfmt_generated_file(output.join("chain.rs").as_path()).unwrap();
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

fn rustfmt_generated_file(file: &Path) -> std::io::Result<()> {
    use std::process::Command;

    let rustfmt = if let Ok(rustfmt) = which::which("rustfmt") {
        rustfmt
    } else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Rustfmt activated, but it could not be found in global path.",
        ));
    };

    let mut cmd = Command::new(rustfmt);

    if let Ok(output) = cmd.arg(file).output() {
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            match output.status.code() {
                Some(2) => Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Rustfmt parsing errors:\n{}", stderr),
                )),
                Some(3) => {
                    println!("Rustfmt could not format some lines:\n{}", stderr);
                    Ok(())
                }
                _ => Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Internal rustfmt error:\n{}", stderr),
                )),
            }
        } else {
            Ok(())
        }
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Error executing rustfmt!",
        ))
    }
}
