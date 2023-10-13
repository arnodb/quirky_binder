use crate::{
    chain::{Chain, ChainCustomizer},
    prelude::*,
    stream::{NodeStream, StreamRecordType},
    support::FullyQualifiedName,
};
use codegen::Scope;
use std::{
    cell::RefCell,
    collections::{btree_map::Entry, BTreeMap},
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

    fn gen_chain(&self, graph: &Graph, chain: &mut Chain);
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

    fn gen_chain(&self, graph: &Graph, chain: &mut Chain) {
        for node in &self.ordered_nodes {
            node.gen_chain(graph, chain);
        }
    }
}

#[derive(new)]
pub struct GraphBuilder<R: TypeResolver + Copy> {
    type_resolver: R,
    chain_customizer: ChainCustomizer,
    #[new(default)]
    record_definitions: BTreeMap<StreamRecordType, RefCell<RecordDefinitionBuilder<R>>>,
    #[new(default)]
    sub_streams: BTreeMap<(StreamRecordType, DatumId), NodeStream>,
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

    pub fn get_sub_stream(
        &self,
        record_type: StreamRecordType,
        datum_id: DatumId,
    ) -> Option<&NodeStream> {
        self.sub_streams.get(&(record_type, datum_id))
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
            sub_streams: self.sub_streams,
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
    sub_streams: Vec<((StreamRecordType, DatumId), NodeStream)>,
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
            sub_streams: Vec::new(),
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
        self.new_output_internal(graph, full_name)
    }

    pub fn new_named_output<'b, 'g, R: TypeResolver + Copy>(
        &'b mut self,
        name: &str,
        graph: &'g GraphBuilder<R>,
    ) -> OutputBuilder<'a, 'b, 'g, R> {
        let full_name = self.name.sub(name);
        self.new_output_internal(graph, full_name)
    }

    fn new_output_internal<'b, 'g, R: TypeResolver + Copy>(
        &'b mut self,
        graph: &'g GraphBuilder<R>,
        full_name: FullyQualifiedName,
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
            source: full_name.into(),
        }
    }

    pub fn output_from_input<'b, 'g, R: TypeResolver + Copy>(
        &'b mut self,
        input_index: usize,
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
            source,
        }
    }

    pub fn build<R: TypeResolver + Copy, const OUT: usize>(
        self,
        graph: &mut GraphBuilder<R>,
    ) -> [NodeStream; OUT] {
        let old_size = graph.sub_streams.len();
        let added_size = self.sub_streams.len();
        graph.sub_streams.extend(self.sub_streams);
        let new_size = graph.sub_streams.len();
        assert_eq!(new_size, old_size + added_size);
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
    source: NodeStreamSource,
}

impl<'a, 'b, 'g, R: TypeResolver + Copy> OutputBuilder<'a, 'b, 'g, R> {
    pub fn for_update(self) -> OutputBuilderForUpdate<'a, 'b, 'g, R> {
        OutputBuilderForUpdate {
            streams: self.streams,
            record_type: self.record_type,
            record_definition: self.record_definition,
            input_variant_id: self.input_variant_id,
            source: self.source,
            sub_streams: Vec::new(),
        }
    }

    pub fn pass_through(self) -> &'g RefCell<RecordDefinitionBuilder<R>> {
        if let Some(input_variant_id) = self.input_variant_id {
            self.streams.outputs.push(NodeStream::new(
                self.record_type,
                input_variant_id,
                self.source,
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
    source: NodeStreamSource,
    sub_streams: Vec<((StreamRecordType, DatumId), NodeStream)>,
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
            source: Default::default(),
            sub_streams: Vec::new(),
        }
    }

    pub fn sub_stream_for_update(
        &mut self,
        sub_stream: &NodeStream,
        graph: &'g GraphBuilder<R>,
    ) -> SubStreamBuilder<'g, R> {
        let record_definition = graph
            .get_stream(sub_stream.record_type())
            .unwrap_or_else(|| panic!(r#"stream "{}""#, sub_stream.record_type()));
        SubStreamBuilder {
            record_type: sub_stream.record_type().clone(),
            record_definition,
            input_variant_id: Some(sub_stream.variant_id()),
            source: Default::default(),
            sub_streams: Vec::new(),
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
            &NodeStream,
            &mut OutputBuilderForUpdate<'c, 'd, 'g, R>,
        ) -> NodeStream,
        UpdatePathStream: Fn(&str, &mut SubStreamBuilder<'g, R>, &NodeStream),
        UpdateRootStream:
            for<'c, 'd> FnOnce(&str, &mut OutputBuilderForUpdate<'c, 'd, 'g, R>, &NodeStream),
    {
        struct PathFieldDetails<'a> {
            stream: &'a NodeStream,
            field: &'a str,
            sub_stream: &'a NodeStream,
        }

        let input = NodeStream::new(
            self.record_type.clone(),
            self.input_variant_id(),
            self.source.clone(),
        );

        // Then find path input streams
        let (mut path_details, leaf_sub_input_stream, _) = path_fields.iter().fold(
            (Vec::new(), &input, &**self),
            |(mut path_data, stream, record_definition), field| {
                let datum_id = record_definition
                    .borrow()
                    .get_latest_variant_datum_definition_by_name(field)
                    .map(DatumDefinition::id);
                if let Some(datum_id) = datum_id {
                    let sub_stream = graph
                        .get_sub_stream(stream.record_type().clone(), datum_id)
                        .expect("sub stream");
                    path_data.push(PathFieldDetails {
                        stream,
                        field,
                        sub_stream,
                    });
                    let sub_record_definition = graph
                        .get_stream(sub_stream.record_type())
                        .unwrap_or_else(|| panic!(r#"stream "{}""#, sub_stream.record_type()));
                    (path_data, sub_stream, sub_record_definition)
                } else {
                    panic!("could not find datum `{}`", field);
                }
            },
        );

        // Then update the leaf sub stream
        let mut sub_output_stream = update_leaf_sub_stream(leaf_sub_input_stream, self);

        // Then all streams up to the root
        let mut path_update_streams = Vec::<PathUpdateElement>::with_capacity(path_fields.len());

        while path_details.len() > 1 {
            if let Some(field_details) = path_details.pop() {
                path_update_streams.push(PathUpdateElement {
                    field: field_details.field.to_owned(),
                    sub_input_stream: field_details.sub_stream.clone(),
                    sub_output_stream: sub_output_stream.clone(),
                });
                let mut path_stream = self.sub_stream_for_update(field_details.stream, graph);
                update_path_stream(field_details.field, &mut path_stream, &sub_output_stream);
                sub_output_stream = self.close_sub_stream_variant(path_stream);
            }
        }

        if let Some(field_details) = path_details.pop() {
            path_update_streams.push(PathUpdateElement {
                field: field_details.field.to_owned(),
                sub_input_stream: field_details.sub_stream.clone(),
                sub_output_stream: sub_output_stream.clone(),
            });
            assert_eq!(field_details.stream.record_type(), input.record_type());

            update_root_stream(field_details.field, self, &sub_output_stream);
        };

        assert_eq!(path_details.len(), 0);

        // They were pushed in reverse order
        path_update_streams.reverse();

        path_update_streams
    }

    pub fn add_vec_datum(&mut self, field: &str, record: &str, sub_stream: NodeStream) {
        let datum_id = add_vec_datum_to_record_definition(
            &mut self.record_definition.borrow_mut(),
            field,
            record,
        );
        self.sub_streams
            .push(((self.record_type.clone(), datum_id), sub_stream));
    }

    pub fn replace_vec_datum(&mut self, field: &str, record: &str, sub_stream: NodeStream) {
        let datum_id = replace_vec_datum_in_record_definition(
            &mut self.record_definition.borrow_mut(),
            field,
            record,
        );
        self.sub_streams
            .push(((self.record_type.clone(), datum_id), sub_stream));
    }

    pub fn close_sub_stream_variant(&mut self, sub_stream: SubStreamBuilder<'g, R>) -> NodeStream {
        sub_stream.close_record_variant(self.streams)
    }
}

impl<'a, 'b, 'g, R: TypeResolver + Copy> Drop for OutputBuilderForUpdate<'a, 'b, 'g, R> {
    fn drop(&mut self) {
        let variant_id = self.record_definition.borrow_mut().close_record_variant();
        self.streams.outputs.push(NodeStream::new(
            self.record_type.clone(),
            variant_id,
            self.source.clone(),
        ));
        let mut sub_streams = Vec::new();
        std::mem::swap(&mut sub_streams, &mut self.sub_streams);
        self.streams.sub_streams.extend(sub_streams);
    }
}

pub struct PathUpdateElement {
    pub field: String,
    pub sub_input_stream: NodeStream,
    pub sub_output_stream: NodeStream,
}

#[derive(Getters)]
pub struct SubStreamBuilder<'g, R: TypeResolver> {
    #[getset(get = "pub")]
    record_type: StreamRecordType,
    record_definition: &'g RefCell<RecordDefinitionBuilder<R>>,
    input_variant_id: Option<RecordVariantId>,
    source: NodeStreamSource,
    sub_streams: Vec<((StreamRecordType, DatumId), NodeStream)>,
}

impl<'g, R: TypeResolver> SubStreamBuilder<'g, R> {
    pub fn input_variant_id(&self) -> RecordVariantId {
        if let Some(input_variant_id) = self.input_variant_id {
            input_variant_id
        } else {
            panic! {"output not derived from input"}
        }
    }

    pub fn add_vec_datum(&mut self, field: &str, record: &str, sub_stream: NodeStream) {
        let datum_id = add_vec_datum_to_record_definition(
            &mut self.record_definition.borrow_mut(),
            field,
            record,
        );
        self.sub_streams
            .push(((self.record_type.clone(), datum_id), sub_stream));
    }

    pub fn replace_vec_datum(&mut self, field: &str, record: &str, sub_stream: NodeStream) {
        let datum_id = replace_vec_datum_in_record_definition(
            &mut self.record_definition.borrow_mut(),
            field,
            record,
        );
        self.sub_streams
            .push(((self.record_type.clone(), datum_id), sub_stream));
    }

    fn close_record_variant(self, streams: &mut StreamsBuilder) -> NodeStream {
        let variant_id = self.record_definition.borrow_mut().close_record_variant();
        streams.sub_streams.extend(self.sub_streams);
        NodeStream::new(self.record_type, variant_id, self.source)
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
) -> DatumId {
    let old_datum = record_definition
        .get_latest_variant_datum_definition_by_name(field)
        .unwrap_or_else(|| panic!(r#"datum "{}""#, field));
    let old_datum_id = old_datum.id();
    record_definition.remove_datum(old_datum_id);
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

pub struct Graph {
    chain_customizer: ChainCustomizer,
    record_definitions: BTreeMap<StreamRecordType, RecordDefinition>,
    entry_nodes: Vec<Box<dyn DynNode>>,
    sub_streams: BTreeMap<(StreamRecordType, DatumId), NodeStream>,
}

impl Graph {
    pub fn chain_customizer(&self) -> &ChainCustomizer {
        &self.chain_customizer
    }

    pub fn record_definitions(&self) -> &BTreeMap<StreamRecordType, RecordDefinition> {
        &self.record_definitions
    }

    pub fn get_sub_stream(
        &self,
        record_type: StreamRecordType,
        datum_id: DatumId,
    ) -> Option<NodeStream> {
        self.sub_streams.get(&(record_type, datum_id)).cloned()
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
