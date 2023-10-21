use self::{pass_through::OutputBuilderForPassThrough, update::OutputBuilderForUpdate};
use crate::prelude::*;
use itertools::Itertools;
use std::{
    cell::RefCell,
    collections::{btree_map::Entry, BTreeMap, BTreeSet},
};
use truc::record::{
    definition::{DatumDefinitionOverride, DatumId, RecordDefinitionBuilder, RecordVariantId},
    type_resolver::TypeResolver,
};

pub mod pass_through;
pub mod update;

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
            facts: StreamFacts::default(),
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
            facts: input.facts().clone(),
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
    facts: StreamFacts,
}

impl<'a, 'b, 'g, R: TypeResolver + Copy> OutputBuilder<'a, 'b, 'g, R> {
    pub fn update<B, O>(self, build: B) -> O
    where
        B: FnOnce(&mut OutputBuilderForUpdate<'a, 'b, 'g, R>) -> O,
    {
        let mut builder = OutputBuilderForUpdate {
            streams: self.streams,
            record_type: self.record_type,
            record_definition: self.record_definition,
            input_variant_id: self.input_variant_id,
            sub_streams: self.input_sub_streams,
            source: self.source,
            is_output_main_stream: self.is_output_main_stream,
            facts: self.facts,
        };
        let output = build(&mut builder);
        builder.build();
        output
    }

    pub fn pass_through<B, O>(self, build: B) -> O
    where
        B: FnOnce(&mut OutputBuilderForPassThrough<'a, 'b, 'g, R>) -> O,
    {
        let mut builder = OutputBuilderForPassThrough {
            streams: self.streams,
            record_type: self.record_type,
            record_definition: self.record_definition,
            input_variant_id: self
                .input_variant_id
                .expect("output not derived from input"),
            sub_streams: self.input_sub_streams,
            source: self.source,
            is_output_main_stream: self.is_output_main_stream,
            facts: self.facts,
        };
        let output = build(&mut builder);
        builder.build();
        output
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

pub fn set_facts_order<R: TypeResolver>(
    facts: &mut StreamFacts,
    order_fields: &[&str],
    record_definition: &RecordDefinitionBuilder<R>,
) {
    let mut seen = BTreeSet::<DatumId>::new();
    let order = order_fields
        .iter()
        .filter_map(|field| {
            let datum_id = record_definition
                .get_latest_variant_datum_definition_by_name(field)
                .expect("datum")
                .id();
            (!seen.contains(&datum_id)).then(|| {
                seen.insert(datum_id);
                datum_id
            })
        })
        .collect::<Vec<DatumId>>();
    assert_eq!(seen.len(), order.len());
    facts.set_order(order.into_boxed_slice());
}

pub fn assert_order_starts_with<R: TypeResolver>(
    expected_order: &[DatumId],
    actual_order: &[DatumId],
    record_definition: &RecordDefinitionBuilder<R>,
    filter_name: &FullyQualifiedName,
) {
    let is_ok = if actual_order.len() >= expected_order.len() {
        let truncated = &actual_order[0..expected_order.len()];
        let mut all_found = true;
        for actual_d in truncated {
            if !expected_order
                .iter()
                .any(|expected_d| expected_d == actual_d)
            {
                all_found = false;
                break;
            }
        }
        all_found
    } else {
        false
    };
    if !is_ok {
        let expected = expected_order
            .iter()
            .map(|d| record_definition[*d].name())
            .join(", ");
        let actual = actual_order
            .iter()
            .map(|d| record_definition[*d].name())
            .join(", ");
        panic!(
            "Fitler {} expected order to start with [{}], but is [{}]",
            filter_name, expected, actual
        );
    }
}
