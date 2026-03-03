use std::{
    cell::RefCell,
    collections::{btree_map::Entry, BTreeMap, BTreeSet},
    ops::Deref,
};

use itertools::Itertools;
use truc::record::definition::{DatumId, RecordVariantId};

use self::{
    params::ParamsBuilder, pass_through::OutputBuilderForPassThrough,
    update::OutputBuilderForUpdate,
};
use crate::prelude::*;

pub mod params;
pub mod pass_through;
pub mod update;

#[derive(new, Getters)]
pub struct GraphBuilder {
    chain_customizer: ChainCustomizer,
    #[new(default)]
    record_definitions: BTreeMap<StreamRecordType, RefCell<QuirkyRecordDefinitionBuilder>>,
    #[new(default)]
    #[getset(get = "pub")]
    params: ParamsBuilder,
    #[new(default)]
    anchor_table_count: usize,
}

impl GraphBuilder {
    pub fn new_stream(&mut self, record_type: StreamRecordType) -> ChainResult<()> {
        match self.record_definitions.entry(record_type) {
            Entry::Vacant(entry) => {
                let record_definition_builder = QuirkyRecordDefinitionBuilder::default();
                entry.insert(record_definition_builder.into());
                Ok(())
            }
            Entry::Occupied(entry) => Err(ChainError::StreamAlreadyExists {
                stream: entry.key().to_string(),
            }),
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
    ) -> ChainResult<&RefCell<QuirkyRecordDefinitionBuilder>> {
        self.record_definitions
            .get(record_type)
            .ok_or_else(|| ChainError::StreamNotFound {
                stream: record_type.to_string(),
            })
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

#[derive(Debug)]
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

    pub fn new_main_stream(&self, graph: &mut GraphBuilder) -> ChainResult<()> {
        let full_name = self.name.clone();
        let record_type = StreamRecordType::from(full_name);
        graph.new_stream(record_type)
    }

    pub fn new_named_stream(
        &self,
        name: &str,
        graph: &mut GraphBuilder,
    ) -> ChainResult<StreamRecordType> {
        let full_name = self.name.sub(name);
        let record_type = StreamRecordType::from(full_name);
        graph.new_stream(record_type.clone())?;
        Ok(record_type)
    }

    pub fn new_main_output<'g>(&self, graph: &'g GraphBuilder) -> ChainResult<OutputBuilder<'g>> {
        let full_name = self.name.clone();
        Self::new_output_internal(graph, full_name, true)
    }

    pub fn new_named_output<'g>(
        &self,
        name: &str,
        graph: &'g GraphBuilder,
    ) -> ChainResult<OutputBuilder<'g>> {
        let full_name = self.name.sub(name);
        Self::new_output_internal(graph, full_name, false)
    }

    fn new_output_internal<'g>(
        graph: &'g GraphBuilder,
        full_name: FullyQualifiedName,
        is_output_main_stream: bool,
    ) -> ChainResult<OutputBuilder<'g>> {
        let record_type = StreamRecordType::from(full_name.clone());
        let record_definition = graph.get_stream(&record_type)?;
        let stream = NodeStream::new(
            record_type,
            RecordVariantId::from(0),
            BTreeMap::new(),
            full_name.into(),
            is_output_main_stream,
            StreamFacts::default(),
        );
        Ok(OutputBuilder {
            record_definition,
            stream,
        })
    }

    pub fn output_from_input<'g>(
        &mut self,
        input_index: usize,
        is_output_main_stream: bool,
        graph: &'g GraphBuilder,
    ) -> ChainResult<OutputBuilder<'g>> {
        let input = &self.inputs[input_index];
        if let Some(output_index) = self.in_out_links[input_index] {
            return Err(ChainError::InputAlreadyDerived {
                input_index,
                output_index,
            });
        }
        let record_definition = graph.get_stream(input.record_type())?;
        let output_index = self.outputs.len();
        self.in_out_links[input_index] = Some(output_index);
        let stream = NodeStream::new(
            input.record_type().clone(),
            input.variant_id(),
            input.sub_streams().clone(),
            self.name.clone().into(),
            is_output_main_stream,
            input.facts().clone(),
        );
        Ok(OutputBuilder {
            record_definition,
            stream,
        })
    }

    pub fn build<const OUT: usize>(self) -> ChainResult<[NodeStream; OUT]> {
        self.outputs
            .try_into()
            .map_err(|err: Vec<_>| ChainError::UnexpectedNumberOfStreams {
                expected: OUT,
                actual: err.len(),
            })
    }
}

#[must_use]
pub struct OutputBuilder<'g> {
    record_definition: &'g RefCell<QuirkyRecordDefinitionBuilder>,
    stream: NodeStream,
}

impl<'g> OutputBuilder<'g> {
    pub fn update(self) -> OutputBuilderForUpdate<'g> {
        OutputBuilderForUpdate {
            record_definition: self.record_definition,
            stream: self.stream,
        }
    }

    pub fn pass_through(self) -> OutputBuilderForPassThrough<'g> {
        OutputBuilderForPassThrough {
            record_definition: self.record_definition,
            stream: self.stream,
        }
    }
}

#[derive(Clone, Copy)]
pub struct NoFactsUpdated<O>(O);

impl<O> NoFactsUpdated<O> {
    /// I am a facts updater and I hereby confirm I updated the order facts.
    pub fn order_facts_updated(self) -> OrderFactsUpdated<O> {
        OrderFactsUpdated(self)
    }
}

pub struct OrderFactsUpdated<O>(NoFactsUpdated<O>);

impl<O> OrderFactsUpdated<O> {
    /// I am a facts updater and I hereby confirm I updated the order facts.
    pub fn distinct_facts_updated(self) -> DistinctFactsUpdated<O> {
        DistinctFactsUpdated(self)
    }
}

pub struct DistinctFactsUpdated<O>(OrderFactsUpdated<O>);

pub type FactsFullyUpdated<O> = DistinctFactsUpdated<O>;

impl<O> FactsFullyUpdated<O> {
    pub fn unwrap(self) -> O {
        self.0 .0 .0
    }
}

impl FactsFullyUpdated<()> {
    pub fn with_output<O>(self, output: O) -> FactsFullyUpdated<O> {
        NoFactsUpdated(output)
            .order_facts_updated()
            .distinct_facts_updated()
    }
}

pub fn add_vec_datum_to_record_definition(
    record_definition: &mut QuirkyRecordDefinitionBuilder,
    field: &str,
    record_type: StreamRecordType,
    variant_id: RecordVariantId,
) -> Result<DatumId, ChainError> {
    record_definition
        .add_datum(
            field,
            QuirkyDatumType::Vec {
                record_type,
                variant_id,
            },
        )
        .map_err(|err| ChainError::Other { msg: err })
}

pub fn replace_vec_datum_in_record_definition(
    record_definition: &mut QuirkyRecordDefinitionBuilder,
    field: &str,
    record_type: StreamRecordType,
    variant_id: RecordVariantId,
) -> ChainResult<DatumId> {
    let old_datum = record_definition
        .get_current_datum_definition_by_name(field)
        .ok_or_else(|| ChainError::FieldNotFound {
            field: field.to_owned(),
        })?;
    record_definition
        .remove_datum(old_datum.id())
        .map_err(|err| ChainError::Other { msg: err })?;
    record_definition
        .add_datum(
            field,
            QuirkyDatumType::Vec {
                record_type,
                variant_id,
            },
        )
        .map_err(|err| ChainError::Other { msg: err })
}

pub fn set_order_fact<I, F>(
    facts: &mut StreamFacts,
    order_fields: I,
    record_definition: &QuirkyRecordDefinitionBuilder,
) -> ChainResult<()>
where
    I: IntoIterator<Item = Directed<F>>,
    F: AsRef<str>,
{
    // Ensure uniqueness, but keep order
    let mut seen = BTreeSet::<DatumId>::new();
    let order = order_fields
        .into_iter()
        .map(|field| {
            let datum_id = record_definition
                .get_current_datum_definition_by_name((*field).as_ref())
                .ok_or_else(|| ChainError::FieldNotFound {
                    field: (*field).as_ref().to_owned(),
                })?
                .id();
            Ok((!seen.contains(&datum_id)).then(|| {
                seen.insert(datum_id);
                field.as_ref().map(|_| datum_id)
            }))
        })
        .filter_map(|res| res.transpose())
        .collect::<Result<Vec<Directed<DatumId>>, _>>()?;
    assert_eq!(seen.len(), order.len());
    facts.set_order(order);
    Ok(())
}

pub fn break_order_fact_at<I, F>(
    facts: &mut StreamFacts,
    fields: I,
    record_definition: &QuirkyRecordDefinitionBuilder,
) -> ChainResult<()>
where
    I: IntoIterator<Item = F>,
    F: AsRef<str>,
{
    break_order_fact_at_ids(
        facts,
        fields
            .into_iter()
            .map(|field| {
                Ok(record_definition
                    .get_current_datum_definition_by_name(field.as_ref())
                    .ok_or_else(|| ChainError::FieldNotFound {
                        field: field.as_ref().to_owned(),
                    })?
                    .id())
            })
            .collect::<Result<Vec<_>, _>>()?,
    );
    Ok(())
}

pub fn break_order_fact_at_ids<I>(facts: &mut StreamFacts, datum_ids: I)
where
    I: IntoIterator<Item = DatumId>,
{
    let datum_ids = datum_ids.into_iter().collect::<BTreeSet<DatumId>>();
    facts.break_order_at_ids(&datum_ids);
}

pub fn check_undirected_order_starts_with(
    expected_order: &[DatumId],
    actual_order: &[Directed<DatumId>],
    record_definition: &QuirkyRecordDefinitionBuilder,
    more_info: &str,
) -> ChainResult<()> {
    assert!(expected_order.iter().all_unique());

    let is_ok = if actual_order.len() >= expected_order.len() {
        let mut sorted_actual = actual_order[0..expected_order.len()]
            .iter()
            // Ignore direction
            .map(Deref::deref)
            .copied()
            .collect::<Vec<DatumId>>();
        sorted_actual.sort();

        let mut sorted_expected = expected_order.to_vec();
        sorted_expected.sort();

        sorted_actual == sorted_expected
    } else {
        false
    };

    if !is_ok {
        let expected = "[".to_string()
            + &expected_order
                .iter()
                .map(|d| record_definition[*d].name())
                .join(", ")
            + "]";
        let actual = "[".to_string()
            + &actual_order
                .iter()
                .map(|d| d.as_ref().map(|d| record_definition[*d].name()))
                .join(", ")
            + "]";
        return Err(ChainError::ExpectedMinimalOrder {
            more_info: more_info.to_owned(),
            expected,
            actual,
        });
    }

    Ok(())
}

pub fn check_directed_order_starts_with(
    expected_order: &[DatumId],
    actual_order: &[Directed<DatumId>],
    record_definition: &QuirkyRecordDefinitionBuilder,
    more_info: &str,
) -> ChainResult<()> {
    assert!(expected_order.iter().all_unique());

    let expected_directed_order = expected_order
        .iter()
        .map(|d| Directed::Ascending(*d))
        .collect::<Vec<Directed<DatumId>>>();

    let is_ok = if actual_order.len() >= expected_order.len() {
        let actual_directed_order = &actual_order[0..expected_order.len()];

        actual_directed_order == expected_directed_order
    } else {
        false
    };

    if !is_ok {
        let expected = "[".to_string()
            + &expected_directed_order
                .iter()
                .map(|d| d.as_ref().map(|d| record_definition[*d].name()))
                .join(", ")
            + "]";
        let actual = "[".to_string()
            + &actual_order
                .iter()
                .map(|d| d.as_ref().map(|d| record_definition[*d].name()))
                .join(", ")
            + "]";
        return Err(ChainError::ExpectedMinimalOrder {
            more_info: more_info.to_owned(),
            expected,
            actual,
        });
    }

    Ok(())
}

pub fn set_distinct_fact<I, F>(
    facts: &mut StreamFacts,
    distinct_fields: I,
    record_definition: &QuirkyRecordDefinitionBuilder,
) -> ChainResult<()>
where
    I: IntoIterator<Item = F>,
    F: AsRef<str>,
{
    set_distinct_fact_ids(
        facts,
        distinct_fields
            .into_iter()
            .map(|field| {
                let datum_id = record_definition
                    .get_current_datum_definition_by_name(field.as_ref())
                    .ok_or_else(|| ChainError::FieldNotFound {
                        field: field.as_ref().to_owned(),
                    })?
                    .id();
                Ok(datum_id)
            })
            .collect::<Result<Vec<_>, _>>()?,
    );
    Ok(())
}

pub fn set_distinct_fact_ids<I>(facts: &mut StreamFacts, distinct_datum_ids: I)
where
    I: IntoIterator<Item = DatumId>,
{
    // Ensure uniqueness and sort
    let distinct = distinct_datum_ids
        .into_iter()
        .collect::<BTreeSet<DatumId>>();
    facts.set_distinct(distinct.into_iter().collect());
}

pub fn set_distinct_fact_all_fields(
    facts: &mut StreamFacts,
    record_definition: &QuirkyRecordDefinitionBuilder,
) {
    let distinct = record_definition.get_current_data();
    facts.set_distinct(distinct.collect());
}

pub fn break_distinct_fact_for<I, F>(
    facts: &mut StreamFacts,
    fields: I,
    record_definition: &QuirkyRecordDefinitionBuilder,
) -> ChainResult<()>
where
    I: IntoIterator<Item = F>,
    F: AsRef<str>,
{
    break_distinct_fact_for_ids(
        facts,
        fields
            .into_iter()
            .map(|field| {
                Ok(record_definition
                    .get_current_datum_definition_by_name(field.as_ref())
                    .ok_or_else(|| ChainError::FieldNotFound {
                        field: field.as_ref().to_owned(),
                    })?
                    .id())
            })
            .collect::<Result<Vec<_>, _>>()?,
    );
    Ok(())
}

pub fn break_distinct_fact_for_ids<I>(facts: &mut StreamFacts, datum_ids: I)
where
    I: IntoIterator<Item = DatumId>,
{
    for datum_id in datum_ids {
        if facts.distinct().contains(&datum_id) {
            facts.set_distinct(Vec::new());
            return;
        }
    }
}

pub fn check_distinct_eq(
    expected_distinct: &[DatumId],
    actual_distinct: &[DatumId],
    record_definition: &QuirkyRecordDefinitionBuilder,
    more_info: &str,
) -> ChainResult<()> {
    assert!(expected_distinct.iter().all_unique());

    let is_ok = {
        let mut sorted_actual = actual_distinct.to_vec();
        sorted_actual.sort();

        let mut sorted_expected = expected_distinct.to_vec();
        sorted_expected.sort();

        sorted_actual == sorted_expected
    };

    if !is_ok {
        let expected = "[".to_string()
            + &expected_distinct
                .iter()
                .map(|d| record_definition[*d].name())
                .join(", ")
            + "]";
        let actual = "[".to_string()
            + &actual_distinct
                .iter()
                .map(|d| record_definition[*d].name())
                .join(", ")
            + "]";
        return Err(ChainError::ExpectedDistinct {
            more_info: more_info.to_owned(),
            expected,
            actual,
        });
    }

    Ok(())
}
