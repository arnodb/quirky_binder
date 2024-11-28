use std::{
    cell::RefCell,
    collections::{btree_map::Entry, BTreeMap, BTreeSet},
    ops::Deref,
};

use itertools::Itertools;
use truc::record::{
    definition::{DatumDefinitionOverride, DatumId, RecordDefinitionBuilder, RecordVariantId},
    type_resolver::TypeResolver,
};

use self::{
    params::ParamsBuilder, pass_through::OutputBuilderForPassThrough,
    update::OutputBuilderForUpdate,
};
use crate::{prelude::*, trace_element};

pub mod params;
pub mod pass_through;
pub mod update;

#[derive(new, Getters)]
pub struct GraphBuilder<R: TypeResolver + Copy> {
    type_resolver: R,
    chain_customizer: ChainCustomizer,
    #[new(default)]
    record_definitions: BTreeMap<StreamRecordType, RefCell<RecordDefinitionBuilder<R>>>,
    #[new(default)]
    #[getset(get = "pub")]
    params: ParamsBuilder,
    #[new(default)]
    anchor_table_count: usize,
}

impl<R: TypeResolver + Copy> GraphBuilder<R> {
    pub fn new_stream(&mut self, record_type: StreamRecordType) -> ChainResult<()> {
        match self.record_definitions.entry(record_type) {
            Entry::Vacant(entry) => {
                let record_definition_builder = RecordDefinitionBuilder::new(self.type_resolver);
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
    ) -> ChainResult<&RefCell<RecordDefinitionBuilder<R>>> {
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

    pub fn new_main_stream<R: TypeResolver + Copy>(
        &mut self,
        graph: &mut GraphBuilder<R>,
    ) -> ChainResult<()> {
        let full_name = self.name.clone();
        let record_type = StreamRecordType::from(full_name);
        graph.new_stream(record_type)
    }

    pub fn new_named_stream<R: TypeResolver + Copy>(
        &mut self,
        name: &str,
        graph: &mut GraphBuilder<R>,
    ) -> ChainResult<()> {
        let full_name = self.name.sub(name);
        let record_type = StreamRecordType::from(full_name);
        graph.new_stream(record_type)
    }

    pub fn new_main_output<'b, 'g, R: TypeResolver + Copy>(
        &'b mut self,
        graph: &'g GraphBuilder<R>,
    ) -> ChainResult<OutputBuilder<'a, 'b, 'g, R, ()>> {
        let full_name = self.name.clone();
        self.new_output_internal(graph, full_name, true)
    }

    pub fn new_named_output<'b, 'g, R: TypeResolver + Copy>(
        &'b mut self,
        name: &str,
        graph: &'g GraphBuilder<R>,
    ) -> ChainResult<OutputBuilder<'a, 'b, 'g, R, ()>> {
        let full_name = self.name.sub(name);
        self.new_output_internal(graph, full_name, false)
    }

    fn new_output_internal<'b, 'g, R: TypeResolver + Copy>(
        &'b mut self,
        graph: &'g GraphBuilder<R>,
        full_name: FullyQualifiedName,
        is_output_main_stream: bool,
    ) -> ChainResult<OutputBuilder<'a, 'b, 'g, R, ()>> {
        let record_type = StreamRecordType::from(full_name.clone());
        let record_definition = graph.get_stream(&record_type)?;
        Ok(OutputBuilder {
            streams: self,
            record_type,
            record_definition,
            input_sub_streams: BTreeMap::new(),
            source: full_name.into(),
            is_output_main_stream,
            facts: StreamFacts::default(),
            extra: (),
        })
    }

    pub fn output_from_input<'b, 'g, R: TypeResolver + Copy>(
        &'b mut self,
        input_index: usize,
        is_output_main_stream: bool,
        graph: &'g GraphBuilder<R>,
    ) -> ChainResult<OutputBuilder<'a, 'b, 'g, R, DerivedExtra>> {
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
        let source = self.name.clone().into();
        Ok(OutputBuilder {
            streams: self,
            record_type: input.record_type().clone(),
            record_definition,
            input_sub_streams: input.sub_streams().clone(),
            source,
            is_output_main_stream,
            facts: input.facts().clone(),
            extra: DerivedExtra {
                input_variant_id: input.variant_id(),
            },
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
#[derive(Getters)]
pub struct OutputBuilder<'a, 'b, 'g, R: TypeResolver, Extra> {
    streams: &'b mut StreamsBuilder<'a>,
    #[getset(get = "pub")]
    record_type: StreamRecordType,
    record_definition: &'g RefCell<RecordDefinitionBuilder<R>>,
    input_sub_streams: BTreeMap<DatumId, NodeSubStream>,
    source: NodeStreamSource,
    is_output_main_stream: bool,
    facts: StreamFacts,
    extra: Extra,
}

impl<'a, 'b, 'g, R: TypeResolver + Copy, Extra> OutputBuilder<'a, 'b, 'g, R, Extra> {
    pub fn update<B, O>(self, build: B) -> ChainResultWithTrace<O>
    where
        B: FnOnce(
            &mut OutputBuilderForUpdate<'a, 'b, 'g, R, Extra>,
            NoFactsUpdated<()>,
        ) -> ChainResultWithTrace<FactsFullyUpdated<O>>,
    {
        let mut builder = OutputBuilderForUpdate {
            streams: self.streams,
            record_type: self.record_type,
            record_definition: self.record_definition,
            sub_streams: self.input_sub_streams,
            source: self.source,
            is_output_main_stream: self.is_output_main_stream,
            facts: self.facts,
            extra: self.extra,
        };
        let output = build(&mut builder, NoFactsUpdated(()))?;
        builder.build();
        Ok(output.unwrap())
    }

    pub fn update_path<B>(
        self,
        graph: &'g GraphBuilder<R>,
        path_fields: &[ValidFieldName],
        build: B,
        trace_name: &str,
    ) -> ChainResultWithTrace<Vec<PathUpdateElement>>
    where
        B: for<'c, 'd> FnOnce(
            &mut OutputBuilderForUpdate<'c, 'd, 'g, R, Extra>,
            &mut SubStreamBuilderForUpdate<'g, R, DerivedExtra>,
            NoFactsUpdated<()>,
        ) -> ChainResultWithTrace<FactsFullyUpdated<()>>,
    {
        self.update(|output_stream, facts_proof| {
            let path_streams = output_stream.update_path(
                path_fields,
                |sub_input_stream, output_stream| {
                    output_stream.update_sub_stream(sub_input_stream, graph, build, trace_name)
                },
                |field: &str, path_stream, sub_output_stream, facts_proof| {
                    let module_name = graph
                        .chain_customizer()
                        .streams_module_name
                        .sub_n(&***sub_output_stream.record_type());
                    let record = &format!(
                        "{module_name}::Record{group_variant_id}",
                        module_name = module_name,
                        group_variant_id = sub_output_stream.variant_id(),
                    );
                    path_stream
                        .replace_vec_datum(field, record, sub_output_stream)
                        .with_trace_element(trace_element!(trace_name))?;
                    Ok(facts_proof.order_facts_updated().distinct_facts_updated())
                },
                |field: &str, output_stream, sub_output_stream| {
                    let module_name = graph
                        .chain_customizer()
                        .streams_module_name
                        .sub_n(&***sub_output_stream.record_type());
                    let record = &format!(
                        "{module_name}::Record{group_variant_id}",
                        module_name = module_name,
                        group_variant_id = sub_output_stream.variant_id(),
                    );
                    output_stream
                        .replace_vec_datum(field, record, sub_output_stream)
                        .with_trace_element(trace_element!(trace_name))?;
                    Ok(())
                },
                graph,
                trace_name,
            )?;

            Ok(facts_proof
                .order_facts_updated()
                .distinct_facts_updated()
                .with_output(path_streams))
        })
    }
}

impl<'a, 'b, 'g, R: TypeResolver + Copy> OutputBuilder<'a, 'b, 'g, R, DerivedExtra> {
    pub fn pass_through<B, O>(self, build: B) -> ChainResultWithTrace<O>
    where
        B: FnOnce(
            &mut OutputBuilderForPassThrough<'a, 'b, 'g, R>,
            NoFactsUpdated<()>,
        ) -> ChainResultWithTrace<FactsFullyUpdated<O>>,
    {
        let mut builder = OutputBuilderForPassThrough {
            streams: self.streams,
            record_type: self.record_type,
            record_definition: self.record_definition,
            input_variant_id: self.extra.input_variant_id,
            sub_streams: self.input_sub_streams,
            source: self.source,
            is_output_main_stream: self.is_output_main_stream,
            facts: self.facts,
        };
        let output = build(&mut builder, NoFactsUpdated(()))?;
        builder.build();
        Ok(output.unwrap())
    }

    pub fn pass_through_path<B>(
        self,
        graph: &'g GraphBuilder<R>,
        path_fields: &[ValidFieldName],
        build: B,
        trace_name: &str,
    ) -> ChainResultWithTrace<NodeSubStream>
    where
        B: FnOnce(&mut SubStreamBuilderForPassThrough<'g, R>) -> ChainResultWithTrace<()>,
    {
        self.pass_through(|output_stream, facts_proof| {
            let path_sub_stream = output_stream.pass_through_path(
                path_fields,
                |sub_input_stream, output_stream| {
                    output_stream
                        .pass_through_sub_stream(sub_input_stream, graph, build, trace_name)
                        .with_trace_element(trace_element!(trace_name))
                },
                graph,
                trace_name,
            )?;
            Ok(facts_proof
                .order_facts_updated()
                .distinct_facts_updated()
                .with_output(path_sub_stream))
        })
    }
}

pub struct DerivedExtra {
    input_variant_id: RecordVariantId,
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
    fn unwrap(self) -> O {
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
) -> ChainResult<(DatumId, DatumId)> {
    let old_datum = record_definition
        .get_current_datum_definition_by_name(field)
        .ok_or_else(|| ChainError::FieldNotFound {
            field: field.to_owned(),
        })?;
    let old_datum_id = old_datum.id();
    record_definition.remove_datum(old_datum_id);
    Ok((
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
    ))
}

pub fn set_order_fact<R: TypeResolver, I, F>(
    facts: &mut StreamFacts,
    order_fields: I,
    record_definition: &RecordDefinitionBuilder<R>,
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

pub fn break_order_fact_at<R: TypeResolver, I, F>(
    facts: &mut StreamFacts,
    fields: I,
    record_definition: &RecordDefinitionBuilder<R>,
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

pub fn check_undirected_order_starts_with<R>(
    expected_order: &[DatumId],
    actual_order: &[Directed<DatumId>],
    record_definition: &RecordDefinitionBuilder<R>,
    more_info: &str,
) -> ChainResult<()>
where
    R: TypeResolver,
{
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

pub fn check_directed_order_starts_with<R>(
    expected_order: &[DatumId],
    actual_order: &[Directed<DatumId>],
    record_definition: &RecordDefinitionBuilder<R>,
    more_info: &str,
) -> ChainResult<()>
where
    R: TypeResolver,
{
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

pub fn set_distinct_fact<R: TypeResolver, I, F>(
    facts: &mut StreamFacts,
    distinct_fields: I,
    record_definition: &RecordDefinitionBuilder<R>,
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

pub fn set_distinct_fact_all_fields<R: TypeResolver>(
    facts: &mut StreamFacts,
    record_definition: &RecordDefinitionBuilder<R>,
) {
    let distinct = record_definition.get_current_data();
    facts.set_distinct(distinct.collect());
}

pub fn break_distinct_fact_for<R: TypeResolver, I, F>(
    facts: &mut StreamFacts,
    fields: I,
    record_definition: &RecordDefinitionBuilder<R>,
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
        if facts.distinct().iter().any(|d| *d == datum_id) {
            facts.set_distinct(Vec::new());
            return;
        }
    }
}

pub fn check_distinct_eq<R>(
    expected_distinct: &[DatumId],
    actual_distinct: &[DatumId],
    record_definition: &RecordDefinitionBuilder<R>,
    more_info: &str,
) -> ChainResult<()>
where
    R: TypeResolver,
{
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
