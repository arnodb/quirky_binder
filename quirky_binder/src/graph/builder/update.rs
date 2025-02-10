use std::{cell::RefCell, collections::BTreeMap};

use truc::record::{
    definition::{DatumDefinition, DatumId, RecordDefinitionBuilder, RecordVariantId},
    type_resolver::TypeResolver,
};

use super::{
    add_vec_datum_to_record_definition, break_distinct_fact_for, break_distinct_fact_for_ids,
    break_order_fact_at, break_order_fact_at_ids, replace_vec_datum_in_record_definition,
    set_distinct_fact, set_distinct_fact_all_fields, set_distinct_fact_ids, set_order_fact,
    FactsFullyUpdated, NoFactsUpdated,
};
use crate::{
    prelude::*,
    stream::{NodeSubStream, StreamFacts},
    trace_element,
};

#[derive(Getters, CopyGetters)]
pub struct OutputBuilderForUpdate<'a, 'b, 'g, R: TypeResolver + Copy, Extra> {
    pub(super) streams: &'b mut StreamsBuilder<'a>,
    #[getset(get = "pub")]
    pub(super) record_type: StreamRecordType,
    #[getset(get_copy = "pub")]
    pub(super) record_definition: &'g RefCell<RecordDefinitionBuilder<R>>,
    pub(super) sub_streams: BTreeMap<DatumId, NodeSubStream>,
    pub(super) source: NodeStreamSource,
    pub(super) is_output_main_stream: bool,
    #[getset(get = "pub")]
    pub(super) facts: StreamFacts,
    pub(super) extra: Extra,
}

impl<'g, R: TypeResolver + Copy, Extra> OutputBuilderForUpdate<'_, '_, 'g, R, Extra> {
    pub fn sub_stream(&self, datum_id: DatumId) -> &NodeSubStream {
        &self.sub_streams[&datum_id]
    }

    pub fn new_named_sub_stream(
        &self,
        name: &str,
        graph: &'g GraphBuilder<R>,
    ) -> ChainResult<SubStreamBuilderForUpdate<'g, R, ()>> {
        let full_name = self.streams.name.sub(name);
        self.new_sub_stream_internal(graph, full_name)
    }

    fn new_sub_stream_internal(
        &self,
        graph: &'g GraphBuilder<R>,
        full_name: FullyQualifiedName,
    ) -> ChainResult<SubStreamBuilderForUpdate<'g, R, ()>> {
        let record_type = StreamRecordType::from(full_name);
        let record_definition = graph.get_stream(&record_type)?;
        Ok(SubStreamBuilderForUpdate {
            record_type,
            record_definition,
            sub_streams: BTreeMap::new(),
            facts: StreamFacts::default(),
            extra: (),
        })
    }

    pub fn update_sub_stream<B>(
        &mut self,
        sub_stream: NodeSubStream,
        graph: &'g GraphBuilder<R>,
        build: B,
        trace_name: &str,
    ) -> ChainResultWithTrace<NodeSubStream>
    where
        B: FnOnce(
            &mut Self,
            &mut SubStreamBuilderForUpdate<'g, R, DerivedExtra>,
            NoFactsUpdated<()>,
        ) -> ChainResultWithTrace<FactsFullyUpdated<()>>,
    {
        let record_definition = graph
            .get_stream(sub_stream.record_type())
            .with_trace_element(trace_element!(trace_name))?;
        let (record_type, variant_id, sub_streams, facts) = sub_stream.destructure();
        let mut builder = SubStreamBuilderForUpdate {
            record_type,
            record_definition,
            sub_streams,
            facts,
            extra: DerivedExtra {
                input_variant_id: variant_id,
            },
        };
        let facts = build(self, &mut builder, NoFactsUpdated(()))?;
        Ok(builder.close_record_variant(facts))
    }

    pub fn update_path<UpdateLeafSubStream, UpdatePathStream, UpdateRootStream>(
        &mut self,
        path_fields: &[ValidFieldName],
        update_leaf_sub_stream: UpdateLeafSubStream,
        update_path_stream: UpdatePathStream,
        update_root_stream: UpdateRootStream,
        graph: &'g GraphBuilder<R>,
        trace_name: &str,
    ) -> ChainResultWithTrace<Vec<PathUpdateElement>>
    where
        UpdateLeafSubStream: for<'c, 'd> FnOnce(
            NodeSubStream,
            &mut OutputBuilderForUpdate<'c, 'd, 'g, R, Extra>,
        ) -> ChainResultWithTrace<NodeSubStream>,
        UpdatePathStream: Fn(
            &str,
            &mut SubStreamBuilderForUpdate<'g, R, DerivedExtra>,
            NodeSubStream,
            NoFactsUpdated<()>,
        ) -> ChainResultWithTrace<FactsFullyUpdated<()>>,
        UpdateRootStream: for<'c, 'd> FnOnce(
            &str,
            &mut OutputBuilderForUpdate<'c, 'd, 'g, R, Extra>,
            NodeSubStream,
        ) -> ChainResultWithTrace<()>,
    {
        struct PathFieldDetails<'a> {
            stream: NodeSubStream,
            field: &'a ValidFieldName,
            datum_id: DatumId,
        }

        // Find the sub stream of the first path field
        let (root_field, root_datum_id, root_sub_stream, root_sub_stream_record_definition) = {
            let field = path_fields.first().expect("first path field");
            let datum_id = self
                .record_definition
                .borrow()
                .get_current_datum_definition_by_name(field.name())
                .map(DatumDefinition::id);
            if let Some(datum_id) = datum_id {
                let sub_stream = self.sub_streams.remove(&datum_id).expect("root sub stream");
                let sub_record_definition = graph
                    .get_stream(sub_stream.record_type())
                    .with_trace_element(trace_element!(trace_name))?;
                (field, datum_id, sub_stream, sub_record_definition)
            } else {
                return Err(ChainError::FieldNotFound {
                    field: field.name().to_owned(),
                })
                .with_trace_element(trace_element!(trace_name));
            }
        };

        // Then find path input streams
        let (mut path_details, leaf_sub_input_stream, _) = path_fields[1..].iter().try_fold(
            (
                Vec::new(),
                root_sub_stream,
                root_sub_stream_record_definition,
            ),
            |(mut path_data, mut stream, record_definition), field| {
                let datum_id = record_definition
                    .borrow()
                    .get_current_datum_definition_by_name(field.name())
                    .map(DatumDefinition::id);
                if let Some(datum_id) = datum_id {
                    let sub_stream = stream
                        .sub_streams_mut()
                        .remove(&datum_id)
                        .expect("sub stream");
                    let sub_record_definition = graph
                        .get_stream(sub_stream.record_type())
                        .with_trace_element(trace_element!(trace_name))?;
                    path_data.push(PathFieldDetails {
                        stream,
                        field,
                        datum_id,
                    });
                    Ok((path_data, sub_stream, sub_record_definition))
                } else {
                    Err(ChainError::FieldNotFound {
                        field: field.name().to_owned(),
                    })
                    .with_trace_element(trace_element!(trace_name))
                }
            },
        )?;

        // Then update the leaf sub stream
        let mut sub_input_stream = Some(leaf_sub_input_stream.clone());
        let mut sub_output_stream = Some(update_leaf_sub_stream(leaf_sub_input_stream, self)?);

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
                    field: field_details.field.clone(),
                    sub_input_stream: sub_input_stream.take().expect("sub_input_stream"),
                    sub_output_stream: sub_stream.clone(),
                });

                let updated_stream = self.update_sub_stream(
                    field_details.stream.clone(),
                    graph,
                    |_, path_stream, facts_proof| {
                        update_path_stream(
                            field_details.field.name(),
                            path_stream,
                            sub_stream,
                            facts_proof,
                        )
                    },
                    trace_name,
                )?;

                sub_input_stream = Some(field_details.stream);
                sub_output_stream = Some(updated_stream);
            }
        }

        {
            let sub_stream = sub_output_stream.take().expect("sub_output_stream");
            let old = self.sub_streams.insert(root_datum_id, sub_stream.clone());
            if old.is_some() {
                panic!("sub stream should have been removed");
            }
            path_update_streams.push(PathUpdateElement {
                field: root_field.clone(),
                sub_input_stream: sub_input_stream.take().expect("sub_input_stream"),
                sub_output_stream: sub_stream.clone(),
            });
            update_root_stream(root_field.name(), self, sub_stream)?;
        }

        // They were pushed in reverse order
        path_update_streams.reverse();

        Ok(path_update_streams)
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

    pub fn replace_vec_datum(
        &mut self,
        field: &str,
        record: &str,
        sub_stream: NodeSubStream,
    ) -> ChainResult<()> {
        let (old_datum_id, new_datum_id) = replace_vec_datum_in_record_definition(
            &mut self.record_definition.borrow_mut(),
            field,
            record,
        )?;
        let old = self.sub_streams.remove(&old_datum_id);
        if old.is_none() {
            panic!("the replaced datum should be registered");
        }
        let old = self.sub_streams.insert(new_datum_id, sub_stream);
        if old.is_some() {
            panic!("the datum should not be registered yet");
        }
        Ok(())
    }

    pub fn set_order_fact<I, F>(&mut self, order_fields: I) -> ChainResult<()>
    where
        I: IntoIterator<Item = Directed<F>>,
        F: AsRef<str>,
    {
        set_order_fact(
            &mut self.facts,
            order_fields,
            &*self.record_definition.borrow(),
        )
    }

    pub fn break_order_fact_at<I, F>(&mut self, fields: I) -> ChainResult<()>
    where
        I: IntoIterator<Item = F>,
        F: AsRef<str>,
    {
        break_order_fact_at(&mut self.facts, fields, &*self.record_definition.borrow())
    }

    pub fn break_order_fact_at_ids<I>(&mut self, datum_ids: I)
    where
        I: IntoIterator<Item = DatumId>,
    {
        break_order_fact_at_ids(&mut self.facts, datum_ids);
    }

    pub fn set_distinct_fact<I, F>(&mut self, distinct_fields: I) -> ChainResult<()>
    where
        I: IntoIterator<Item = F>,
        F: AsRef<str>,
    {
        set_distinct_fact(
            &mut self.facts,
            distinct_fields,
            &*self.record_definition.borrow(),
        )
    }

    pub fn set_distinct_fact_ids<I>(&mut self, distinct_datum_ids: I)
    where
        I: IntoIterator<Item = DatumId>,
    {
        set_distinct_fact_ids(&mut self.facts, distinct_datum_ids);
    }

    pub fn set_distinct_fact_all_fields(&mut self) {
        set_distinct_fact_all_fields(&mut self.facts, &*self.record_definition.borrow());
    }

    pub fn break_distinct_fact_for<I, F>(&mut self, fields: I) -> ChainResult<()>
    where
        I: IntoIterator<Item = F>,
        F: AsRef<str>,
    {
        break_distinct_fact_for(&mut self.facts, fields, &*self.record_definition.borrow())
    }

    pub fn break_distinct_fact_for_ids<I>(&mut self, datum_ids: I)
    where
        I: IntoIterator<Item = DatumId>,
    {
        break_distinct_fact_for_ids(&mut self.facts, datum_ids);
    }

    pub fn build(self) -> &'g RefCell<RecordDefinitionBuilder<R>> {
        let variant_id = close_record_variant_internal(&mut self.record_definition.borrow_mut());

        self.streams.outputs.push(NodeStream::new(
            self.record_type,
            variant_id,
            self.sub_streams,
            self.source,
            self.is_output_main_stream,
            self.facts,
        ));

        self.record_definition
    }
}

impl<R: TypeResolver + Copy> OutputBuilderForUpdate<'_, '_, '_, R, DerivedExtra> {
    pub fn input_variant_id(&self) -> RecordVariantId {
        self.extra.input_variant_id
    }
}

pub struct PathUpdateElement {
    pub field: ValidFieldName,
    pub sub_input_stream: NodeSubStream,
    pub sub_output_stream: NodeSubStream,
}

#[derive(Getters, CopyGetters, MutGetters)]
pub struct SubStreamBuilderForUpdate<'g, R: TypeResolver, Extra> {
    #[getset(get = "pub")]
    record_type: StreamRecordType,
    #[getset(get_copy = "pub")]
    record_definition: &'g RefCell<RecordDefinitionBuilder<R>>,
    sub_streams: BTreeMap<DatumId, NodeSubStream>,
    #[getset(get = "pub", get_mut = "pub")]
    facts: StreamFacts,
    extra: Extra,
}

impl<R: TypeResolver, Extra> SubStreamBuilderForUpdate<'_, R, Extra> {
    pub fn sub_stream(&self, datum_id: DatumId) -> &NodeSubStream {
        &self.sub_streams[&datum_id]
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

    pub fn replace_vec_datum(
        &mut self,
        field: &str,
        record: &str,
        sub_stream: NodeSubStream,
    ) -> ChainResult<()> {
        let (old_datum_id, new_datum_id) = replace_vec_datum_in_record_definition(
            &mut self.record_definition.borrow_mut(),
            field,
            record,
        )?;
        let old = self.sub_streams.remove(&old_datum_id);
        if old.is_none() {
            panic!("the replaced datum should be registered");
        }
        let old = self.sub_streams.insert(new_datum_id, sub_stream);
        if old.is_some() {
            panic!("the datum should not be registered yet");
        }
        Ok(())
    }

    pub fn set_order_fact<I, F>(&mut self, order_fields: I) -> ChainResult<()>
    where
        I: IntoIterator<Item = Directed<F>>,
        F: AsRef<str>,
    {
        set_order_fact(
            &mut self.facts,
            order_fields,
            &*self.record_definition.borrow(),
        )
    }

    pub fn break_order_fact_at<I, F>(&mut self, fields: I) -> ChainResult<()>
    where
        I: IntoIterator<Item = F>,
        F: AsRef<str>,
    {
        break_order_fact_at(&mut self.facts, fields, &*self.record_definition.borrow())
    }

    pub fn break_order_fact_at_ids<I>(&mut self, datum_ids: I)
    where
        I: IntoIterator<Item = DatumId>,
    {
        break_order_fact_at_ids(&mut self.facts, datum_ids);
    }

    pub fn set_distinct_fact<I, F>(&mut self, distinct_fields: I) -> ChainResult<()>
    where
        I: IntoIterator<Item = F>,
        F: AsRef<str>,
    {
        set_distinct_fact(
            &mut self.facts,
            distinct_fields,
            &*self.record_definition.borrow(),
        )
    }

    pub fn set_distinct_fact_ids<I>(&mut self, distinct_datum_ids: I)
    where
        I: IntoIterator<Item = DatumId>,
    {
        set_distinct_fact_ids(&mut self.facts, distinct_datum_ids);
    }

    pub fn break_distinct_fact_for<I, F>(&mut self, fields: I) -> ChainResult<()>
    where
        I: IntoIterator<Item = F>,
        F: AsRef<str>,
    {
        break_distinct_fact_for(&mut self.facts, fields, &*self.record_definition.borrow())
    }

    pub fn close_record_variant(self, _facts: FactsFullyUpdated<()>) -> NodeSubStream {
        let variant_id = close_record_variant_internal(&mut self.record_definition.borrow_mut());

        NodeSubStream::new(self.record_type, variant_id, self.sub_streams, self.facts)
    }
}

impl<R: TypeResolver> SubStreamBuilderForUpdate<'_, R, DerivedExtra> {
    pub fn input_variant_id(&self) -> RecordVariantId {
        self.extra.input_variant_id
    }
}

fn close_record_variant_internal<R>(builder: &mut RecordDefinitionBuilder<R>) -> RecordVariantId
where
    R: TypeResolver,
{
    #[cfg(not(feature = "fuzz_data_order"))]
    let variant_id = builder.close_record_variant();

    #[cfg(feature = "fuzz_data_order")]
    let variant_id = match std::env::var("FUZZ_DATA_ORDER") {
        Ok(_) => {
            use truc::record::definition::builder::variant::append_data_reverse;
            // With this one, it is almost assured that record data have an unexpected order
            builder.close_record_variant_with(append_data_reverse)
        }
        Err(_) => builder.close_record_variant(),
    };

    variant_id
}
