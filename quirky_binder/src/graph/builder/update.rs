use std::{cell::RefCell, collections::BTreeMap};

use truc::record::definition::{
    builder::generic::variant, DatumDefinition, DatumId, RecordVariantId,
};

use super::{
    break_distinct_fact_for, break_distinct_fact_for_ids, break_order_fact_at,
    break_order_fact_at_ids, set_distinct_fact, set_distinct_fact_all_fields,
    set_distinct_fact_ids, set_order_fact, FactsFullyUpdated, NoFactsUpdated,
};
use crate::{prelude::*, trace_element};

#[derive(Getters, CopyGetters, MutGetters)]
pub struct OutputBuilderForUpdate<'g, Extra> {
    #[getset(get = "pub")]
    pub(super) record_type: StreamRecordType,
    #[getset(get_copy = "pub")]
    pub(super) record_definition: &'g RefCell<QuirkyRecordDefinitionBuilder>,
    #[getset(get = "pub", get_mut = "pub")]
    pub(super) sub_streams: BTreeMap<DatumId, NodeSubStream>,
    pub(super) source: NodeStreamSource,
    pub(super) is_output_main_stream: bool,
    #[getset(get = "pub")]
    pub(super) facts: StreamFacts,
    pub(super) extra: Extra,
}

impl<'g, Extra> OutputBuilderForUpdate<'g, Extra> {
    pub fn sub_stream(&self, datum_id: DatumId) -> &NodeSubStream {
        &self.sub_streams[&datum_id]
    }

    pub fn update_sub_stream<B>(
        &mut self,
        sub_stream: NodeSubStream,
        graph: &'g GraphBuilder,
        build: B,
    ) -> ChainResultWithTrace<NodeSubStream>
    where
        B: FnOnce(
            &mut SubStreamBuilderForUpdate<'g, DerivedExtra>,
            NoFactsUpdated<()>,
        ) -> ChainResultWithTrace<FactsFullyUpdated<()>>,
    {
        let record_definition = graph
            .get_stream(sub_stream.record_type())
            .with_trace_element(trace_element!())?;
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
        let facts = build(&mut builder, NoFactsUpdated(()))?;
        Ok(builder.close_record_variant(facts))
    }

    pub fn update_path<UpdateLeafSubStream, UpdatePathStream, UpdateRootStream>(
        &mut self,
        path_fields: &[ValidFieldName],
        update_leaf_sub_stream: UpdateLeafSubStream,
        update_path_stream: UpdatePathStream,
        update_root_stream: UpdateRootStream,
        graph: &'g GraphBuilder,
    ) -> ChainResultWithTrace<Vec<PathUpdateElement>>
    where
        UpdateLeafSubStream: FnOnce(
            NodeSubStream,
            &mut OutputBuilderForUpdate<'g, Extra>,
        ) -> ChainResultWithTrace<NodeSubStream>,
        UpdatePathStream: Fn(
            &str,
            &mut SubStreamBuilderForUpdate<'g, DerivedExtra>,
            &NodeSubStream,
            NoFactsUpdated<()>,
        ) -> ChainResultWithTrace<(DatumId, FactsFullyUpdated<()>)>,
        UpdateRootStream: FnOnce(
            &str,
            &mut OutputBuilderForUpdate<'g, Extra>,
            &NodeSubStream,
        ) -> ChainResultWithTrace<DatumId>,
    {
        #[derive(Debug)]
        struct PathFieldDetails<'a> {
            stream: NodeSubStream,
            field: &'a ValidFieldName,
        }

        // Find the sub stream of the first path field
        let (root_field, root_sub_stream, root_sub_stream_record_definition) = {
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
                    .with_trace_element(trace_element!())?;
                (field, sub_stream, sub_record_definition)
            } else {
                return Err(ChainError::FieldNotFound {
                    field: field.name().to_owned(),
                })
                .with_trace_element(trace_element!());
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
                        .with_trace_element(trace_element!())?;
                    path_data.push(PathFieldDetails { stream, field });
                    Ok((path_data, sub_stream, sub_record_definition))
                } else {
                    Err(ChainError::FieldNotFound {
                        field: field.name().to_owned(),
                    })
                    .with_trace_element(trace_element!())
                }
            },
        )?;

        // Then update the leaf sub stream
        let mut sub_input_stream = StreamInfo::from(&leaf_sub_input_stream);
        let mut sub_output_stream = update_leaf_sub_stream(leaf_sub_input_stream, self)?;

        // Then all streams up to the root
        let mut path_update_streams = Vec::<PathUpdateElement>::with_capacity(path_fields.len());

        while let Some(field_details) = path_details.pop() {
            let updated_sub_stream = sub_output_stream;

            path_update_streams.push(PathUpdateElement {
                field: field_details.field.clone(),
                sub_input_stream,
                sub_output_stream: StreamInfo::from(&updated_sub_stream),
            });

            sub_input_stream = StreamInfo::from(&field_details.stream);

            let mut new_datum_id = None;

            let mut updated_stream =
                self.update_sub_stream(field_details.stream, graph, |path_stream, facts_proof| {
                    let (id, facts_proof) = update_path_stream(
                        field_details.field.name(),
                        path_stream,
                        &updated_sub_stream,
                        facts_proof,
                    )?;
                    new_datum_id = Some(id);
                    Ok(facts_proof)
                })?;

            let None = updated_stream
                .sub_streams_mut()
                .insert(new_datum_id.expect("new datum id"), updated_sub_stream)
            else {
                return Err(ChainError::Other {
                    msg: "sub stream should have been removed".to_owned(),
                })
                .with_trace_element(trace_element!());
            };

            sub_output_stream = updated_stream;
        }

        {
            let updated_sub_stream = sub_output_stream;

            path_update_streams.push(PathUpdateElement {
                field: root_field.clone(),
                sub_input_stream,
                sub_output_stream: StreamInfo::from(&updated_sub_stream),
            });

            let new_root_datum_id =
                update_root_stream(root_field.name(), self, &updated_sub_stream)?;

            let None = self
                .sub_streams
                .insert(new_root_datum_id, updated_sub_stream)
            else {
                return Err(ChainError::Other {
                    msg: "sub stream should have been removed".to_owned(),
                })
                .with_trace_element(trace_element!());
            };
        }

        // They were pushed in reverse order
        path_update_streams.reverse();

        Ok(path_update_streams)
    }

    pub fn set_order_fact<I, F>(&mut self, order_fields: I) -> ChainResult<()>
    where
        I: IntoIterator<Item = Directed<F>>,
        F: AsRef<str>,
    {
        set_order_fact(
            &mut self.facts,
            order_fields,
            &self.record_definition.borrow(),
        )
    }

    pub fn break_order_fact_at<I, F>(&mut self, fields: I) -> ChainResult<()>
    where
        I: IntoIterator<Item = F>,
        F: AsRef<str>,
    {
        break_order_fact_at(&mut self.facts, fields, &self.record_definition.borrow())
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
            &self.record_definition.borrow(),
        )
    }

    pub fn set_distinct_fact_ids<I>(&mut self, distinct_datum_ids: I)
    where
        I: IntoIterator<Item = DatumId>,
    {
        set_distinct_fact_ids(&mut self.facts, distinct_datum_ids);
    }

    pub fn set_distinct_fact_all_fields(&mut self) {
        set_distinct_fact_all_fields(&mut self.facts, &self.record_definition.borrow());
    }

    pub fn break_distinct_fact_for<I, F>(&mut self, fields: I) -> ChainResult<()>
    where
        I: IntoIterator<Item = F>,
        F: AsRef<str>,
    {
        break_distinct_fact_for(&mut self.facts, fields, &self.record_definition.borrow())
    }

    pub fn break_distinct_fact_for_ids<I>(&mut self, datum_ids: I)
    where
        I: IntoIterator<Item = DatumId>,
    {
        break_distinct_fact_for_ids(&mut self.facts, datum_ids);
    }

    pub fn build(self, streams: &mut StreamsBuilder) -> &'g RefCell<QuirkyRecordDefinitionBuilder> {
        let variant_id = self
            .record_definition
            .borrow_mut()
            .close_record_variant_with(variant::append_data);
        streams.outputs.push(NodeStream::new(
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

impl OutputBuilderForUpdate<'_, DerivedExtra> {
    pub fn input_variant_id(&self) -> RecordVariantId {
        self.extra.input_variant_id
    }
}

#[derive(Debug)]
pub struct PathUpdateElement {
    pub field: ValidFieldName,
    pub sub_input_stream: StreamInfo,
    pub sub_output_stream: StreamInfo,
}

#[derive(Getters, CopyGetters, MutGetters)]
pub struct SubStreamBuilderForUpdate<'g, Extra> {
    #[getset(get = "pub")]
    record_type: StreamRecordType,
    #[getset(get_copy = "pub")]
    record_definition: &'g RefCell<QuirkyRecordDefinitionBuilder>,
    #[getset(get = "pub", get_mut = "pub")]
    sub_streams: BTreeMap<DatumId, NodeSubStream>,
    #[getset(get = "pub", get_mut = "pub")]
    facts: StreamFacts,
    extra: Extra,
}

impl<Extra> SubStreamBuilderForUpdate<'_, Extra> {
    pub fn sub_stream(&self, datum_id: DatumId) -> &NodeSubStream {
        &self.sub_streams[&datum_id]
    }

    pub fn set_order_fact<I, F>(&mut self, order_fields: I) -> ChainResult<()>
    where
        I: IntoIterator<Item = Directed<F>>,
        F: AsRef<str>,
    {
        set_order_fact(
            &mut self.facts,
            order_fields,
            &self.record_definition.borrow(),
        )
    }

    pub fn break_order_fact_at<I, F>(&mut self, fields: I) -> ChainResult<()>
    where
        I: IntoIterator<Item = F>,
        F: AsRef<str>,
    {
        break_order_fact_at(&mut self.facts, fields, &self.record_definition.borrow())
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
            &self.record_definition.borrow(),
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
        break_distinct_fact_for(&mut self.facts, fields, &self.record_definition.borrow())
    }

    pub fn close_record_variant(self, _facts: FactsFullyUpdated<()>) -> NodeSubStream {
        let variant_id = self
            .record_definition
            .borrow_mut()
            .close_record_variant_with(variant::append_data);
        NodeSubStream::new(self.record_type, variant_id, self.sub_streams, self.facts)
    }
}

impl SubStreamBuilderForUpdate<'_, DerivedExtra> {
    pub fn input_variant_id(&self) -> RecordVariantId {
        self.extra.input_variant_id
    }
}
