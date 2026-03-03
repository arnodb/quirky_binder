use std::{
    cell::RefCell,
    collections::{BTreeMap, VecDeque},
};

use truc::record::definition::{builder::generic::variant, DatumDefinition, DatumId};

use super::NoFactsUpdated;
use crate::{prelude::*, trace_element};

pub struct OutputBuilderForUpdate<'g> {
    pub(super) record_definition: &'g RefCell<QuirkyRecordDefinitionBuilder>,
    pub(super) stream: NodeStream,
}

impl<'g> OutputBuilderForUpdate<'g> {
    pub fn root<UpdateStream>(
        mut self,
        streams: &mut StreamsBuilder,
        update_stream: UpdateStream,
    ) -> ChainResultWithTrace<StreamInfo>
    where
        UpdateStream: FnOnce(
            &mut NodeStream,
            NoFactsUpdated<()>,
        ) -> ChainResultWithTrace<FactsFullyUpdated<()>>,
    {
        update_stream(&mut self.stream, NoFactsUpdated(()))?;
        let stream_info = StreamInfo::from(&self.stream);
        self.build(streams);
        Ok(stream_info)
    }

    pub fn path<UpdateLeafSubStream>(
        mut self,
        graph: &'g GraphBuilder,
        streams: &mut StreamsBuilder,
        path_fields: Vec<ValidFieldName>,
        update_leaf_sub_stream: UpdateLeafSubStream,
    ) -> ChainResultWithTrace<Vec<UpdatePathElement>>
    where
        UpdateLeafSubStream: FnOnce(
            &mut NodeSubStream,
            NoFactsUpdated<()>,
        ) -> ChainResultWithTrace<FactsFullyUpdated<()>>,
    {
        let mut path_fields = VecDeque::from(path_fields);

        let b = {
            let field = path_fields.pop_front().expect("first path field");
            let Some(datum_id) = self
                .record_definition
                .borrow()
                .get_current_datum_definition_by_name(field.name())
                .map(DatumDefinition::id)
            else {
                return Err(ChainError::FieldNotFound {
                    field: field.name().to_owned(),
                })
                .with_trace_element(trace_element!());
            };
            let sub_streams = self.stream.sub_streams_mut();
            SubStreamBuilderForUpdate {
                record_definition: self.record_definition,
                sub_streams,
                field,
                datum_id,
            }
        };

        fn recurse<UpdateLeafSubStream>(
            graph: &GraphBuilder,
            mut b: SubStreamBuilderForUpdate,
            mut path_fields: VecDeque<ValidFieldName>,
            update_leaf_sub_stream: UpdateLeafSubStream,
            path_streams: &mut Vec<UpdatePathElement>,
        ) -> ChainResultWithTrace<()>
        where
            UpdateLeafSubStream: FnOnce(
                &mut NodeSubStream,
                NoFactsUpdated<()>,
            ) -> ChainResultWithTrace<FactsFullyUpdated<()>>,
        {
            let path_element = if !path_fields.is_empty() {
                let sub_b = b.sub_stream(graph, path_fields.pop_front().unwrap())?;
                recurse(
                    graph,
                    sub_b,
                    path_fields,
                    update_leaf_sub_stream,
                    path_streams,
                )?;
                b.build(graph, |_, facts_proof| {
                    Ok(facts_proof.order_facts_updated().distinct_facts_updated())
                })?
            } else {
                b.build(graph, update_leaf_sub_stream)?
            };
            path_streams.push(path_element);
            Ok(())
        }

        let mut path_streams = Vec::with_capacity(path_fields.len());

        recurse(
            graph,
            b,
            path_fields,
            update_leaf_sub_stream,
            &mut path_streams,
        )?;

        self.build(streams);

        path_streams.reverse();

        Ok(path_streams)
    }

    fn build(mut self, streams: &mut StreamsBuilder) {
        let new_variant_id = self
            .record_definition
            .borrow_mut()
            .close_record_variant_with(variant::append_data);

        *self.stream.variant_id_mut() = new_variant_id;

        streams.outputs.push(self.stream);
    }
}

#[derive(Debug)]
pub struct UpdatePathElement {
    pub field: ValidFieldName,
    pub sub_input_stream: StreamInfo,
    pub sub_output_stream: StreamInfo,
}

pub struct SubStreamBuilderForUpdate<'a> {
    record_definition: &'a RefCell<QuirkyRecordDefinitionBuilder>,
    sub_streams: &'a mut BTreeMap<DatumId, NodeSubStream>,
    field: ValidFieldName,
    datum_id: DatumId,
}

impl<'a> SubStreamBuilderForUpdate<'a> {
    fn sub_stream<'b>(
        &'b mut self,
        graph: &'b GraphBuilder,
        field: ValidFieldName,
    ) -> ChainResultWithTrace<SubStreamBuilderForUpdate<'b>> {
        let datum_id = self.datum_id;
        let sub_stream = self.sub_streams.get_mut(&datum_id).unwrap();
        let sub_record_definition = graph
            .get_stream(sub_stream.record_type())
            .with_trace_element(trace_element!())?;
        let sub_sub_streams = sub_stream.sub_streams_mut();
        let Some(sub_datum_id) = sub_record_definition
            .borrow()
            .get_current_datum_definition_by_name(field.name())
            .map(DatumDefinition::id)
        else {
            return Err(ChainError::FieldNotFound {
                field: field.name().to_owned(),
            })
            .with_trace_element(trace_element!());
        };
        Ok(SubStreamBuilderForUpdate {
            record_definition: sub_record_definition,
            sub_streams: sub_sub_streams,
            field,
            datum_id: sub_datum_id,
        })
    }

    fn build<Update>(
        self,
        graph: &GraphBuilder,
        update: Update,
    ) -> ChainResultWithTrace<UpdatePathElement>
    where
        Update: FnOnce(
            &mut NodeSubStream,
            NoFactsUpdated<()>,
        ) -> ChainResultWithTrace<FactsFullyUpdated<()>>,
    {
        let mut sub_stream = self.sub_streams.remove(&self.datum_id).unwrap();

        let sub_input_stream = StreamInfo::from(&sub_stream);

        let record_definition = graph
            .get_stream(sub_stream.record_type())
            .with_trace_element(trace_element!())?;

        update(&mut sub_stream, NoFactsUpdated(()))?;

        let new_variant_id = record_definition
            .borrow_mut()
            .close_record_variant_with(variant::append_data);

        *sub_stream.variant_id_mut() = new_variant_id;

        let sub_output_stream = StreamInfo::from(&sub_stream);

        let new_datum_id = replace_vec_datum_in_record_definition(
            &mut self.record_definition.borrow_mut(),
            self.field.name(),
            sub_stream.record_type().clone(),
            sub_stream.variant_id(),
        )
        .with_trace_element(trace_element!())?;

        self.sub_streams.insert(new_datum_id, sub_stream);

        let path_update_element = UpdatePathElement {
            field: self.field,
            sub_input_stream,
            sub_output_stream,
        };

        Ok(path_update_element)
    }
}
