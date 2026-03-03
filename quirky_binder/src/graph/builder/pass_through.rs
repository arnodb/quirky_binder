use std::{
    cell::RefCell,
    collections::{BTreeMap, VecDeque},
};

use truc::record::definition::{DatumDefinition, DatumId};

use super::NoFactsUpdated;
use crate::{prelude::*, trace_element};

pub struct OutputBuilderForPassThrough<'g> {
    pub(super) record_definition: &'g RefCell<QuirkyRecordDefinitionBuilder>,
    pub(super) stream: NodeStream,
}

impl<'g> OutputBuilderForPassThrough<'g> {
    pub fn root<PassThroughStream>(
        mut self,
        streams: &mut StreamsBuilder,
        pass_through_stream: PassThroughStream,
    ) -> ChainResultWithTrace<StreamInfo>
    where
        PassThroughStream: FnOnce(
            &mut NodeStream,
            NoFactsUpdated<()>,
        ) -> ChainResultWithTrace<FactsFullyUpdated<()>>,
    {
        pass_through_stream(&mut self.stream, NoFactsUpdated(()))?;
        let stream_info = StreamInfo::from(&self.stream);
        self.build(streams);
        Ok(stream_info)
    }

    pub fn path<PassThroughLeafSubStream>(
        mut self,
        graph: &'g GraphBuilder,
        streams: &mut StreamsBuilder,
        path_fields: Vec<ValidFieldName>,
        pass_through_leaf_sub_stream: PassThroughLeafSubStream,
    ) -> ChainResultWithTrace<Vec<PassThroughPathElement>>
    where
        PassThroughLeafSubStream: FnOnce(
            &mut NodeSubStream,
            NoFactsUpdated<()>,
        ) -> ChainResultWithTrace<FactsFullyUpdated<()>>,
    {
        let mut path_fields = VecDeque::from(path_fields);

        let b = {
            let field = path_fields.pop_front().expect("first path field");
            let datum_id = self
                .record_definition
                .borrow()
                .get_current_datum_definition_by_name(field.name())
                .map(DatumDefinition::id);
            if let Some(datum_id) = datum_id {
                let sub_streams = self.stream.sub_streams_mut();
                SubStreamBuilderForPassThrough {
                    sub_streams,
                    field,
                    datum_id,
                }
            } else {
                return Err(ChainError::FieldNotFound {
                    field: field.name().to_owned(),
                })
                .with_trace_element(trace_element!());
            }
        };

        fn recurse<PassThroughLeafSubStream>(
            graph: &GraphBuilder,
            mut b: SubStreamBuilderForPassThrough,
            mut path_fields: VecDeque<ValidFieldName>,
            pass_through_leaf_sub_stream: PassThroughLeafSubStream,
            path_streams: &mut Vec<PassThroughPathElement>,
        ) -> ChainResultWithTrace<()>
        where
            PassThroughLeafSubStream: FnOnce(
                &mut NodeSubStream,
                NoFactsUpdated<()>,
            )
                -> ChainResultWithTrace<FactsFullyUpdated<()>>,
        {
            let path_element = if !path_fields.is_empty() {
                let sub_b = b.sub_stream(graph, path_fields.pop_front().unwrap())?;
                recurse(
                    graph,
                    sub_b,
                    path_fields,
                    pass_through_leaf_sub_stream,
                    path_streams,
                )?;
                b.build(|_, facts_proof| {
                    Ok(facts_proof.order_facts_updated().distinct_facts_updated())
                })?
            } else {
                b.build(pass_through_leaf_sub_stream)?
            };
            path_streams.push(path_element);
            Ok(())
        }

        let mut path_streams = Vec::with_capacity(path_fields.len());

        recurse(
            graph,
            b,
            path_fields,
            pass_through_leaf_sub_stream,
            &mut path_streams,
        )?;

        self.build(streams);

        path_streams.reverse();

        Ok(path_streams)
    }

    pub fn build(self, streams: &mut StreamsBuilder) {
        streams.outputs.push(self.stream);
    }
}

#[derive(Debug)]
pub struct PassThroughPathElement {
    pub field: ValidFieldName,
    pub sub_stream: StreamInfo,
}

pub struct SubStreamBuilderForPassThrough<'a> {
    sub_streams: &'a mut BTreeMap<DatumId, NodeSubStream>,
    field: ValidFieldName,
    datum_id: DatumId,
}

impl<'a> SubStreamBuilderForPassThrough<'a> {
    fn sub_stream(
        &mut self,
        graph: &GraphBuilder,
        field: ValidFieldName,
    ) -> ChainResultWithTrace<SubStreamBuilderForPassThrough<'_>> {
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
        Ok(SubStreamBuilderForPassThrough {
            sub_streams: sub_sub_streams,
            field,
            datum_id: sub_datum_id,
        })
    }

    fn build<PassThrough>(
        self,
        pass_through: PassThrough,
    ) -> ChainResultWithTrace<PassThroughPathElement>
    where
        PassThrough: FnOnce(
            &mut NodeSubStream,
            NoFactsUpdated<()>,
        ) -> ChainResultWithTrace<FactsFullyUpdated<()>>,
    {
        let sub_stream = self.sub_streams.get_mut(&self.datum_id).unwrap();

        pass_through(sub_stream, NoFactsUpdated(()))?;

        let stream_info = StreamInfo::from(&*sub_stream);

        Ok(PassThroughPathElement {
            field: self.field,
            sub_stream: stream_info,
        })
    }
}
