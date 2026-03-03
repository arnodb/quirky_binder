use std::{
    cell::RefCell,
    collections::{BTreeMap, VecDeque},
};

use truc::record::definition::{DatumDefinition, DatumId, RecordVariantId};

use super::{
    break_distinct_fact_for, break_distinct_fact_for_ids, break_order_fact_at,
    break_order_fact_at_ids, set_distinct_fact, set_distinct_fact_all_fields,
    set_distinct_fact_ids, set_order_fact,
};
use crate::{prelude::*, trace_element};

#[derive(Getters, CopyGetters)]
pub struct OutputBuilderForPassThrough<'g> {
    #[getset(get = "pub")]
    pub(super) record_type: StreamRecordType,
    #[getset(get_copy = "pub")]
    pub(super) record_definition: &'g RefCell<QuirkyRecordDefinitionBuilder>,
    pub(super) input_variant_id: RecordVariantId,
    #[getset(get = "pub")]
    pub(super) sub_streams: BTreeMap<DatumId, NodeSubStream>,
    pub(super) source: NodeStreamSource,
    pub(super) is_output_main_stream: bool,
    #[getset(get = "pub")]
    pub(super) facts: StreamFacts,
}

impl<'g> OutputBuilderForPassThrough<'g> {
    pub fn pass_through_sub_stream<B>(
        &mut self,
        sub_stream: NodeSubStream,
        graph: &'g GraphBuilder,
        build: B,
    ) -> ChainResultWithTrace<NodeSubStream>
    where
        B: FnOnce(&mut SubStreamBuilderForPassThrough<'g>) -> ChainResultWithTrace<()>,
    {
        let record_definition = graph
            .get_stream(sub_stream.record_type())
            .with_trace_element(trace_element!())?;
        let (record_type, variant_id, sub_streams, facts) = sub_stream.destructure();
        let mut builder = SubStreamBuilderForPassThrough {
            record_type,
            record_definition,
            input_variant_id: variant_id,
            sub_streams,
            facts,
        };
        build(&mut builder)?;
        Ok(builder.close_pass_through())
    }

    pub fn pass_through_path<PassThroughLeafSubStream>(
        &mut self,
        path_fields: Vec<ValidFieldName>,
        pass_through_leaf_sub_stream: PassThroughLeafSubStream,
        graph: &'g GraphBuilder,
    ) -> ChainResultWithTrace<Vec<PassThroughPathElement>>
    where
        PassThroughLeafSubStream: FnOnce(
            NodeSubStream,
            &mut OutputBuilderForPassThrough<'g>,
        ) -> ChainResultWithTrace<NodeSubStream>,
    {
        struct PathFieldDetails {
            stream: NodeSubStream,
            field: ValidFieldName,
            datum_id: DatumId,
        }

        let mut path_fields = VecDeque::from(path_fields);

        // Find the sub stream of the first path field
        let (root_field, root_datum_id, root_sub_stream, root_sub_stream_record_definition) = {
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
            let sub_stream = self.sub_streams.remove(&datum_id).expect("root sub stream");
            let sub_record_definition = graph
                .get_stream(sub_stream.record_type())
                .with_trace_element(trace_element!())?;
            (field, datum_id, sub_stream, sub_record_definition)
        };

        // Then find path input streams
        let (mut path_details, leaf_sub_input_stream, _) = path_fields.into_iter().try_fold(
            (
                Vec::new(),
                root_sub_stream,
                root_sub_stream_record_definition,
            ),
            |(mut path_data, mut stream, record_definition), field| {
                let datum_id = record_definition
                    .borrow()
                    .get_current_datum_definition_by_name(field.name())
                    .ok_or_else(|| ChainError::FieldNotFound {
                        field: field.name().to_owned(),
                    })
                    .with_trace_element(trace_element!())?
                    .id();
                let sub_stream = stream
                    .sub_streams_mut()
                    .remove(&datum_id)
                    .expect("sub stream");
                let sub_record_definition = graph
                    .get_stream(sub_stream.record_type())
                    .with_trace_element(trace_element!())?;
                path_data.push(PathFieldDetails {
                    field,
                    stream,
                    datum_id,
                });
                Ok((path_data, sub_stream, sub_record_definition))
            },
        )?;

        // Then update the leaf sub stream
        let mut sub_output_stream = pass_through_leaf_sub_stream(leaf_sub_input_stream, self)?;

        let mut path_streams = Vec::<PassThroughPathElement>::with_capacity(path_details.len() + 1);

        // Then all streams up to the root
        while let Some(mut field_details) = path_details.pop() {
            let updated_sub_stream = sub_output_stream;

            path_streams.push(PassThroughPathElement {
                field: field_details.field,
                sub_stream: StreamInfo::from(&updated_sub_stream),
            });

            let None = field_details
                .stream
                .sub_streams_mut()
                .insert(field_details.datum_id, updated_sub_stream)
            else {
                return Err(ChainError::Other {
                    msg: "sub stream should have been removed".to_owned(),
                })
                .with_trace_element(trace_element!());
            };

            sub_output_stream = field_details.stream;
        }

        {
            let updated_sub_stream = sub_output_stream;

            path_streams.push(PassThroughPathElement {
                field: root_field,
                sub_stream: StreamInfo::from(&updated_sub_stream),
            });

            let None = self.sub_streams.insert(root_datum_id, updated_sub_stream) else {
                return Err(ChainError::Other {
                    msg: "sub stream should have been removed".to_owned(),
                })
                .with_trace_element(trace_element!());
            };
        }

        // They were pushed in reverse order
        path_streams.reverse();

        Ok(path_streams)
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

    pub fn set_distinct_fact(&mut self, distinct_fields: &[&str]) -> ChainResult<()> {
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
        streams.outputs.push(NodeStream::new(
            self.record_type,
            self.input_variant_id,
            self.sub_streams,
            self.source,
            self.is_output_main_stream,
            self.facts,
        ));
        self.record_definition
    }
}

#[derive(Debug)]
pub struct PassThroughPathElement {
    pub field: ValidFieldName,
    pub sub_stream: StreamInfo,
}

#[derive(Getters, CopyGetters)]
pub struct SubStreamBuilderForPassThrough<'g> {
    #[getset(get = "pub")]
    record_type: StreamRecordType,
    #[getset(get_copy = "pub")]
    record_definition: &'g RefCell<QuirkyRecordDefinitionBuilder>,
    input_variant_id: RecordVariantId,
    sub_streams: BTreeMap<DatumId, NodeSubStream>,
    #[getset(get = "pub")]
    facts: StreamFacts,
}

impl SubStreamBuilderForPassThrough<'_> {
    pub fn close_pass_through(self) -> NodeSubStream {
        NodeSubStream::new(
            self.record_type,
            self.input_variant_id,
            self.sub_streams,
            self.facts,
        )
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

    pub fn set_distinct_fact_all_fields(&mut self) {
        set_distinct_fact_all_fields(&mut self.facts, &self.record_definition.borrow());
    }
}
