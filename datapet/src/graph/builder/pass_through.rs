use super::{
    break_distinct_fact_for, break_distinct_fact_for_ids, break_order_fact_at,
    break_order_fact_at_ids, set_distinct_fact, set_distinct_fact_all_fields,
    set_distinct_fact_ids, set_order_fact,
};
use crate::{
    prelude::*,
    stream::{NodeSubStream, StreamFacts},
};
use std::{cell::RefCell, collections::BTreeMap};
use truc::record::{
    definition::{DatumDefinition, DatumId, RecordDefinitionBuilder, RecordVariantId},
    type_resolver::TypeResolver,
};

#[derive(Getters, CopyGetters)]
pub struct OutputBuilderForPassThrough<'a, 'b, 'g, R: TypeResolver + Copy> {
    pub(super) streams: &'b mut StreamsBuilder<'a>,
    #[getset(get = "pub")]
    pub(super) record_type: StreamRecordType,
    #[getset(get_copy = "pub")]
    pub(super) record_definition: &'g RefCell<RecordDefinitionBuilder<R>>,
    pub(super) input_variant_id: RecordVariantId,
    pub(super) sub_streams: BTreeMap<DatumId, NodeSubStream>,
    pub(super) source: NodeStreamSource,
    pub(super) is_output_main_stream: bool,
    #[getset(get = "pub")]
    pub(super) facts: StreamFacts,
}

impl<'a, 'b, 'g, R: TypeResolver + Copy> OutputBuilderForPassThrough<'a, 'b, 'g, R> {
    pub fn pass_through_sub_stream<B>(
        &mut self,
        sub_stream: NodeSubStream,
        graph: &'g GraphBuilder<R>,
        build: B,
    ) -> NodeSubStream
    where
        B: FnOnce(&mut SubStreamBuilderForPassThrough<'g, R>),
    {
        let record_definition = graph
            .get_stream(sub_stream.record_type())
            .unwrap_or_else(|| panic!(r#"stream "{}""#, sub_stream.record_type()));
        let (record_type, variant_id, sub_streams, facts) = sub_stream.destructure();
        let mut builder = SubStreamBuilderForPassThrough {
            record_type,
            record_definition,
            input_variant_id: variant_id,
            sub_streams,
            facts,
        };
        build(&mut builder);
        builder.close_pass_through()
    }

    pub fn pass_through_path<PassThroughLeafSubStream>(
        &mut self,
        path_fields: &[&str],
        pass_through_leaf_sub_stream: PassThroughLeafSubStream,
        graph: &'g GraphBuilder<R>,
    ) -> NodeSubStream
    where
        PassThroughLeafSubStream: for<'c, 'd> FnOnce(
            NodeSubStream,
            &mut OutputBuilderForPassThrough<'c, 'd, 'g, R>,
        ) -> NodeSubStream,
    {
        struct PathFieldDetails {
            stream: NodeSubStream,
            datum_id: DatumId,
        }

        // Find the sub stream of the first path field
        let (root_datum_id, root_sub_stream, root_sub_stream_record_definition) = {
            let field = path_fields.first().expect("first path field");
            let datum_id = self
                .record_definition
                .borrow()
                .get_current_datum_definition_by_name(field)
                .map(DatumDefinition::id);
            if let Some(datum_id) = datum_id {
                let sub_stream = self.sub_streams.remove(&datum_id).expect("root sub stream");
                let sub_record_definition = graph
                    .get_stream(sub_stream.record_type())
                    .unwrap_or_else(|| panic!(r#"stream "{}""#, sub_stream.record_type()));
                (datum_id, sub_stream, sub_record_definition)
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
                    .get_current_datum_definition_by_name(field)
                    .map(DatumDefinition::id);
                if let Some(datum_id) = datum_id {
                    let sub_stream = stream
                        .sub_streams_mut()
                        .remove(&datum_id)
                        .expect("sub stream");
                    let sub_record_definition = graph
                        .get_stream(sub_stream.record_type())
                        .unwrap_or_else(|| panic!(r#"stream "{}""#, sub_stream.record_type()));
                    path_data.push(PathFieldDetails { stream, datum_id });
                    (path_data, sub_stream, sub_record_definition)
                } else {
                    panic!("could not find datum `{}`", field);
                }
            },
        );

        // Then update the leaf sub stream
        let leaf_sub_output_stream = pass_through_leaf_sub_stream(leaf_sub_input_stream, self);
        let mut sub_output_stream = Some(leaf_sub_output_stream.clone());

        // Then all streams up to the root
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

                sub_output_stream = Some(field_details.stream);
            }
        }

        {
            let sub_stream = sub_output_stream.take().expect("sub_output_stream");
            let old = self.sub_streams.insert(root_datum_id, sub_stream);
            if old.is_some() {
                panic!("sub stream should have been removed");
            }
        }

        leaf_sub_output_stream
    }

    pub fn set_order_fact(&mut self, order_fields: &[&str]) {
        set_order_fact(
            &mut self.facts,
            order_fields,
            &*self.record_definition.borrow(),
        );
    }

    pub fn break_order_fact_at(&mut self, fields: &[&str]) {
        break_order_fact_at(&mut self.facts, fields, &*self.record_definition.borrow());
    }

    pub fn break_order_fact_at_ids<I>(&mut self, datum_ids: I)
    where
        I: IntoIterator<Item = DatumId>,
    {
        break_order_fact_at_ids(&mut self.facts, datum_ids);
    }

    pub fn set_distinct_fact(&mut self, distinct_fields: &[&str]) {
        set_distinct_fact(
            &mut self.facts,
            distinct_fields,
            &*self.record_definition.borrow(),
        );
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

    pub fn break_distinct_fact_for(&mut self, fields: &[&str]) {
        break_distinct_fact_for(&mut self.facts, fields, &*self.record_definition.borrow());
    }

    pub fn break_distinct_fact_for_ids<I>(&mut self, datum_ids: I)
    where
        I: IntoIterator<Item = DatumId>,
    {
        break_distinct_fact_for_ids(&mut self.facts, datum_ids);
    }

    pub fn build(self) -> &'g RefCell<RecordDefinitionBuilder<R>> {
        self.streams.outputs.push(NodeStream::new(
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

#[derive(Getters, CopyGetters)]
pub struct SubStreamBuilderForPassThrough<'g, R: TypeResolver> {
    #[getset(get = "pub")]
    record_type: StreamRecordType,
    #[getset(get_copy = "pub")]
    record_definition: &'g RefCell<RecordDefinitionBuilder<R>>,
    input_variant_id: RecordVariantId,
    sub_streams: BTreeMap<DatumId, NodeSubStream>,
    #[getset(get = "pub")]
    facts: StreamFacts,
}

impl<'g, R: TypeResolver> SubStreamBuilderForPassThrough<'g, R> {
    pub fn close_pass_through(self) -> NodeSubStream {
        NodeSubStream::new(
            self.record_type,
            self.input_variant_id,
            self.sub_streams,
            self.facts,
        )
    }

    pub fn set_order_fact(&mut self, order_fields: &[&str]) {
        set_order_fact(
            &mut self.facts,
            order_fields,
            &*self.record_definition.borrow(),
        );
    }
}
