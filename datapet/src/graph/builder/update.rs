use std::{cell::RefCell, collections::BTreeMap};

use truc::record::{
    definition::{DatumDefinition, DatumId, RecordDefinitionBuilder, RecordVariantId},
    type_resolver::TypeResolver,
};

use super::{
    add_vec_datum_to_record_definition, break_distinct_fact_for, break_distinct_fact_for_ids,
    break_order_fact_at, break_order_fact_at_ids, replace_vec_datum_in_record_definition,
    set_distinct_fact, set_distinct_fact_all_fields, set_distinct_fact_ids, set_order_fact,
};
use crate::{
    prelude::*,
    stream::{NodeSubStream, StreamFacts},
};

#[derive(Getters, CopyGetters)]
pub struct OutputBuilderForUpdate<'a, 'b, 'g, R: TypeResolver + Copy> {
    pub(super) streams: &'b mut StreamsBuilder<'a>,
    #[getset(get = "pub")]
    pub(super) record_type: StreamRecordType,
    #[getset(get_copy = "pub")]
    pub(super) record_definition: &'g RefCell<RecordDefinitionBuilder<R>>,
    pub(super) input_variant_id: Option<RecordVariantId>,
    pub(super) sub_streams: BTreeMap<DatumId, NodeSubStream>,
    pub(super) source: NodeStreamSource,
    pub(super) is_output_main_stream: bool,
    #[getset(get = "pub")]
    pub(super) facts: StreamFacts,
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
    ) -> SubStreamBuilderForUpdate<'g, R> {
        let full_name = self.streams.name.sub(name);
        self.new_sub_stream_internal(graph, full_name)
    }

    fn new_sub_stream_internal(
        &self,
        graph: &'g GraphBuilder<R>,
        full_name: FullyQualifiedName,
    ) -> SubStreamBuilderForUpdate<'g, R> {
        let record_type = StreamRecordType::from(full_name);
        let record_definition = graph
            .get_stream(&record_type)
            .unwrap_or_else(|| panic!(r#"stream "{}""#, &record_type));
        SubStreamBuilderForUpdate {
            record_type,
            record_definition,
            input_variant_id: None,
            sub_streams: BTreeMap::new(),
            facts: StreamFacts::default(),
        }
    }

    pub fn update_sub_stream<B>(
        &mut self,
        sub_stream: NodeSubStream,
        graph: &'g GraphBuilder<R>,
        build: B,
    ) -> ChainResult<NodeSubStream>
    where
        B: FnOnce(&mut Self, &mut SubStreamBuilderForUpdate<'g, R>) -> ChainResult<()>,
    {
        let record_definition = graph
            .get_stream(sub_stream.record_type())
            .unwrap_or_else(|| panic!(r#"stream "{}""#, sub_stream.record_type()));
        let (record_type, variant_id, sub_streams, facts) = sub_stream.destructure();
        let mut builder = SubStreamBuilderForUpdate {
            record_type,
            record_definition,
            input_variant_id: Some(variant_id),
            sub_streams,
            facts,
        };
        build(self, &mut builder)?;
        Ok(builder.close_record_variant())
    }

    pub fn update_path<UpdateLeafSubStream, UpdatePathStream, UpdateRootStream>(
        &mut self,
        path_fields: &[&str],
        update_leaf_sub_stream: UpdateLeafSubStream,
        update_path_stream: UpdatePathStream,
        update_root_stream: UpdateRootStream,
        graph: &'g GraphBuilder<R>,
    ) -> ChainResult<Vec<PathUpdateElement>>
    where
        UpdateLeafSubStream: for<'c, 'd> FnOnce(
            NodeSubStream,
            &mut OutputBuilderForUpdate<'c, 'd, 'g, R>,
        ) -> ChainResult<NodeSubStream>,
        UpdatePathStream:
            Fn(&str, &mut SubStreamBuilderForUpdate<'g, R>, NodeSubStream) -> ChainResult<()>,
        UpdateRootStream: for<'c, 'd> FnOnce(
            &str,
            &mut OutputBuilderForUpdate<'c, 'd, 'g, R>,
            NodeSubStream,
        ) -> ChainResult<()>,
    {
        struct PathFieldDetails<'a> {
            stream: NodeSubStream,
            field: &'a str,
            datum_id: DatumId,
        }

        // Find the sub stream of the first path field
        let (root_field, root_datum_id, root_sub_stream, root_sub_stream_record_definition) = {
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
                (*field, datum_id, sub_stream, sub_record_definition)
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
                    path_data.push(PathFieldDetails {
                        stream,
                        field,
                        datum_id,
                    });
                    (path_data, sub_stream, sub_record_definition)
                } else {
                    panic!("could not find datum `{}`", field);
                }
            },
        );

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
                    field: field_details.field.to_owned(),
                    sub_input_stream: sub_input_stream.take().expect("sub_input_stream"),
                    sub_output_stream: sub_stream.clone(),
                });

                let updated_stream = self.update_sub_stream(
                    field_details.stream.clone(),
                    graph,
                    |_, path_stream| {
                        update_path_stream(field_details.field, path_stream, sub_stream)?;
                        Ok(())
                    },
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
                field: root_field.to_owned(),
                sub_input_stream: sub_input_stream.take().expect("sub_input_stream"),
                sub_output_stream: sub_stream.clone(),
            });
            update_root_stream(root_field, self, sub_stream)?;
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

    pub fn replace_vec_datum(&mut self, field: &str, record: &str, sub_stream: NodeSubStream) {
        let (old_datum_id, new_datum_id) = replace_vec_datum_in_record_definition(
            &mut self.record_definition.borrow_mut(),
            field,
            record,
        );
        let old = self.sub_streams.remove(&old_datum_id);
        if old.is_none() {
            panic!("the replaced datum should be registered");
        }
        let old = self.sub_streams.insert(new_datum_id, sub_stream);
        if old.is_some() {
            panic!("the datum should not be registered yet");
        }
    }

    pub fn set_order_fact(&mut self, order_fields: &[Directed<&str>]) {
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
        let variant_id = self.record_definition.borrow_mut().close_record_variant();
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

pub struct PathUpdateElement {
    pub field: String,
    pub sub_input_stream: NodeSubStream,
    pub sub_output_stream: NodeSubStream,
}

#[derive(Getters, CopyGetters)]
pub struct SubStreamBuilderForUpdate<'g, R: TypeResolver> {
    #[getset(get = "pub")]
    record_type: StreamRecordType,
    #[getset(get_copy = "pub")]
    record_definition: &'g RefCell<RecordDefinitionBuilder<R>>,
    input_variant_id: Option<RecordVariantId>,
    sub_streams: BTreeMap<DatumId, NodeSubStream>,
    #[getset(get = "pub")]
    facts: StreamFacts,
}

impl<'g, R: TypeResolver> SubStreamBuilderForUpdate<'g, R> {
    pub fn input_variant_id(&self) -> RecordVariantId {
        if let Some(input_variant_id) = self.input_variant_id {
            input_variant_id
        } else {
            panic! {"output not derived from input"}
        }
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

    pub fn replace_vec_datum(&mut self, field: &str, record: &str, sub_stream: NodeSubStream) {
        let (old_datum_id, new_datum_id) = replace_vec_datum_in_record_definition(
            &mut self.record_definition.borrow_mut(),
            field,
            record,
        );
        let old = self.sub_streams.remove(&old_datum_id);
        if old.is_none() {
            panic!("the replaced datum should be registered");
        }
        let old = self.sub_streams.insert(new_datum_id, sub_stream);
        if old.is_some() {
            panic!("the datum should not be registered yet");
        }
    }

    pub fn close_record_variant(self) -> NodeSubStream {
        let variant_id = self.record_definition.borrow_mut().close_record_variant();
        NodeSubStream::new(self.record_type, variant_id, self.sub_streams, self.facts)
    }
}
