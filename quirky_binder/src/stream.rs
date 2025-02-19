use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Deref,
};

use itertools::Itertools;
use truc::record::definition::RecordVariantId;

use crate::prelude::*;

/// Defines the type of records going through a given stream.
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Hash, Default, Display, Debug, From)]
pub struct StreamRecordType(FullyQualifiedName);

impl Deref for StreamRecordType {
    type Target = Box<[Box<str>]>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Defines the source of a node stream, i.e. the way to connect to the source of records.
#[derive(PartialEq, Eq, Clone, Hash, Default, Display, Debug, From)]
pub struct NodeStreamSource(FullyQualifiedName);

impl Deref for NodeStreamSource {
    type Target = Box<[Box<str>]>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Node stream information
#[derive(Clone, Debug, new, Getters, CopyGetters)]
pub struct NodeStream {
    /// The type of the records going through the entire stream.
    #[getset(get = "pub")]
    record_type: StreamRecordType,
    /// The record variant for a specific node.
    #[getset(get_copy = "pub")]
    variant_id: QuirkyRecordVariantId,
    /// The sub-streams indexed by datum ID.
    #[getset(get = "pub")]
    sub_streams: BTreeMap<QuirkyDatumId, NodeSubStream>,
    /// The source to connect to in order to read records from it.
    #[getset(get = "pub")]
    source: NodeStreamSource,
    /// Whether or not the stream is the source main stream.
    #[getset(get_copy = "pub")]
    is_source_main_stream: bool,
    /// The facts (order and distinct) of the stream.
    #[getset(get = "pub")]
    facts: StreamFacts,
}

impl NodeStream {
    /// Creates a new stream with a different source.
    pub fn with_source(&self, source: NodeStreamSource) -> Self {
        Self {
            source,
            ..self.clone()
        }
    }
}

pub trait NoneNodeStream {
    fn none(&self) -> Option<&NodeStream>;
}

impl NoneNodeStream for [NodeStream; 0] {
    fn none(&self) -> Option<&NodeStream> {
        None
    }
}

pub trait SingleNodeStream {
    fn single(&self) -> &NodeStream;

    fn some_single(&self) -> Option<&NodeStream>;
}

impl SingleNodeStream for [NodeStream; 1] {
    fn single(&self) -> &NodeStream {
        &self[0]
    }

    fn some_single(&self) -> Option<&NodeStream> {
        Some(self.single())
    }
}

/// Node sub stream information
#[derive(Clone, Debug, new, Getters, CopyGetters, MutGetters)]
pub struct NodeSubStream {
    /// The type of the records going through the entire stream.
    #[getset(get = "pub")]
    record_type: StreamRecordType,
    /// The record variant for a specific node.
    #[getset(get_copy = "pub")]
    variant_id: QuirkyRecordVariantId,
    /// The sub-streams indexed by datum ID.
    #[getset(get = "pub", get_mut = "pub")]
    sub_streams: BTreeMap<QuirkyDatumId, NodeSubStream>,
    /// The facts (order and distinct) of the sub-stream.
    #[getset(get = "pub")]
    facts: StreamFacts,
}

impl NodeSubStream {
    pub fn destructure(
        self,
    ) -> (
        StreamRecordType,
        QuirkyRecordVariantId,
        BTreeMap<QuirkyDatumId, NodeSubStream>,
        StreamFacts,
    ) {
        (
            self.record_type,
            self.variant_id,
            self.sub_streams,
            self.facts,
        )
    }
}

#[derive(new)]
pub struct RecordDefinitionFragments<'a> {
    record_type: &'a StreamRecordType,
    variant_id: RecordVariantId,
    module_prefix: &'a FullyQualifiedName,
}

impl RecordDefinitionFragments<'_> {
    pub fn record(&self) -> syn::Type {
        syn::parse_str::<syn::Type>(&format!(
            "{}::{}::Record{}",
            self.module_prefix, self.record_type, self.variant_id
        ))
        .expect("record")
    }

    pub fn unpacked_record(&self) -> syn::Type {
        syn::parse_str::<syn::Type>(&format!(
            "{}::{}::UnpackedRecord{}",
            self.module_prefix, self.record_type, self.variant_id
        ))
        .expect("unpacked_record")
    }

    pub fn unpacked_record_in(&self) -> syn::Type {
        syn::parse_str::<syn::Type>(&format!(
            "{}::{}::UnpackedRecordIn{}",
            self.module_prefix, self.record_type, self.variant_id
        ))
        .expect("unpacked_record_in")
    }

    pub fn record_and_unpacked_out(&self) -> syn::Type {
        syn::parse_str::<syn::Type>(&format!(
            "{}::{}::Record{}AndUnpackedOut",
            self.module_prefix, self.record_type, self.variant_id
        ))
        .expect("record_and_unpacked_out")
    }
}

#[derive(Clone, Default, Getters, Debug)]
pub struct StreamFacts {
    #[getset(get = "pub")]
    order: Vec<Directed<QuirkyDatumId>>,
    #[getset(get = "pub")]
    distinct: Vec<QuirkyDatumId>,
}

impl StreamFacts {
    pub fn set_order(&mut self, order: Vec<Directed<QuirkyDatumId>>) {
        assert!(order.iter().all_unique());
        self.order = order;
    }

    pub fn set_distinct(&mut self, distinct: Vec<QuirkyDatumId>) {
        assert!(distinct.iter().all_unique());
        self.distinct = distinct;
    }

    pub fn break_order_at_ids(&mut self, datum_ids: &BTreeSet<QuirkyDatumId>) {
        let position = self.order.iter().position(|d| datum_ids.contains(d));
        if let Some(position) = position {
            self.order.truncate(position);
        }
    }
}
