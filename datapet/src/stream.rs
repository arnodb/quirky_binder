use crate::support::FullyQualifiedName;
use std::ops::Deref;
use truc::record::definition::RecordVariantId;

/// Defines the type of records going through a given stream.
#[derive(PartialEq, Eq, Clone, Hash, Default, Display, Debug, From)]
pub struct StreamRecordType(FullyQualifiedName);

impl Deref for StreamRecordType {
    type Target = Box<[Box<str>]>;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

/// Defines the source of a node stream, i.e. the way to connect to the source of records.
#[derive(PartialEq, Eq, Clone, Hash, Default, Display, Debug, From)]
pub struct NodeStreamSource(FullyQualifiedName);

impl Deref for NodeStreamSource {
    type Target = Box<[Box<str>]>;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

/// Node stream information
#[derive(Clone, Debug, new, Getters, CopyGetters, MutGetters)]
pub struct NodeStream {
    /// The type of the records going through the entire stream.
    #[getset(get = "pub")]
    record_type: StreamRecordType,
    /// The record variant for a specific node.
    #[getset(get_copy = "pub")]
    variant_id: RecordVariantId,
    /// The source to connect to in order to read records from it.
    #[getset(get = "pub")]
    source: NodeStreamSource,
}

impl NodeStream {
    /// Creates a new stream with a different source.
    pub fn with_source(&self, source: NodeStreamSource) -> Self {
        Self {
            source,
            ..self.clone()
        }
    }

    pub fn definition_fragments<'a>(
        &'a self,
        module_prefix: &'a FullyQualifiedName,
    ) -> RecordDefinitionFragments<'a> {
        RecordDefinitionFragments {
            stream: self,
            module_prefix,
        }
    }
}

pub struct RecordDefinitionFragments<'a> {
    stream: &'a NodeStream,
    module_prefix: &'a FullyQualifiedName,
}

impl<'a> RecordDefinitionFragments<'a> {
    pub fn record(&self) -> syn::Type {
        syn::parse_str::<syn::Type>(&format!(
            "{}::{}::Record{}",
            self.module_prefix, self.stream.record_type, self.stream.variant_id
        ))
        .expect("record")
    }

    pub fn unpacked_record(&self) -> syn::Type {
        syn::parse_str::<syn::Type>(&format!(
            "{}::{}::UnpackedRecord{}",
            self.module_prefix, self.stream.record_type, self.stream.variant_id
        ))
        .expect("unpacked_record")
    }

    pub fn unpacked_record_in(&self) -> syn::Type {
        syn::parse_str::<syn::Type>(&format!(
            "{}::{}::UnpackedRecordIn{}",
            self.module_prefix, self.stream.record_type, self.stream.variant_id
        ))
        .expect("unpacked_record_in")
    }

    pub fn record_and_unpacked_out(&self) -> syn::Type {
        syn::parse_str::<syn::Type>(&format!(
            "{}::{}::Record{}AndUnpackedOut",
            self.module_prefix, self.stream.record_type, self.stream.variant_id
        ))
        .expect("record_and_unpacked_out")
    }
}
