//! Replication of `truc` record definition but `truc` agnostic in order to allow later static type
//! resolution.

use std::{collections::BTreeMap, ops::Index};

use truc::record::{
    definition::{
        DatumDefinitionOverride, DatumId, RecordDefinition, RecordDefinitionBuilder,
        RecordVariantId,
    },
    type_resolver::TypeResolver,
};

use crate::{chain::ChainCustomizer, stream::StreamRecordType};

pub mod builder;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Debug)]
pub struct QuirkyDatumId(usize);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Debug)]
pub struct QuirkyRecordVariantId(usize);

#[derive(Clone, Debug)]
pub enum QuirkyDatumType {
    Simple {
        type_name: String,
    },
    Vec {
        record_type: StreamRecordType,
        variant_id: QuirkyRecordVariantId,
    },
}

#[derive(Debug)]
pub struct QuirkyDatumDefinition {
    id: QuirkyDatumId,
    name: String,
    datum_type: QuirkyDatumType,
}

impl QuirkyDatumDefinition {
    pub fn id(&self) -> QuirkyDatumId {
        self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn datum_type(&self) -> &QuirkyDatumType {
        &self.datum_type
    }
}

pub struct QuirkyRecordVariant {
    data: Vec<QuirkyDatumId>,
}

impl QuirkyRecordVariant {
    pub fn data(&self) -> impl Iterator<Item = QuirkyDatumId> + '_ {
        self.data.iter().copied()
    }

    pub fn data_len(&self) -> usize {
        self.data.len()
    }
}

pub struct QuirkyRecordDefinition {
    datum_definitions: Vec<QuirkyDatumDefinition>,
    variants: Vec<QuirkyRecordVariant>,
}

impl QuirkyRecordDefinition {
    pub fn get_datum_definition(&self, id: QuirkyDatumId) -> Option<&QuirkyDatumDefinition> {
        self.datum_definitions.get(id.0)
    }

    pub fn get_variant(&self, id: QuirkyRecordVariantId) -> Option<&QuirkyRecordVariant> {
        self.variants.get(id.0)
    }

    /// Converts to a `truc` record definition and provide a variants mapping.
    pub fn truc<R>(
        &self,
        chain_customizer: &ChainCustomizer,
        type_resolver: R,
    ) -> (
        RecordDefinition,
        BTreeMap<QuirkyRecordVariantId, RecordVariantId>,
    )
    where
        R: TypeResolver,
    {
        let mut truc = RecordDefinitionBuilder::new(type_resolver);
        let mut datum_ids_mapping = BTreeMap::<QuirkyDatumId, DatumId>::new();
        let mut variants_mapping = BTreeMap::<QuirkyRecordVariantId, RecordVariantId>::new();
        for v in 0..self.variants.len() {
            let (to_add, to_remove) = if v == 0 {
                (self.variants[v].data.clone(), Vec::new())
            } else {
                let old = &self.variants[v - 1].data;
                let new = &self.variants[v].data;
                let mut to_add = new.clone();
                to_add.retain(|d| !old.contains(d));
                let mut to_remove = old.clone();
                to_remove.retain(|d| !new.contains(d));
                (to_add, to_remove)
            };
            for d in to_remove {
                truc.remove_datum(datum_ids_mapping[&d]);
            }
            for d in to_add {
                let QuirkyDatumDefinition {
                    id,
                    name,
                    datum_type,
                } = &self.datum_definitions[d.0];
                let truc_datum_id = match datum_type {
                    QuirkyDatumType::Simple { type_name } => {
                        truc.add_dynamic_datum(name, type_name)
                    }
                    QuirkyDatumType::Vec {
                        record_type,
                        variant_id,
                    } => {
                        let module_name =
                            chain_customizer.streams_module_name.sub_n(&***record_type);
                        truc.add_datum_override::<Vec<()>, _>(
                            name,
                            DatumDefinitionOverride {
                                type_name: Some(format!(
                                    "Vec<{}::Record{}>",
                                    module_name, variant_id
                                )),
                                size: None,
                                align: None,
                                allow_uninit: None,
                            },
                        )
                    }
                };
                datum_ids_mapping.insert(*id, truc_datum_id);
            }
            let truc_variant_id = truc.close_record_variant();
            // In theory they are equal in value, but we should not rely on it.
            variants_mapping.insert(QuirkyRecordVariantId(v), truc_variant_id);
        }
        (truc.build(), variants_mapping)
    }
}

impl Index<QuirkyDatumId> for QuirkyRecordDefinition {
    type Output = QuirkyDatumDefinition;

    fn index(&self, index: QuirkyDatumId) -> &Self::Output {
        self.get_datum_definition(index)
            .unwrap_or_else(|| panic!("datum #{} not found", index))
    }
}

impl Index<QuirkyRecordVariantId> for QuirkyRecordDefinition {
    type Output = QuirkyRecordVariant;

    fn index(&self, index: QuirkyRecordVariantId) -> &Self::Output {
        self.get_variant(index)
            .unwrap_or_else(|| panic!("variant #{} not found", index))
    }
}
