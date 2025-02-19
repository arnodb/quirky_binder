use std::ops::Index;

use crate::prelude::*;

#[derive(Default)]
pub struct QuirkyRecordDefinitionBuilder {
    datum_definitions: Vec<QuirkyDatumDefinition>,
    variants: Vec<QuirkyRecordVariant>,
    data_to_add: Vec<QuirkyDatumId>,
    data_to_remove: Vec<QuirkyDatumId>,
}

impl QuirkyRecordDefinitionBuilder {
    pub fn get_datum_definition(&self, id: QuirkyDatumId) -> Option<&QuirkyDatumDefinition> {
        self.datum_definitions.get(id.0)
    }

    pub fn get_variant(&self, id: QuirkyRecordVariantId) -> Option<&QuirkyRecordVariant> {
        self.variants.get(id.0)
    }

    pub fn add_datum(
        &mut self,
        name: &str,
        datum_type: QuirkyDatumType,
    ) -> ChainResult<QuirkyDatumId> {
        if self.get_current_datum_definition_by_name(name).is_some() {
            return Err(ChainError::FieldAlreadyExistsInStream {
                field: name.to_owned(),
            });
        }
        let id = QuirkyDatumId::from(self.datum_definitions.len());
        self.datum_definitions.push(QuirkyDatumDefinition {
            id,
            name: name.to_owned(),
            datum_type,
        });
        self.data_to_add.push(id);
        Ok(id)
    }

    pub fn copy_datum(&mut self, datum: &QuirkyDatumDefinition) -> ChainResult<QuirkyDatumId> {
        self.add_datum(datum.name(), datum.datum_type().clone())
    }

    pub fn remove_datum(&mut self, datum_id: QuirkyDatumId) -> ChainResult<()> {
        if !self.get_current_data().any(|d| d == datum_id) {
            return Err(ChainError::FieldNotFoundInStream {
                field: self.get_datum_definition(datum_id).unwrap().name.clone(),
            });
        }
        self.data_to_remove.push(datum_id);
        Ok(())
    }

    pub fn close_record_variant(&mut self) -> QuirkyRecordVariantId {
        if !self.data_to_add.is_empty() || !self.data_to_remove.is_empty() {
            let mut data = self
                .variants
                .last()
                .map(|variant| variant.data.clone())
                .unwrap_or_default();
            data.retain(|id| !self.data_to_remove.contains(id));
            data.extend(self.data_to_add.iter());
            self.variants.push(QuirkyRecordVariant { data });
            self.data_to_add.clear();
            self.data_to_remove.clear();
        }
        QuirkyRecordVariantId::from(self.variants.len() - 1)
    }

    pub fn build(self) -> QuirkyRecordDefinition {
        let Self {
            datum_definitions,
            variants,
            data_to_add,
            data_to_remove,
        } = self;
        assert!(data_to_add.is_empty());
        assert!(data_to_remove.is_empty());
        QuirkyRecordDefinition {
            datum_definitions,
            variants,
        }
    }

    pub fn get_variant_datum_definition_by_name(
        &self,
        variant_id: QuirkyRecordVariantId,
        name: &str,
    ) -> Option<&QuirkyDatumDefinition> {
        self.get_variant(variant_id).and_then(|variant| {
            for d in variant.data() {
                let datum = self
                    .datum_definitions
                    .get(d.0)
                    .filter(|datum| datum.name() == name);
                if datum.is_some() {
                    return datum;
                }
            }
            None
        })
    }

    pub fn get_current_data(&self) -> impl Iterator<Item = QuirkyDatumId> + '_ {
        self.variants
            .last()
            .map(|variant| {
                variant
                    .data
                    .iter()
                    .cloned()
                    .filter(|id| !self.data_to_remove.contains(id))
            })
            .into_iter()
            .flatten()
            .chain(self.data_to_add.iter().cloned())
    }

    pub fn get_current_datum_definition_by_name(
        &self,
        name: &str,
    ) -> Option<&QuirkyDatumDefinition> {
        self.get_current_data()
            .filter_map(|d| self.datum_definitions.get(d.0))
            .find(|datum| datum.name() == name)
    }
}

impl Index<QuirkyDatumId> for QuirkyRecordDefinitionBuilder {
    type Output = QuirkyDatumDefinition;

    fn index(&self, index: QuirkyDatumId) -> &Self::Output {
        self.get_datum_definition(index)
            .unwrap_or_else(|| panic!("datum #{} not found", index))
    }
}

impl Index<QuirkyRecordVariantId> for QuirkyRecordDefinitionBuilder {
    type Output = QuirkyRecordVariant;

    fn index(&self, index: QuirkyRecordVariantId) -> &Self::Output {
        self.get_variant(index)
            .unwrap_or_else(|| panic!("variant #{} not found", index))
    }
}
