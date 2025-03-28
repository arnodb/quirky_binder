use std::{cell::Ref, iter::once};

use serde::Deserialize;
use truc::record::definition::DatumDefinition;

use crate::{prelude::*, trace_element};

#[derive(Deserialize, Debug, Deref)]
pub struct FieldParam<'a>(#[serde(borrow)] &'a str);

impl FieldParam<'_> {
    pub fn validate<L>(self, lookup: L) -> ChainResult<ValidFieldName>
    where
        L: Fn(&ValidFieldName) -> bool,
    {
        self.validate_ext(|name| {
            if lookup(&name) {
                Ok(name)
            } else {
                Err(ChainError::FieldNotFound {
                    field: name.name().to_owned(),
                })
            }
        })
    }

    pub fn validate_ext<L, O>(self, lookup: L) -> ChainResult<O>
    where
        L: Fn(ValidFieldName) -> ChainResult<O>,
    {
        let valid = ValidFieldName::try_from(*self).map_err(|_| ChainError::InvalidFieldName {
            name: (*self).to_owned(),
        })?;
        lookup(valid)
    }

    pub fn validate_on_record_definition(
        self,
        def: &QuirkyRecordDefinitionBuilder,
    ) -> ChainResult<ValidFieldName> {
        self.validate_on_record_definition_ext(def, |name, _datum| Ok(name))
    }

    pub fn validate_on_record_definition_ext<M, O>(
        self,
        def: &QuirkyRecordDefinitionBuilder,
        try_map: M,
    ) -> ChainResult<O>
    where
        M: Fn(ValidFieldName, &DatumDefinition<QuirkyDatumType>) -> ChainResult<O>,
    {
        self.validate_ext(|name| {
            if let Some(datum) = def.get_current_datum_definition_by_name(name.name()) {
                try_map(name, datum)
            } else {
                Err(ChainError::FieldNotFound {
                    field: name.name().to_owned(),
                })
            }
        })
    }

    pub fn validate_on_stream(
        self,
        stream: &NodeStream,
        graph: &GraphBuilder,
    ) -> ChainResult<ValidFieldName> {
        self.validate_on_stream_ext(stream, graph, |name, _datum| Ok(name))
    }

    pub fn validate_on_stream_ext<M, O>(
        self,
        stream: &NodeStream,
        graph: &GraphBuilder,
        try_map: M,
    ) -> ChainResult<O>
    where
        M: Fn(ValidFieldName, &DatumDefinition<QuirkyDatumType>) -> ChainResult<O>,
    {
        let def = graph.get_stream(stream.record_type())?.borrow();
        self.validate_on_record_definition_ext(&def, try_map)
    }
}

#[derive(Deserialize, Debug, Deref)]
pub struct FieldsParam<'a>(#[serde(borrow)] Box<[&'a str]>);

impl FieldsParam<'_> {
    pub fn empty() -> Self {
        Self(Default::default())
    }

    pub fn validate_new(self) -> ChainResult<Vec<ValidFieldName>> {
        self.iter()
            .map(|name| {
                ValidFieldName::try_from(*name).map_err(|_| ChainError::InvalidFieldName {
                    name: (*name).to_owned(),
                })
            })
            .collect::<Result<Vec<_>, _>>()
    }

    pub fn validate<L>(
        self,
        lookup: L,
        trace_name: &str,
    ) -> ChainResultWithTrace<Vec<ValidFieldName>>
    where
        L: Fn(&ValidFieldName) -> bool,
    {
        self.validate_ext(
            |name| {
                if lookup(&name) {
                    Ok(name)
                } else {
                    Err(ChainError::FieldNotFound {
                        field: name.name().to_owned(),
                    })
                    .with_trace_element(trace_element!(trace_name))
                }
            },
            trace_name,
        )
    }

    pub fn validate_ext<L, O>(self, lookup: L, trace_name: &str) -> ChainResultWithTrace<Vec<O>>
    where
        L: Fn(ValidFieldName) -> ChainResultWithTrace<O>,
    {
        let valid_fields = self
            .iter()
            .map(|name| {
                let valid = ValidFieldName::try_from(*name)
                    .map_err(|_| ChainError::InvalidFieldName {
                        name: (*name).to_owned(),
                    })
                    .with_trace_element(trace_element!(trace_name))?;
                lookup(valid)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(valid_fields)
    }

    pub fn validate_on_record_definition(
        self,
        def: &QuirkyRecordDefinitionBuilder,
        trace_name: &str,
    ) -> ChainResultWithTrace<Vec<ValidFieldName>> {
        self.validate_on_record_definition_ext(def, |name, _datum| Ok(name), trace_name)
    }

    pub fn validate_on_record_definition_ext<M, O>(
        self,
        def: &QuirkyRecordDefinitionBuilder,
        try_map: M,
        trace_name: &str,
    ) -> ChainResultWithTrace<Vec<O>>
    where
        M: Fn(ValidFieldName, &DatumDefinition<QuirkyDatumType>) -> ChainResultWithTrace<O>,
    {
        self.validate_ext(
            |name| {
                if let Some(datum) = def.get_current_datum_definition_by_name(name.name()) {
                    try_map(name, datum)
                } else {
                    Err(ChainError::FieldNotFound {
                        field: name.name().to_owned(),
                    })
                    .with_trace_element(trace_element!(trace_name))
                }
            },
            trace_name,
        )
    }

    pub fn validate_on_stream(
        self,
        stream: &NodeStream,
        graph: &GraphBuilder,
        trace_name: &str,
    ) -> ChainResultWithTrace<Vec<ValidFieldName>> {
        self.validate_on_stream_ext(stream, graph, |name, _datum| Ok(name), trace_name)
    }

    pub fn validate_on_stream_ext<M, O>(
        self,
        stream: &NodeStream,
        graph: &GraphBuilder,
        try_map: M,
        trace_name: &str,
    ) -> ChainResultWithTrace<Vec<O>>
    where
        M: Fn(ValidFieldName, &DatumDefinition<QuirkyDatumType>) -> ChainResultWithTrace<O>,
    {
        let def = graph
            .get_stream(stream.record_type())
            .with_trace_element(trace_element!(trace_name))?
            .borrow();
        self.validate_on_record_definition_ext(&def, try_map, trace_name)
    }

    pub fn validate_path_on_stream<'g>(
        self,
        stream: &NodeStream,
        graph: &'g GraphBuilder,
    ) -> ChainResult<(Vec<ValidFieldName>, Ref<'g, QuirkyRecordDefinitionBuilder>)> {
        let (valid_first, mut s, mut def) = {
            let valid =
                ValidFieldName::try_from(self[0]).map_err(|_| ChainError::InvalidFieldName {
                    name: (self[0]).to_owned(),
                })?;
            let def = graph.get_stream(stream.record_type())?.borrow();
            let datum = def
                .get_current_datum_definition_by_name(valid.name())
                .ok_or_else(|| ChainError::FieldNotFound {
                    field: valid.name().to_owned(),
                })?;
            let s = &stream.sub_streams()[&datum.id()];
            let def = graph.get_stream(s.record_type())?.borrow();
            (valid, s, def)
        };
        let valid_fields = once(Ok(valid_first))
            .chain(self[1..].iter().map(|name| {
                let valid =
                    ValidFieldName::try_from(*name).map_err(|_| ChainError::InvalidFieldName {
                        name: (*name).to_owned(),
                    })?;
                let datum = def
                    .get_current_datum_definition_by_name(valid.name())
                    .ok_or_else(|| ChainError::FieldNotFound {
                        field: (valid.name()).to_owned(),
                    })?;
                s = &s.sub_streams()[&datum.id()];
                def = graph.get_stream(s.record_type())?.borrow();
                Ok(valid)
            }))
            .collect::<Result<Vec<_>, _>>()?;
        Ok((valid_fields, def))
    }
}

#[derive(Deserialize, Debug, Deref)]
pub struct DirectedFieldsParam<'a>(#[serde(borrow)] Box<[Directed<&'a str>]>);

impl DirectedFieldsParam<'_> {
    pub fn validate<L>(self, lookup: L) -> ChainResult<Vec<Directed<ValidFieldName>>>
    where
        L: Fn(&str) -> bool,
    {
        let valid_fields = self
            .iter()
            .map(|dir_name| {
                let valid = ValidFieldName::try_from(**dir_name).map_err(|_| {
                    ChainError::InvalidFieldName {
                        name: (**dir_name).to_owned(),
                    }
                })?;
                if !lookup(valid.name()) {
                    return Err(ChainError::FieldNotFound {
                        field: valid.name().to_owned(),
                    });
                }
                Ok(dir_name.map(|_name| valid))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(valid_fields)
    }

    pub fn validate_on_record_definition(
        self,
        def: &QuirkyRecordDefinitionBuilder,
    ) -> ChainResult<Vec<Directed<ValidFieldName>>> {
        self.validate(|field| def.get_current_datum_definition_by_name(field).is_some())
    }

    pub fn validate_on_stream(
        self,
        stream: &NodeStream,
        graph: &GraphBuilder,
    ) -> ChainResult<Vec<Directed<ValidFieldName>>> {
        let def = graph.get_stream(stream.record_type())?.borrow();
        self.validate_on_record_definition(&def)
    }
}

#[derive(Deserialize, Debug, Deref)]
pub struct TypedFieldsParam<'a>(#[serde(borrow)] Box<[(&'a str, &'a str)]>);

impl TypedFieldsParam<'_> {
    pub fn validate_new(self) -> ChainResult<Vec<(ValidFieldName, ValidFieldType)>> {
        self.iter()
            .map(|(name, r#type)| {
                let valid_name =
                    ValidFieldName::try_from(*name).map_err(|_| ChainError::InvalidFieldName {
                        name: (*name).to_owned(),
                    })?;
                let valid_type = ValidFieldType::try_from(*r#type).map_err(|_| {
                    ChainError::InvalidFieldType {
                        type_name: (*r#type).to_owned(),
                    }
                })?;
                Ok((valid_name, valid_type))
            })
            .collect::<Result<Vec<_>, _>>()
    }
}
