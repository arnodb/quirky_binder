use std::{cell::Ref, iter::once};

use serde::Deserialize;
use truc::record::{
    definition::{DatumDefinition, RecordDefinitionBuilder},
    type_resolver::TypeResolver,
};

use crate::{
    chain::{error::ChainError, ChainResult, Trace},
    prelude::{GraphBuilder, NodeStream},
    support::{
        cmp::Directed,
        valid::{ValidFieldName, ValidFieldType},
    },
};

#[derive(Deserialize, Debug, Deref)]
pub struct FieldsParam<'a>(#[serde(borrow)] Box<[&'a str]>);

impl<'a> FieldsParam<'a> {
    pub fn empty() -> Self {
        Self(Default::default())
    }

    pub fn validate<L, TRACE>(self, lookup: L, trace: TRACE) -> ChainResult<Vec<ValidFieldName>>
    where
        L: Fn(&ValidFieldName) -> bool,
        TRACE: Fn() -> Trace<'static> + Copy,
    {
        self.validate_ext(
            |name| {
                if lookup(&name) {
                    Ok(name)
                } else {
                    Err(ChainError::FieldNotFound {
                        field: name.name().to_owned(),
                        trace: trace(),
                    })
                }
            },
            trace,
        )
    }

    pub fn validate_ext<L, O, TRACE>(self, lookup: L, trace: TRACE) -> ChainResult<Vec<O>>
    where
        L: Fn(ValidFieldName) -> ChainResult<O>,
        TRACE: Fn() -> Trace<'static>,
    {
        let valid_fields = self
            .iter()
            .map(|name| {
                let valid =
                    ValidFieldName::try_from(*name).map_err(|_| ChainError::InvalidFieldName {
                        name: (*name).to_owned(),
                        trace: trace(),
                    })?;
                lookup(valid)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(valid_fields)
    }

    pub fn validate_on_record_definition<R, TRACE>(
        self,
        def: &RecordDefinitionBuilder<R>,
        trace: TRACE,
    ) -> ChainResult<Vec<ValidFieldName>>
    where
        R: TypeResolver,
        TRACE: Fn() -> Trace<'static> + Copy,
    {
        self.validate_on_record_definition_ext(def, |name, _datum| Ok(name), trace)
    }

    pub fn validate_on_record_definition_ext<R, M, O, TRACE>(
        self,
        def: &RecordDefinitionBuilder<R>,
        try_map: M,
        trace: TRACE,
    ) -> ChainResult<Vec<O>>
    where
        R: TypeResolver,
        M: Fn(ValidFieldName, &DatumDefinition) -> ChainResult<O>,
        TRACE: Fn() -> Trace<'static> + Copy,
    {
        self.validate_ext(
            |name| {
                if let Some(datum) = def.get_current_datum_definition_by_name(name.name()) {
                    try_map(name, datum)
                } else {
                    Err(ChainError::FieldNotFound {
                        field: name.name().to_owned(),
                        trace: trace(),
                    })
                }
            },
            trace,
        )
    }

    pub fn validate_on_stream<R, TRACE>(
        self,
        stream: &NodeStream,
        graph: &GraphBuilder<R>,
        trace: TRACE,
    ) -> ChainResult<Vec<ValidFieldName>>
    where
        R: TypeResolver + Copy,
        TRACE: Fn() -> Trace<'static> + Copy,
    {
        self.validate_on_stream_ext(stream, graph, |name, _datum| Ok(name), trace)
    }

    pub fn validate_on_stream_ext<R, M, O, TRACE>(
        self,
        stream: &NodeStream,
        graph: &GraphBuilder<R>,
        try_map: M,
        trace: TRACE,
    ) -> ChainResult<Vec<O>>
    where
        R: TypeResolver + Copy,
        M: Fn(ValidFieldName, &DatumDefinition) -> ChainResult<O>,
        TRACE: Fn() -> Trace<'static> + Copy,
    {
        let def = graph.get_stream(stream.record_type(), trace)?.borrow();
        self.validate_on_record_definition_ext(&def, try_map, trace)
    }

    pub fn validate_path_on_stream<'g, R, TRACE>(
        self,
        stream: &NodeStream,
        graph: &'g GraphBuilder<R>,
        trace: TRACE,
    ) -> ChainResult<(Vec<ValidFieldName>, Ref<'g, RecordDefinitionBuilder<R>>)>
    where
        R: TypeResolver + Copy,
        TRACE: Fn() -> Trace<'static> + Copy,
    {
        let (valid_first, mut s, mut def) = {
            let valid =
                ValidFieldName::try_from(self[0]).map_err(|_| ChainError::InvalidFieldName {
                    name: (self[0]).to_owned(),
                    trace: trace(),
                })?;
            let def = graph.get_stream(stream.record_type(), trace)?.borrow();
            let datum = def
                .get_current_datum_definition_by_name(valid.name())
                .ok_or_else(|| ChainError::FieldNotFound {
                    field: valid.name().to_owned(),
                    trace: trace(),
                })?;
            let s = &stream.sub_streams()[&datum.id()];
            let def = graph.get_stream(s.record_type(), trace)?.borrow();
            (valid, s, def)
        };
        let valid_fields = once(Ok(valid_first))
            .chain(self[1..].iter().map(|name| {
                let valid =
                    ValidFieldName::try_from(*name).map_err(|_| ChainError::InvalidFieldName {
                        name: (*name).to_owned(),
                        trace: trace(),
                    })?;
                let datum = def
                    .get_current_datum_definition_by_name(valid.name())
                    .ok_or_else(|| ChainError::FieldNotFound {
                        field: (valid.name()).to_owned(),
                        trace: trace(),
                    })?;
                s = &s.sub_streams()[&datum.id()];
                def = graph.get_stream(s.record_type(), trace)?.borrow();
                Ok(valid)
            }))
            .collect::<Result<Vec<_>, _>>()?;
        Ok((valid_fields, def))
    }
}

#[derive(Deserialize, Debug, Deref)]
pub struct DirectedFieldsParam<'a>(#[serde(borrow)] Box<[Directed<&'a str>]>);

impl<'a> DirectedFieldsParam<'a> {
    pub fn validate<L, TRACE>(
        self,
        lookup: L,
        trace: TRACE,
    ) -> ChainResult<Vec<Directed<ValidFieldName>>>
    where
        L: Fn(&str) -> bool,
        TRACE: Fn() -> Trace<'static>,
    {
        let valid_fields = self
            .iter()
            .map(|dir_name| {
                let valid = ValidFieldName::try_from(**dir_name).map_err(|_| {
                    ChainError::InvalidFieldName {
                        name: (**dir_name).to_owned(),
                        trace: trace(),
                    }
                })?;
                if !lookup(valid.name()) {
                    return Err(ChainError::FieldNotFound {
                        field: valid.name().to_owned(),
                        trace: trace(),
                    });
                }
                Ok(dir_name.map(|_name| valid))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(valid_fields)
    }

    pub fn validate_on_record_definition<R, TRACE>(
        self,
        def: &RecordDefinitionBuilder<R>,
        trace: TRACE,
    ) -> ChainResult<Vec<Directed<ValidFieldName>>>
    where
        R: TypeResolver,
        TRACE: Fn() -> Trace<'static>,
    {
        self.validate(
            |field| def.get_current_datum_definition_by_name(field).is_some(),
            trace,
        )
    }

    pub fn validate_on_stream<R, TRACE>(
        self,
        stream: &NodeStream,
        graph: &GraphBuilder<R>,
        trace: TRACE,
    ) -> ChainResult<Vec<Directed<ValidFieldName>>>
    where
        R: TypeResolver + Copy,
        TRACE: Fn() -> Trace<'static> + Copy,
    {
        let def = graph.get_stream(stream.record_type(), trace)?.borrow();
        self.validate_on_record_definition(&def, trace)
    }
}

#[derive(Deserialize, Debug, Deref)]
pub struct TypedFieldsParam<'a>(#[serde(borrow)] Box<[(&'a str, &'a str)]>);

impl<'a> TypedFieldsParam<'a> {
    pub fn validate_new<TRACE>(
        self,
        trace: TRACE,
    ) -> ChainResult<Vec<(ValidFieldName, ValidFieldType)>>
    where
        TRACE: Fn() -> Trace<'static>,
    {
        self.iter()
            .map(|(name, r#type)| {
                let valid_name =
                    ValidFieldName::try_from(*name).map_err(|_| ChainError::InvalidFieldName {
                        name: (*name).to_owned(),
                        trace: trace(),
                    })?;
                let valid_type = ValidFieldType::try_from(*r#type).map_err(|_| {
                    ChainError::InvalidFieldType {
                        type_name: (*r#type).to_owned(),
                        trace: trace(),
                    }
                })?;
                Ok((valid_name, valid_type))
            })
            .collect::<Result<Vec<_>, _>>()
    }
}
