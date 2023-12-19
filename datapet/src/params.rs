use std::cell::Ref;

use serde::Deserialize;
use truc::record::{definition::RecordDefinitionBuilder, type_resolver::TypeResolver};

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
    pub fn validate<L, TRACE>(self, lookup: L, trace: TRACE) -> ChainResult<Self>
    where
        L: Fn(&str) -> bool,
        TRACE: Fn() -> Trace<'static>,
    {
        for f in self.iter() {
            if !lookup(AsRef::<str>::as_ref(*f)) {
                return Err(ChainError::FieldNotFound {
                    field: AsRef::<str>::as_ref(*f).to_owned(),
                    trace: trace(),
                });
            }
        }
        Ok(self)
    }

    pub fn validate_on_record_definition<R, TRACE>(
        self,
        def: &RecordDefinitionBuilder<R>,
        trace: TRACE,
    ) -> ChainResult<Self>
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
    ) -> ChainResult<Self>
    where
        R: TypeResolver + Copy,
        TRACE: Fn() -> Trace<'static>,
    {
        let def = graph
            .get_stream(stream.record_type())
            .ok_or_else(|| ChainError::StreamNotFound {
                stream: stream.record_type().to_string(),
                trace: trace(),
            })?
            .borrow();
        self.validate_on_record_definition(&def, trace)
    }

    pub fn validate_path_on_stream<'g, R, TRACE>(
        self,
        stream: &NodeStream,
        graph: &'g GraphBuilder<R>,
        trace: TRACE,
    ) -> ChainResult<(Self, Ref<'g, RecordDefinitionBuilder<R>>)>
    where
        R: TypeResolver + Copy,
        TRACE: Fn() -> Trace<'static>,
    {
        let (mut s, mut def) = {
            let def = graph
                .get_stream(stream.record_type())
                .ok_or_else(|| ChainError::StreamNotFound {
                    stream: stream.record_type().to_string(),
                    trace: trace(),
                })?
                .borrow();
            let datum = def
                .get_current_datum_definition_by_name(self[0])
                .ok_or_else(|| ChainError::FieldNotFound {
                    field: self[0].to_owned(),
                    trace: trace(),
                })?;
            let s = &stream.sub_streams()[&datum.id()];
            let def = graph
                .get_stream(s.record_type())
                .ok_or_else(|| ChainError::StreamNotFound {
                    stream: stream.record_type().to_string(),
                    trace: trace(),
                })?
                .borrow();
            (s, def)
        };
        for field in &self[1..] {
            let datum = def
                .get_current_datum_definition_by_name(field)
                .ok_or_else(|| ChainError::FieldNotFound {
                    field: (*field).to_owned(),
                    trace: trace(),
                })?;
            s = &s.sub_streams()[&datum.id()];
            def = graph
                .get_stream(s.record_type())
                .ok_or_else(|| ChainError::StreamNotFound {
                    stream: stream.record_type().to_string(),
                    trace: trace(),
                })?
                .borrow();
        }
        Ok((self, def))
    }
}

#[derive(Deserialize, Debug, Deref)]
pub struct DirectedFieldsParam<'a>(#[serde(borrow)] Box<[Directed<&'a str>]>);

impl<'a> DirectedFieldsParam<'a> {
    pub fn validate<L, TRACE>(self, lookup: L, trace: TRACE) -> ChainResult<Self>
    where
        L: Fn(&str) -> bool,
        TRACE: Fn() -> Trace<'static>,
    {
        for f in self.iter() {
            if !lookup(AsRef::<str>::as_ref(**f)) {
                return Err(ChainError::FieldNotFound {
                    field: AsRef::<str>::as_ref(**f).to_owned(),
                    trace: trace(),
                });
            }
        }
        Ok(self)
    }

    pub fn validate_on_record_definition<R, TRACE>(
        self,
        def: &RecordDefinitionBuilder<R>,
        trace: TRACE,
    ) -> ChainResult<Self>
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
    ) -> ChainResult<Self>
    where
        R: TypeResolver + Copy,
        TRACE: Fn() -> Trace<'static>,
    {
        let def = graph
            .get_stream(stream.record_type())
            .ok_or_else(|| ChainError::StreamNotFound {
                stream: stream.record_type().to_string(),
                trace: trace(),
            })?
            .borrow();
        self.validate_on_record_definition(&def, trace)
    }
}

#[derive(Deserialize, Debug, Deref)]
pub struct TypedFieldsParam<'a>(#[serde(borrow)] Box<[(&'a str, &'a str)]>);

impl<'a> TypedFieldsParam<'a> {
    pub fn validate<TRACE>(self, trace: TRACE) -> ChainResult<Vec<(ValidFieldName, ValidFieldType)>>
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
