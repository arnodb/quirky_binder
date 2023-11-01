use crate::prelude::Directed;
use serde::Deserialize;

#[derive(Deserialize, Debug, Deref)]
pub struct FieldsParam<'a>(#[serde(borrow)] pub Box<[&'a str]>);

#[derive(Deserialize, Debug, Deref)]
pub struct TypedFieldsParam<'a>(#[serde(borrow)] pub Box<[(&'a str, &'a str)]>);

#[derive(Deserialize, Debug, Deref)]
pub struct DirectedFieldsParam<'a>(#[serde(borrow)] pub Box<[Directed<&'a str>]>);
