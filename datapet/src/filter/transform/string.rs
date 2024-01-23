use proc_macro2::TokenStream;
use serde::Deserialize;
use truc::record::type_resolver::TypeResolver;

use crate::{
    filter::transform::{Transform, TransformParams, TransformSpec},
    prelude::*,
};

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct InplaceStringParams<'a> {
    #[serde(borrow)]
    fields: FieldsParam<'a>,
}

const TO_LOWERCASE_TRACE_NAME: &str = "to_lowercase";

pub struct ToLowercase;

impl TransformSpec for ToLowercase {
    fn update_facts<R: TypeResolver + Copy>(
        &self,
        output_stream: &mut OutputBuilderForUpdate<R>,
        update_fields: &[ValidFieldName],
        _type_update_fields: &[(ValidFieldName, ValidFieldType)],
        facts_proof: NoFactsUpdated<()>,
    ) -> FactsFullyUpdated<()> {
        output_stream.break_order_fact_at(update_fields.iter().map(ValidFieldName::name));
        output_stream.break_distinct_fact_for(update_fields.iter().map(ValidFieldName::name));
        facts_proof.order_facts_updated().distinct_facts_updated()
    }

    fn update_field(&self, _name: &str, src: TokenStream) -> TokenStream {
        quote! { #src.to_lowercase().into() }
    }

    fn type_update_field(&self, _name: &str, _src: TokenStream) -> TokenStream {
        unimplemented!()
    }
}

pub fn to_lowercase<R: TypeResolver + Copy>(
    graph: &mut GraphBuilder<R>,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    params: InplaceStringParams,
    trace: Trace,
) -> ChainResult<Transform<ToLowercase>> {
    Transform::new(
        ToLowercase,
        graph,
        name,
        inputs,
        TransformParams {
            update_fields: params.fields,
            ..Default::default()
        },
        trace,
        TO_LOWERCASE_TRACE_NAME,
    )
}

const REVERSE_CHARS_TRACE_NAME: &str = "reverse_chars";

pub struct ReverseChars;

impl TransformSpec for ReverseChars {
    fn update_facts<R: TypeResolver + Copy>(
        &self,
        output_stream: &mut OutputBuilderForUpdate<R>,
        update_fields: &[ValidFieldName],
        _type_update_fields: &[(ValidFieldName, ValidFieldType)],
        facts_proof: NoFactsUpdated<()>,
    ) -> FactsFullyUpdated<()> {
        // TODO nice to have: change order direction
        output_stream.break_order_fact_at(update_fields.iter().map(ValidFieldName::name));
        facts_proof.order_facts_updated().distinct_facts_updated()
    }

    fn update_field(&self, _name: &str, src: TokenStream) -> TokenStream {
        quote! { #src.chars().rev().collect::<String>().into() }
    }

    fn type_update_field(&self, _name: &str, _src: TokenStream) -> TokenStream {
        unimplemented!()
    }
}

pub fn reverse_chars<R: TypeResolver + Copy>(
    graph: &mut GraphBuilder<R>,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    params: InplaceStringParams,
    trace: Trace,
) -> ChainResult<Transform<ReverseChars>> {
    Transform::new(
        ReverseChars,
        graph,
        name,
        inputs,
        TransformParams {
            update_fields: params.fields,
            ..Default::default()
        },
        trace,
        REVERSE_CHARS_TRACE_NAME,
    )
}
