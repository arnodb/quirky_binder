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
        output_stream: &mut OutputBuilderForPassThrough<R>,
        update_fields: &[ValidFieldName],
        facts_proof: NoFactsUpdated<()>,
    ) -> FactsFullyUpdated<()> {
        let fields = update_fields;
        output_stream.break_order_fact_at(fields.iter().map(ValidFieldName::name));
        output_stream.break_distinct_fact_for(fields.iter().map(ValidFieldName::name));
        facts_proof.order_facts_updated().distinct_facts_updated()
    }

    fn update_field(&self, src: TokenStream) -> TokenStream {
        quote! { #src.to_lowercase().into() }
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
        output_stream: &mut OutputBuilderForPassThrough<R>,
        update_fields: &[ValidFieldName],
        facts_proof: NoFactsUpdated<()>,
    ) -> FactsFullyUpdated<()> {
        let fields = update_fields;
        // TODO nice to have: change order direction
        output_stream.break_order_fact_at(fields.iter().map(ValidFieldName::name));
        facts_proof.order_facts_updated().distinct_facts_updated()
    }

    fn update_field(&self, src: TokenStream) -> TokenStream {
        quote! { #src.chars().rev().collect::<String>().into() }
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
        },
        trace,
        REVERSE_CHARS_TRACE_NAME,
    )
}
