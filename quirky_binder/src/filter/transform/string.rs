use proc_macro2::TokenStream;
use serde::Deserialize;
use truc::record::type_resolver::TypeResolver;

use super::{
    SubTransform, SubTransformParams, SubTransformSpec, Transform, TransformParams, TransformSpec,
};
use crate::prelude::*;

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct TransformStringParams<'a> {
    #[serde(borrow)]
    fields: FieldsParam<'a>,
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct SubTransformStringParams<'a> {
    #[serde(borrow)]
    path_fields: FieldsParam<'a>,
    fields: FieldsParam<'a>,
}

const TO_LOWERCASE_TRACE_NAME: &str = "to_lowercase";

pub struct ToLowercase;

impl TransformSpec for ToLowercase {
    fn update_facts<R: TypeResolver + Copy>(
        &self,
        output_stream: &mut OutputBuilderForUpdate<R, DerivedExtra>,
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
    params: TransformStringParams,
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

const SUB_TO_LOWERCASE_TRACE_NAME: &str = "sub_to_lowercase";

pub struct SubToLowercase;

impl SubTransformSpec for SubToLowercase {
    fn update_facts<R: TypeResolver + Copy>(
        &self,
        output_stream: &mut SubStreamBuilderForUpdate<R>,
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

pub fn sub_to_lowercase<R: TypeResolver + Copy>(
    graph: &mut GraphBuilder<R>,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    params: SubTransformStringParams,
    trace: Trace,
) -> ChainResult<SubTransform<SubToLowercase>> {
    SubTransform::new(
        SubToLowercase,
        graph,
        name,
        inputs,
        SubTransformParams {
            path_fields: params.path_fields,
            update_fields: params.fields,
            ..Default::default()
        },
        trace,
        SUB_TO_LOWERCASE_TRACE_NAME,
    )
}

const REVERSE_CHARS_TRACE_NAME: &str = "reverse_chars";

pub struct ReverseChars;

impl TransformSpec for ReverseChars {
    fn update_facts<R: TypeResolver + Copy>(
        &self,
        output_stream: &mut OutputBuilderForUpdate<R, DerivedExtra>,
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
    params: TransformStringParams,
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

const SUB_REVERSE_CHARS_TRACE_NAME: &str = "sub_reverse_chars";

pub struct SubReverseChars;

impl SubTransformSpec for SubReverseChars {
    fn update_facts<R: TypeResolver + Copy>(
        &self,
        output_stream: &mut SubStreamBuilderForUpdate<R>,
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

pub fn sub_reverse_chars<R: TypeResolver + Copy>(
    graph: &mut GraphBuilder<R>,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    params: SubTransformStringParams,
    trace: Trace,
) -> ChainResult<SubTransform<SubReverseChars>> {
    SubTransform::new(
        SubReverseChars,
        graph,
        name,
        inputs,
        SubTransformParams {
            path_fields: params.path_fields,
            update_fields: params.fields,
            ..Default::default()
        },
        trace,
        SUB_REVERSE_CHARS_TRACE_NAME,
    )
}
