use proc_macro2::TokenStream;
use serde::Deserialize;

use super::transform::{
    SubTransform, SubTransformParams, SubTransformSpec, Transform, TransformParams, TransformSpec,
};
use crate::{prelude::*, trace_element};

fn unwrap_field_type<'a>(
    name: &ValidFieldName,
    datum_type: &'a QuirkyDatumType,
) -> ChainResult<&'a str> {
    match datum_type {
        QuirkyDatumType::Simple {
            type_name: optional_type_name,
        } => {
            let type_name = {
                let s = optional_type_name.trim();
                let t = s.strip_prefix("Option").and_then(|t| {
                    let t = t.trim();
                    if t.starts_with('<') && t.ends_with('>') {
                        Some(&t[1..t.len() - 1])
                    } else {
                        None
                    }
                });
                match t {
                    Some(t) => t,
                    None => {
                        return Err(ChainError::Other {
                            msg: format!(
                                "field `{}` is not an option: {}",
                                name.name(),
                                optional_type_name
                            ),
                        });
                    }
                }
            };
            Ok(type_name)
        }
        QuirkyDatumType::Vec { .. } => Err(ChainError::Other {
            msg: format!("field `{}` is not an option, it is a vector", name.name()),
        }),
    }
}

enum NoneStrategy {
    Skip,
    Fail { error: proc_macro2::TokenStream },
}

const UNWRAP_TRACE_NAME: &str = "unwrap";

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct UnwrapParams<'a> {
    #[serde(borrow)]
    fields: FieldsParam<'a>,
    skip_nones: bool,
}

pub struct Unwrap {
    none_strategy: NoneStrategy,
}

impl TransformSpec for Unwrap {
    fn validate_type_update_field(
        &self,
        name: ValidFieldName,
        datum: &QuirkyDatumDefinition,
    ) -> ChainResultWithTrace<(ValidFieldName, ValidFieldType)> {
        let type_name = unwrap_field_type(&name, datum.datum_type())
            .with_trace_element(trace_element!(UNWRAP_TRACE_NAME))?;
        let valid_type = ValidFieldType::try_from(type_name)
            .map_err(|_| ChainError::InvalidFieldType {
                type_name: type_name.to_owned(),
            })
            .with_trace_element(trace_element!(UNWRAP_TRACE_NAME))?;
        Ok((name, valid_type))
    }

    fn update_facts(
        &self,
        _output_stream: &mut OutputBuilderForUpdate<DerivedExtra>,
        _update_fields: &[ValidFieldName],
        _type_update_fields: &[(ValidFieldName, ValidFieldType)],
        facts_proof: NoFactsUpdated<()>,
    ) -> ChainResultWithTrace<FactsFullyUpdated<()>> {
        Ok(facts_proof.order_facts_updated().distinct_facts_updated())
    }

    fn update_field(&self, _name: &str, _src: TokenStream) -> TokenStream {
        unimplemented!()
    }

    fn type_update_field(&self, name: &str, src: TokenStream) -> TokenStream {
        let error = match &self.none_strategy {
            NoneStrategy::Skip => {
                quote! {
                    return Ok(None);
                }
            }
            NoneStrategy::Fail { error } => {
                quote! {
                    return Err(#error!("found None for field `{}`", #name));
                }
            }
        };
        quote! {
            match #src {
                Some(value) => value,
                None => {
                    #error
                }
            }
        }
    }
}

pub fn unwrap(
    graph: &mut GraphBuilder,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    params: UnwrapParams,
) -> ChainResultWithTrace<Transform<Unwrap>> {
    Transform::new(
        Unwrap {
            none_strategy: if params.skip_nones {
                NoneStrategy::Skip
            } else {
                NoneStrategy::Fail {
                    error: graph.chain_customizer().error_macro.to_full_name(),
                }
            },
        },
        graph,
        name,
        inputs,
        TransformParams {
            type_update_fields: params.fields,
            ..Default::default()
        },
        UNWRAP_TRACE_NAME,
    )
}

const SUB_UNWRAP_TRACE_NAME: &str = "sub_unwrap";

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct SubUnwrapParams<'a> {
    #[serde(borrow)]
    path_fields: FieldsParam<'a>,
    fields: FieldsParam<'a>,
    skip_nones: bool,
}

pub struct SubUnwrap {
    none_strategy: NoneStrategy,
}

impl SubTransformSpec for SubUnwrap {
    fn validate_type_update_field(
        &self,
        name: ValidFieldName,
        datum: &QuirkyDatumDefinition,
    ) -> ChainResultWithTrace<(ValidFieldName, ValidFieldType)> {
        let type_name = unwrap_field_type(&name, datum.datum_type())
            .with_trace_element(trace_element!(SUB_UNWRAP_TRACE_NAME))?;
        let valid_type = ValidFieldType::try_from(type_name)
            .map_err(|_| ChainError::InvalidFieldType {
                type_name: type_name.to_owned(),
            })
            .with_trace_element(trace_element!(SUB_UNWRAP_TRACE_NAME))?;
        Ok((name, valid_type))
    }

    fn update_facts(
        &self,
        _output_stream: &mut SubStreamBuilderForUpdate<DerivedExtra>,
        _update_fields: &[ValidFieldName],
        _type_update_fields: &[(ValidFieldName, ValidFieldType)],
        facts_proof: NoFactsUpdated<()>,
    ) -> ChainResultWithTrace<FactsFullyUpdated<()>> {
        Ok(facts_proof.order_facts_updated().distinct_facts_updated())
    }

    fn update_field(&self, _name: &str, _src: TokenStream) -> TokenStream {
        unimplemented!()
    }

    fn type_update_field(&self, name: &str, src: TokenStream) -> TokenStream {
        let error = match &self.none_strategy {
            NoneStrategy::Skip => {
                quote! {
                    return Ok(VecElementConversionResult::Abandonned);
                }
            }
            NoneStrategy::Fail { error } => {
                quote! {
                    return Err(#error!("found None for field `{}`", #name));
                }
            }
        };
        quote! {
            match #src {
                Some(value) => value,
                None => {
                    #error
                }
            }
        }
    }
}

pub fn sub_unwrap(
    graph: &mut GraphBuilder,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    params: SubUnwrapParams,
) -> ChainResultWithTrace<SubTransform<SubUnwrap>> {
    SubTransform::new(
        SubUnwrap {
            none_strategy: if params.skip_nones {
                NoneStrategy::Skip
            } else {
                NoneStrategy::Fail {
                    error: graph.chain_customizer().error_macro.to_full_name(),
                }
            },
        },
        graph,
        name,
        inputs,
        SubTransformParams {
            path_fields: params.path_fields,
            type_update_fields: params.fields,
            ..Default::default()
        },
        SUB_UNWRAP_TRACE_NAME,
    )
}
