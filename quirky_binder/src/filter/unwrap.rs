use proc_macro2::TokenStream;
use serde::Deserialize;
use truc::record::{definition::DatumDefinition, type_resolver::TypeResolver};

use super::transform::{
    SubTransform, SubTransformParams, SubTransformSpec, Transform, TransformParams, TransformSpec,
};
use crate::{prelude::*, trace_element};

enum NoneStrategy {
    Skip,
    Fail { error_type: syn::Ident },
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
        datum: &DatumDefinition,
    ) -> ChainResultWithTrace<(ValidFieldName, ValidFieldType)> {
        let optional_type_name = datum.type_name().to_string();
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
                    })
                    .with_trace_element(trace_element!(UNWRAP_TRACE_NAME));
                }
            }
        };
        let valid_type = ValidFieldType::try_from(type_name)
            .map_err(|_| ChainError::InvalidFieldType {
                type_name: type_name.to_owned(),
            })
            .with_trace_element(trace_element!(UNWRAP_TRACE_NAME))?;
        Ok((name, valid_type))
    }

    fn update_facts<R: TypeResolver + Copy>(
        &self,
        _output_stream: &mut OutputBuilderForUpdate<R, DerivedExtra>,
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
            NoneStrategy::Fail { error_type } => {
                quote! {
                    return Err(#error_type::custom(
                        format!("found None for field `{}`", #name)
                    ));
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

pub fn unwrap<R: TypeResolver + Copy>(
    graph: &mut GraphBuilder<R>,
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
                    error_type: graph.chain_customizer().error_type.to_name(),
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
        datum: &DatumDefinition,
    ) -> ChainResultWithTrace<(ValidFieldName, ValidFieldType)> {
        let optional_type_name = datum.type_name().to_string();
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
                    })
                    .with_trace_element(trace_element!(SUB_UNWRAP_TRACE_NAME));
                }
            }
        };
        let valid_type = ValidFieldType::try_from(type_name)
            .map_err(|_| ChainError::InvalidFieldType {
                type_name: type_name.to_owned(),
            })
            .with_trace_element(trace_element!(SUB_UNWRAP_TRACE_NAME))?;
        Ok((name, valid_type))
    }

    fn update_facts<R: TypeResolver + Copy>(
        &self,
        _output_stream: &mut SubStreamBuilderForUpdate<R>,
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
                    return VecElementConversionResult::Abandonned;
                }
            }
            NoneStrategy::Fail { error_type } => {
                quote! {
                    return Err(#error_type::custom(
                        format!("found None for field `{}`", #name)
                    ));
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

pub fn sub_unwrap<R: TypeResolver + Copy>(
    graph: &mut GraphBuilder<R>,
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
                    error_type: graph.chain_customizer().error_type.to_name(),
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
