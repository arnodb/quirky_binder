use std::{collections::BTreeSet, fmt::Debug};

use proc_macro2::TokenStream;
use truc::record::definition::DatumDefinition;

use crate::{prelude::*, trace_element};

pub mod string;

#[derive(Debug)]
pub struct TransformParams<'a> {
    pub update_fields: FieldsParam<'a>,
    pub type_update_fields: FieldsParam<'a>,
}

impl Default for TransformParams<'_> {
    fn default() -> Self {
        Self {
            update_fields: FieldsParam::empty(),
            type_update_fields: FieldsParam::empty(),
        }
    }
}

pub trait TransformSpec: Debug {
    fn validate_type_update_field(
        &self,
        _name: ValidFieldName,
        _datum: &DatumDefinition<QuirkyDatumType>,
    ) -> ChainResultWithTrace<(ValidFieldName, ValidFieldType)> {
        unimplemented!()
    }

    fn update_facts(
        &self,
        output_stream: &mut OutputBuilderForUpdate<DerivedExtra>,
        update_fields: &[ValidFieldName],
        type_update_fields: &[(ValidFieldName, ValidFieldType)],
        facts_proof: NoFactsUpdated<()>,
    ) -> ChainResultWithTrace<FactsFullyUpdated<()>>;

    fn update_field(&self, name: &str, src: TokenStream) -> TokenStream;

    fn type_update_field(&self, name: &str, src: TokenStream) -> TokenStream;
}

#[derive(Debug, Getters)]
pub struct Transform<Spec: TransformSpec> {
    spec: Spec,
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 1],
    #[getset(get = "pub")]
    outputs: [NodeStream; 1],
    update_fields: Vec<ValidFieldName>,
    type_update_fields: Vec<(ValidFieldName, ValidFieldType)>,
    new_variant: bool,
}

impl<Spec: TransformSpec> Transform<Spec> {
    pub fn new(
        spec: Spec,
        graph: &mut GraphBuilder,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        params: TransformParams,
    ) -> ChainResultWithTrace<Self> {
        let ValidTransformFieldParams {
            valid_update_fields,
            valid_type_update_fields,
        } = Self::validate(
            &spec,
            graph,
            inputs.single(),
            TransformFieldParams {
                update_fields: params.update_fields,
                type_update_fields: params.type_update_fields,
            },
        )?;

        let mut new_variant = false;

        let mut streams = StreamsBuilder::new(&name, &inputs);
        streams
            .output_from_input(0, true, graph)
            .with_trace_element(trace_element!())?
            .update(|output_stream, facts_proof| {
                {
                    let mut record_definition = output_stream.record_definition().borrow_mut();

                    for (f, t) in &valid_type_update_fields {
                        let datum_id = record_definition
                            .get_current_datum_definition_by_name(f.name())
                            .expect("datum")
                            .id();
                        record_definition
                            .remove_datum(datum_id)
                            .map_err(|err| ChainError::Other { msg: err })
                            .with_trace_element(trace_element!())?;
                        record_definition
                            .add_datum(
                                f.name(),
                                QuirkyDatumType::Simple {
                                    type_name: t.type_name().to_owned(),
                                },
                            )
                            .map_err(|err| ChainError::Other { msg: err })
                            .with_trace_element(trace_element!())?;
                        new_variant = true;
                    }
                }

                spec.update_facts(
                    output_stream,
                    &valid_update_fields,
                    &valid_type_update_fields,
                    facts_proof,
                )
            })?;

        let outputs = streams.build().with_trace_element(trace_element!())?;

        Ok(Self {
            spec,
            name,
            inputs,
            outputs,
            update_fields: valid_update_fields,
            type_update_fields: valid_type_update_fields,
            new_variant,
        })
    }

    fn validate(
        spec: &Spec,
        graph: &mut GraphBuilder,
        input: &NodeStream,
        params: TransformFieldParams,
    ) -> ChainResultWithTrace<ValidTransformFieldParams> {
        let valid_update_fields = params.update_fields.validate_on_stream(input, graph)?;

        let valid_type_update_fields =
            params
                .type_update_fields
                .validate_on_stream_ext(input, graph, |name, datum| {
                    spec.validate_type_update_field(name, datum)
                })?;

        Ok(ValidTransformFieldParams {
            valid_update_fields,
            valid_type_update_fields,
        })
    }
}

struct TransformFieldParams<'a> {
    update_fields: FieldsParam<'a>,
    type_update_fields: FieldsParam<'a>,
}

struct ValidTransformFieldParams {
    valid_update_fields: Vec<ValidFieldName>,
    valid_type_update_fields: Vec<(ValidFieldName, ValidFieldType)>,
}

impl<Spec: TransformSpec> DynNode for Transform<Spec> {
    fn name(&self) -> &FullyQualifiedName {
        &self.name
    }

    fn inputs(&self) -> &[NodeStream] {
        &self.inputs
    }

    fn outputs(&self) -> &[NodeStream] {
        &self.outputs
    }

    fn gen_chain(&self, graph: &Graph, chain: &mut Chain) {
        let updates = {
            let mut_fields = self.update_fields.iter().map(ValidFieldName::mut_ident);

            let update_ops = self.update_fields.iter().map(|field| {
                let name = field.name();
                let ident = field.ident();
                self.spec.update_field(name, quote! { record.#ident() })
            });

            quote! {
                #[allow(clippy::useless_conversion)]
                {
                    #(
                        *record.#mut_fields() = #update_ops;
                    )*
                }
            }
        };

        let new_variant_body = if self.new_variant {
            let record_definition =
                &graph.record_definitions()[self.outputs.single().record_type()];
            let variant = &record_definition[self.outputs.single().variant_id()];

            let mut touched_fields = BTreeSet::<&str>::new();

            let type_updates = {
                let fields = self
                    .type_update_fields
                    .iter()
                    .map(|(name, _type)| name.ident());

                let update_ops = self.type_update_fields.iter().map(|(field, _type)| {
                    touched_fields.insert(field.name());
                    let name = field.name();
                    let ident = field.ident();
                    self.spec
                        .type_update_field(name, quote! { unpacked.#ident })
                });

                quote! {
                    #(
                        let #fields = #update_ops;
                    )*
                }
            };

            let unpacked_fields = {
                let fields = variant.data().map(|d| {
                    let datum = &record_definition[d];
                    let name_ident = format_ident!("{}", datum.name());
                    if touched_fields.contains(datum.name()) {
                        quote! { #name_ident }
                    } else {
                        quote! { #name_ident: unpacked.#name_ident }
                    }
                });
                quote!(#(#fields),*)
            };
            Some(quote! {
                let unpacked = record.unpack();

                #type_updates

                let record = Output0::from(UnpackedOutput0 { #unpacked_fields });
            })
        } else {
            None
        };

        let record_param = if self.update_fields.is_empty() {
            quote! { record }
        } else {
            quote! { mut record }
        };

        let inline_body = quote! {
            input.filter_map(|#record_param| {
                #updates

                #new_variant_body

                Ok(Some(record))
            })
        };

        chain.implement_inline_node(
            self,
            self.inputs.some_single(),
            self.outputs.single(),
            &inline_body,
        );
    }
}

#[derive(Debug)]
pub struct SubTransformParams<'a> {
    pub path_fields: FieldsParam<'a>,
    pub update_fields: FieldsParam<'a>,
    pub type_update_fields: FieldsParam<'a>,
}

impl Default for SubTransformParams<'_> {
    fn default() -> Self {
        Self {
            path_fields: FieldsParam::empty(),
            update_fields: FieldsParam::empty(),
            type_update_fields: FieldsParam::empty(),
        }
    }
}

pub trait SubTransformSpec: Debug {
    fn validate_type_update_field(
        &self,
        _name: ValidFieldName,
        _datum: &DatumDefinition<QuirkyDatumType>,
    ) -> ChainResultWithTrace<(ValidFieldName, ValidFieldType)> {
        unimplemented!()
    }

    fn update_facts(
        &self,
        output_stream: &mut SubStreamBuilderForUpdate<DerivedExtra>,
        update_fields: &[ValidFieldName],
        type_update_fields: &[(ValidFieldName, ValidFieldType)],
        facts_proof: NoFactsUpdated<()>,
    ) -> ChainResultWithTrace<FactsFullyUpdated<()>>;

    fn update_field(&self, name: &str, src: TokenStream) -> TokenStream;

    fn type_update_field(&self, name: &str, src: TokenStream) -> TokenStream;
}

#[derive(Debug, Getters)]
pub struct SubTransform<Spec: SubTransformSpec> {
    spec: Spec,
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 1],
    #[getset(get = "pub")]
    outputs: [NodeStream; 1],
    path_streams: Vec<PathUpdateElement>,
    update_fields: Vec<ValidFieldName>,
    type_update_fields: Vec<(ValidFieldName, ValidFieldType)>,
    new_variant: bool,
}

impl<Spec: SubTransformSpec> SubTransform<Spec> {
    pub fn new(
        spec: Spec,
        graph: &mut GraphBuilder,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        params: SubTransformParams,
    ) -> ChainResultWithTrace<Self> {
        let ValidSubTransformFieldParams {
            valid_path_fields,
            valid_update_fields,
            valid_type_update_fields,
        } = Self::validate(
            &spec,
            graph,
            inputs.single(),
            SubTransformFieldParams {
                path_fields: params.path_fields,
                update_fields: params.update_fields,
                type_update_fields: params.type_update_fields,
            },
        )?;

        let mut new_variant = false;

        let mut streams = StreamsBuilder::new(&name, &inputs);
        let path_streams = streams
            .output_from_input(0, true, graph)
            .with_trace_element(trace_element!())?
            .update_path(
                graph,
                &valid_path_fields,
                |_output_stream, sub_output_stream, facts_proof| {
                    {
                        let mut record_definition =
                            sub_output_stream.record_definition().borrow_mut();

                        for (f, t) in &valid_type_update_fields {
                            let datum_id = record_definition
                                .get_current_datum_definition_by_name(f.name())
                                .expect("datum")
                                .id();
                            record_definition
                                .remove_datum(datum_id)
                                .map_err(|err| ChainError::Other { msg: err })
                                .with_trace_element(trace_element!())?;
                            record_definition
                                .add_datum(
                                    f.name(),
                                    QuirkyDatumType::Simple {
                                        type_name: t.type_name().to_owned(),
                                    },
                                )
                                .map_err(|err| ChainError::Other { msg: err })
                                .with_trace_element(trace_element!())?;
                            new_variant = true;
                        }
                    }

                    spec.update_facts(
                        sub_output_stream,
                        &valid_update_fields,
                        &valid_type_update_fields,
                        facts_proof,
                    )
                },
            )?;

        let outputs = streams.build().with_trace_element(trace_element!())?;

        Ok(Self {
            spec,
            name,
            inputs,
            outputs,
            path_streams,
            update_fields: valid_update_fields,
            type_update_fields: valid_type_update_fields,
            new_variant,
        })
    }

    fn validate(
        spec: &Spec,
        graph: &mut GraphBuilder,
        input: &NodeStream,
        params: SubTransformFieldParams,
    ) -> ChainResultWithTrace<ValidSubTransformFieldParams> {
        let (valid_path_fields, path_def) = params
            .path_fields
            .validate_path_on_stream(input, graph)
            .with_trace_element(trace_element!())?;

        let valid_update_fields = params
            .update_fields
            .validate_on_record_definition(&path_def)
            .with_trace_element(trace_element!())?;

        let valid_type_update_fields = params
            .type_update_fields
            .validate_on_record_definition_ext(&path_def, |name, datum| {
                spec.validate_type_update_field(name, datum)
            })?;

        Ok(ValidSubTransformFieldParams {
            valid_path_fields,
            valid_update_fields,
            valid_type_update_fields,
        })
    }
}

struct SubTransformFieldParams<'a> {
    path_fields: FieldsParam<'a>,
    update_fields: FieldsParam<'a>,
    type_update_fields: FieldsParam<'a>,
}

struct ValidSubTransformFieldParams {
    valid_path_fields: Vec<ValidFieldName>,
    valid_update_fields: Vec<ValidFieldName>,
    valid_type_update_fields: Vec<(ValidFieldName, ValidFieldType)>,
}

impl<Spec: SubTransformSpec> DynNode for SubTransform<Spec> {
    fn name(&self) -> &FullyQualifiedName {
        &self.name
    }

    fn inputs(&self) -> &[NodeStream] {
        &self.inputs
    }

    fn outputs(&self) -> &[NodeStream] {
        &self.outputs
    }

    fn gen_chain(&self, graph: &Graph, chain: &mut Chain) {
        let updates = {
            let mut_fields = self.update_fields.iter().map(ValidFieldName::mut_ident);

            let update_ops = self.update_fields.iter().map(|field| {
                let name = field.name();
                let ident = field.ident();
                self.spec.update_field(name, quote! { record.#ident() })
            });

            quote! {
                #[allow(clippy::useless_conversion)]
                {
                    #(
                        *record.#mut_fields() = #update_ops;
                    )*
                }
            }
        };

        let new_variant_body = if self.new_variant {
            let path_stream = self.path_streams.last().expect("last path field");

            let def = chain
                .customizer()
                .definition_fragments(&path_stream.sub_output_stream);
            let record = def.record();
            let unpacked_record = def.unpacked_record();

            let record_definition =
                &graph.record_definitions()[path_stream.sub_output_stream.record_type()];
            let variant = &record_definition[path_stream.sub_output_stream.variant_id()];

            let mut touched_fields = BTreeSet::<&str>::new();

            let type_updates = {
                let fields = self
                    .type_update_fields
                    .iter()
                    .map(|(name, _type)| name.ident());

                let update_ops = self.type_update_fields.iter().map(|(field, _type)| {
                    touched_fields.insert(field.name());
                    let name = field.name();
                    let ident = field.ident();
                    self.spec
                        .type_update_field(name, quote! { unpacked.#ident })
                });

                quote! {
                    #(
                        let #fields = #update_ops;
                    )*
                }
            };

            let unpacked_fields = {
                let fields = variant.data().map(|d| {
                    let datum = &record_definition[d];
                    let name_ident = format_ident!("{}", datum.name());
                    if touched_fields.contains(datum.name()) {
                        quote! { #name_ident }
                    } else {
                        quote! { #name_ident: unpacked.#name_ident }
                    }
                });
                quote!(#(#fields),*)
            };
            Some(quote! {
                let unpacked = record.unpack();

                #type_updates

                let record = #record::from(#unpacked_record { #unpacked_fields });
            })
        } else {
            None
        };

        let record_param = if self.update_fields.is_empty() {
            quote! { record }
        } else {
            quote! { mut record }
        };

        let error_type = graph.chain_customizer().error_type.to_full_name();

        let build_leaf_body = |input_record, record, access, mut_access| {
            quote! {
                // TODO optimize this code in truc
                let converted = try_convert_vec_in_place::<#input_record, #record, _, #error_type>(
                    #access,
                    |#record_param, _prev_record| -> Result<_, #error_type> {
                        #updates

                        #new_variant_body

                        Ok(VecElementConversionResult::Converted(record))
                    }
                )?;
                *record.#mut_access() = converted;
            }
        };

        chain.implement_path_update(
            self,
            self.inputs.single(),
            self.outputs.single(),
            &self.path_streams,
            None,
            build_leaf_body,
        );
    }
}
