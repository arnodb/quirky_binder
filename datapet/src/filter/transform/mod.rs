use std::collections::BTreeSet;

use proc_macro2::TokenStream;
use truc::record::{definition::DatumDefinition, type_resolver::TypeResolver};

use crate::{prelude::*, trace_filter};

pub mod string;

#[derive(Debug)]
pub struct TransformParams<'a> {
    pub update_fields: FieldsParam<'a>,
    pub type_update_fields: FieldsParam<'a>,
}

impl<'a> Default for TransformParams<'a> {
    fn default() -> Self {
        Self {
            update_fields: FieldsParam::empty(),
            type_update_fields: FieldsParam::empty(),
        }
    }
}

pub trait TransformSpec {
    fn validate_type_update_field(
        &self,
        _name: ValidFieldName,
        _datum: &DatumDefinition,
        _trace: Trace,
    ) -> ChainResult<(ValidFieldName, ValidFieldType)> {
        unimplemented!()
    }

    fn update_facts<R: TypeResolver + Copy>(
        &self,
        output_stream: &mut OutputBuilderForUpdate<R>,
        update_fields: &[ValidFieldName],
        type_update_fields: &[(ValidFieldName, ValidFieldType)],
        facts_proof: NoFactsUpdated<()>,
    ) -> FactsFullyUpdated<()>;

    fn update_field(&self, name: &str, src: TokenStream) -> TokenStream;

    fn type_update_field(&self, name: &str, src: TokenStream) -> TokenStream;
}

#[derive(Getters)]
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
    pub fn new<R: TypeResolver + Copy>(
        spec: Spec,
        graph: &mut GraphBuilder<R>,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        params: TransformParams,
        trace: Trace,
        trace_name: &str,
    ) -> ChainResult<Self> {
        let valid_update_fields =
            params
                .update_fields
                .validate_on_stream(inputs.single(), graph, || trace_filter!(trace, trace_name))?;
        let valid_type_update_fields = params.type_update_fields.validate_on_stream_ext(
            inputs.single(),
            graph,
            |name, datum| spec.validate_type_update_field(name, datum, trace.clone()),
            || trace_filter!(trace, trace_name),
        )?;

        let mut new_variant = false;

        let mut streams = StreamsBuilder::new(&name, &inputs);
        streams
            .output_from_input(0, true, graph)
            .update(|output_stream, facts_proof| {
                {
                    let mut record_definition = output_stream.record_definition().borrow_mut();

                    for (f, t) in &valid_type_update_fields {
                        let datum_id = record_definition
                            .get_current_datum_definition_by_name(f.name())
                            .expect("datum")
                            .id();
                        record_definition.remove_datum(datum_id);
                        record_definition.add_dynamic_datum(f.name(), t.type_name());
                        new_variant = true;
                    }
                }

                Ok(spec.update_facts(
                    output_stream,
                    &valid_update_fields,
                    &valid_type_update_fields,
                    facts_proof,
                ))
            })?;

        let outputs = streams.build();

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
            let def = chain.stream_definition_fragments(self.outputs.single());
            let record = def.record();
            let unpacked_record = def.unpacked_record();

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

        let inline_body = quote! {
            input.filter_map(|#record_param| {
                #updates

                #new_variant_body

                Ok(Some(record))
            })
        };

        chain.implement_inline_node(
            self,
            self.inputs.single(),
            self.outputs.single(),
            &inline_body,
        );
    }

    fn all_nodes(&self) -> Box<dyn Iterator<Item = &dyn DynNode> + '_> {
        Box::new(Some(self as &dyn DynNode).into_iter())
    }
}
