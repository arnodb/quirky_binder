use crate::prelude::*;
use serde::Deserialize;
use truc::record::type_resolver::TypeResolver;

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct UnwrapParams<'a> {
    #[serde(borrow)]
    fields: FieldsParam<'a>,
    skip_nones: bool,
}

#[derive(Getters)]
pub struct Unwrap {
    name: FullyQualifiedName,
    #[getset(get = "pub")]
    inputs: [NodeStream; 1],
    #[getset(get = "pub")]
    outputs: [NodeStream; 1],
    fields: Vec<String>,
    skip_nones: bool,
}

impl Unwrap {
    fn new<R: TypeResolver + Copy>(
        graph: &mut GraphBuilder<R>,
        name: FullyQualifiedName,
        inputs: [NodeStream; 1],
        params: UnwrapParams,
    ) -> ChainResult<Self> {
        let mut streams = StreamsBuilder::new(&name, &inputs);

        streams
            .output_from_input(0, true, graph)
            .update(|output_stream, facts_proof| {
                let input_variant_id = output_stream.input_variant_id();
                let mut output_stream_def = output_stream.record_definition().borrow_mut();
                for &field in &**params.fields {
                    let datum = output_stream_def
                        .get_variant_datum_definition_by_name(input_variant_id, field)
                        .unwrap_or_else(|| panic!(r#"datum "{}""#, field));
                    let datum_id = datum.id();
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
                                panic!(
                                    "field `{}` is not an option: {}",
                                    field, optional_type_name
                                );
                            }
                        }
                    };
                    // TODO Make truc support Option<T> replacement with T where the T value is stored
                    // at the same offset so that records may remain identical, the enum discriminant
                    // becoming a void space.
                    output_stream_def.remove_datum(datum_id);
                    output_stream_def.add_dynamic_datum(field, type_name);
                }
                facts_proof.order_facts_updated().distinct_facts_updated()
            });

        let outputs = streams.build();

        Ok(Self {
            name,
            inputs,
            outputs,
            fields: params
                .fields
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            skip_nones: params.skip_nones,
        })
    }
}

impl DynNode for Unwrap {
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
        let def = chain.stream_definition_fragments(self.outputs.single());
        let record = def.record();
        let unpacked_record = def.unpacked_record();

        let record_definition = &graph.record_definitions()[self.inputs.single().record_type()];
        let variant = &record_definition[self.inputs.single().variant_id()];

        let error_type = graph.chain_customizer().error_type.to_name();

        let unwrap_fields = {
            let unwraps = variant
                .data()
                .map(|d| {
                    let datum = &record_definition[d];
                    let name_ident = format_ident!("{}", datum.name());
                    let error = if self.skip_nones {
                        quote! {
                            return Ok(None);
                        }
                    } else {
                        let name = datum.name();
                        quote! {
                            return Err(#error_type::custom(
                                format!("found None for field `{}`", #name)
                            ));
                        }
                    };
                    if self.fields.iter().any(|field| field == datum.name()) {
                        quote! {
                            #name_ident: match unpacked.#name_ident {
                                Some(value) => value,
                                None => {
                                    #error
                                }
                            }
                        }
                    } else {
                        quote! {
                            #name_ident: unpacked.#name_ident
                        }
                    }
                })
                .collect::<Vec<_>>();
            quote!(#(#unwraps),*)
        };

        let inline_body = quote! {
            input.filter_map(|rec| {
                let unpacked = rec.unpack();
                let unwrapped = #record::from(#unpacked_record { #unwrap_fields });
                Ok(Some(unwrapped))
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

pub fn unwrap<R: TypeResolver + Copy>(
    graph: &mut GraphBuilder<R>,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    params: UnwrapParams,
) -> ChainResult<Unwrap> {
    Unwrap::new(graph, name, inputs, params)
}
