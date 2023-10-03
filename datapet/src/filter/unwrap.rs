use crate::{prelude::*, stream::UniqueNodeStream};
use truc::record::type_resolver::TypeResolver;

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
        fields: &[&str],
        skip_nones: bool,
    ) -> Self {
        let mut streams = StreamsBuilder::new(&name, &inputs);

        {
            let output_stream = streams.output_from_input(0, graph).for_update();
            let input_variant_id = output_stream.input_variant_id();
            let mut output_stream_def = output_stream.borrow_mut();
            for &field in fields {
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
                            panic!("field `{}` is not an option: {}", field, optional_type_name);
                        }
                    }
                };
                // TODO Make truc support Option<T> replacement with T where the T value is stored
                // at the same offset so that records may remain identical, the enum discriminant
                // becoming a void space.
                output_stream_def.remove_datum(datum_id);
                output_stream_def.add_dynamic_datum(field, type_name);
            }
        }

        let outputs = streams.build();

        Self {
            name,
            inputs,
            outputs,
            fields: fields.iter().map(ToString::to_string).collect::<Vec<_>>(),
            skip_nones,
        }
    }
}

impl DynNode for Unwrap {
    fn gen_chain(&self, graph: &Graph, chain: &mut Chain) {
        let thread = chain.get_thread_id_and_module_by_source(
            self.inputs.unique(),
            &self.name,
            self.outputs.some_unique(),
        );

        let def = chain.stream_definition_fragments(self.outputs.unique());

        let scope = chain.get_or_new_module_scope(
            self.name.iter().take(self.name.len() - 1),
            graph.chain_customizer(),
            thread.thread_id,
        );
        let mut import_scope = ImportScope::default();
        import_scope.add_import_with_error_type("fallible_iterator", "FallibleIterator");

        {
            let fn_name = format_ident!("{}", **self.name.last().expect("local name"));
            let thread_module = format_ident!("thread_{}", thread.thread_id);
            let error_type = graph.chain_customizer().error_type.to_name();

            let record_definition = &graph.record_definitions()[self.inputs.unique().record_type()];
            let variant = &record_definition[self.inputs.unique().variant_id()];

            let record = def.record();
            let unpacked_record = def.unpacked_record();

            let input = thread.format_input(
                self.inputs.unique().source(),
                graph.chain_customizer(),
                &mut import_scope,
            );

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

            let fn_def = quote! {
                pub fn #fn_name(#[allow(unused_mut)] mut thread_control: #thread_module::ThreadControl) -> impl FallibleIterator<Item = #record, Error = #error_type> {
                    #input
                    input.filter_map(|rec| {
                        let unpacked = rec.unpack();
                        let unwrapped = #record::from(#unpacked_record { #unwrap_fields });
                        Ok(Some(unwrapped))
                    })
                }
            };
            scope.raw(&fn_def.to_string());
        }

        import_scope.import(scope, graph.chain_customizer());
    }
}

pub fn unwrap<R: TypeResolver + Copy>(
    graph: &mut GraphBuilder<R>,
    name: FullyQualifiedName,
    inputs: [NodeStream; 1],
    fields: &[&str],
    skip_nones: bool,
) -> Unwrap {
    Unwrap::new(graph, name, inputs, fields, skip_nones)
}
