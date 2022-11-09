use datapet_lang::ast::FilterParam;
use datapet_lang::ast::Module;
use datapet_lang::ast::StreamLineInput;
use datapet_lang::ast::StreamLineOutput;
use datapet_lang::parser::module;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use syn::parse::Parse;
use syn::parse::ParseStream;
use syn::Error;
use syn::LitStr;

struct Fragment {
    module: Module,
}

impl Parse for Fragment {
    fn parse(input: ParseStream) -> Result<Self, Error> {
        let input_str: LitStr = input.parse()?;
        let i = input_str.value();
        let res = module(&i);
        let (i, module) = match res {
            Ok(res) => res,
            Err(err) => {
                return Err(Error::new(input_str.span(), err));
            }
        };
        if !i.is_empty() {
            return Err(Error::new(
                input_str.span(),
                format!("did not consume the entire input, {:?}", i),
            ));
        }
        Ok(Fragment { module })
    }
}

#[proc_macro]
pub fn dtpt_module(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let fragment: Fragment = match syn::parse(input) {
        Ok(res) => res,
        Err(err) => {
            return err.into_compile_error().into();
        }
    };
    TokenStream::from_iter(
        fragment
            .module
            .graph_definitions
            .into_iter()
            .map(|graph_definition| {
                let name = format_ident!("{}", graph_definition.signature.name);
                let input_count = graph_definition
                    .signature
                    .inputs
                    .map_or(0, |inputs| inputs.len() + 1);
                let output_count = graph_definition
                    .signature
                    .outputs
                    .as_ref()
                    .map_or(0, |outputs| outputs.len() + 1);
                let params = graph_definition.signature.params.into_iter().map(|param| {
                    let name = format_ident!("{}", param);
                    quote! { #name: &str }
                });
                let mut all_var_names = Vec::new();
                let mut main_stream = None;
                let mut named_streams = HashMap::<String, TokenStream>::new();
                let body = graph_definition
                    .stream_lines
                    .into_iter()
                    .map(|flow_line| {
                        let var_names = flow_line
                            .filters
                            .iter()
                            .enumerate()
                            .map(|(filter_index, filter)| {
                                if let Some(alias) = &filter.filter.alias {
                                    format_ident!("{}", alias)
                                } else {
                                    format_ident!("dtpt_filter_{}", filter_index)
                                }
                            })
                            .collect::<Vec<Ident>>();
                        let filters = flow_line
                            .filters
                            .into_iter()
                            .enumerate()
                            .map(|(filter_index, filter)| {
                                let var_name = &var_names[filter_index];
                                let name = format_ident!("{}", filter.filter.name);
                                let inputs = filter
                                    .inputs
                                    .into_iter()
                                    .map(|input| match input {
                                        StreamLineInput::Main => {
                                            let inputs = if filter_index == 0 {
                                                quote! { inputs }
                                            } else {
                                                let preceding_var_name =
                                                    &var_names[filter_index - 1];
                                                quote! { #preceding_var_name.outputs() }
                                            };
                                            quote! { #inputs[0].clone() }
                                        }
                                        StreamLineInput::Named(name) => {
                                            let input =
                                                if let Some(stream) = named_streams.get(&name) {
                                                    stream.clone()
                                                } else {
                                                    panic!("stream `{}` not registered", name);
                                                };
                                            quote! { #input.clone() }
                                        }
                                    })
                                    .collect::<Vec<TokenStream>>();
                                let params = filter
                                    .filter
                                    .params
                                    .into_iter()
                                    .map(|param| match param {
                                        FilterParam::Single(single_param) => {
                                            let single_param = format_ident!("{}", single_param);
                                            quote! { #single_param }
                                        }
                                        FilterParam::Array(array_param) => {
                                            let array_param_items = array_param
                                                .into_iter()
                                                .map(|item| format_ident!("{}", item));
                                            quote! { &[#(#array_param_items,)*] }
                                        }
                                    })
                                    .collect::<Vec<TokenStream>>();
                                for (extra_output_index, extra_output) in
                                    filter.filter.extra_outputs.into_iter().enumerate()
                                {
                                    match named_streams.entry(extra_output) {
                                        Entry::Vacant(entry) => {
                                            let output_index = extra_output_index + 1;
                                            entry.insert(
                                                quote! { #var_name.outputs()[#output_index]},
                                            );
                                        }
                                        Entry::Occupied(entry) => {
                                            panic!("stream `{}` already registered", entry.key());
                                        }
                                    }
                                }
                                quote! {
                                    let #var_name = #name(
                                        graph,
                                        name.sub(stringify!(#var_name)),
                                        [#(#inputs,)*],
                                        #(#params,)*
                                    );
                                }
                            })
                            .collect::<Vec<TokenStream>>();
                        if let Some(output) = flow_line.output {
                            let last_var_name = var_names.last().expect("last filter");
                            match output {
                                StreamLineOutput::Main => match &mut main_stream {
                                    main_stream @ None => {
                                        *main_stream = Some(quote! { #last_var_name.outputs()[0] });
                                    }
                                    Some(_) => {
                                        panic!("main stream already registered");
                                    }
                                },
                                StreamLineOutput::Named(output) => {
                                    match named_streams.entry(output) {
                                        Entry::Vacant(entry) => {
                                            entry.insert(quote! { #last_var_name.outputs()[0]});
                                        }
                                        Entry::Occupied(entry) => {
                                            panic!("stream `{}` already registered", entry.key());
                                        }
                                    }
                                }
                            }
                        }
                        all_var_names.extend(var_names);
                        quote! {
                            #(#filters)*
                        }
                    })
                    .collect::<Vec<TokenStream>>();
                let outputs = graph_definition
                    .signature
                    .outputs
                    .map_or_else(Vec::new, |outputs| {
                        Some(main_stream.expect("main stream"))
                            .into_iter()
                            .chain(outputs.into_iter().map(|name| {
                                if let Some(stream) = named_streams.get(&name) {
                                    stream.clone()
                                } else {
                                    panic!("stream `{}` not registered", name);
                                }
                            }))
                            .collect::<Vec<TokenStream>>()
                    });
                quote! {
                    pub fn #name(
                        graph: &mut GraphBuilder,
                        name: FullyQualifiedName,
                        inputs: [NodeStream; #input_count],
                        #(#params,)*
                        ) -> NodeCluster<#input_count, #output_count> {

                        #(#body)*

                        let outputs = [#(#outputs.clone(),)*];

                        NodeCluster::new(
                            name,
                            vec![#(Box::new(#all_var_names),)*],
                            inputs,
                            outputs,
                        )
                    }
                }
            }),
    )
    .into()
}
