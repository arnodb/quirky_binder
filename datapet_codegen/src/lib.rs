#[macro_use]
extern crate quote;
#[macro_use]
extern crate syn;

use proc_macro::TokenStream;
use syn::ext::IdentExt;
use syn::parse::Parse;
use syn::parse::ParseStream;
use syn::parse_macro_input;
use syn::Error;
use syn::FnArg;
use syn::Ident;
use syn::ItemStruct;
use syn::LitStr;
use syn::Type;

#[derive(Debug)]
struct Args {
    inputs: Vec<String>,
    outputs: Vec<(String, bool)>,
    init: Initialization,
    args: Vec<FnArg>,
    fields: Option<proc_macro2::TokenStream>,
}

#[derive(Debug)]
enum Initialization {
    None,
    Streams,
    GraphAndStreams,
}

impl Parse for Args {
    fn parse(input: ParseStream) -> Result<Self, Error> {
        let mut inputs = Vec::new();
        let mut outputs = Vec::new();
        let mut args = Vec::new();
        let mut fields = None;
        let mut init = Initialization::None;
        while !input.is_empty() {
            let lookahead = input.lookahead1();
            if input.peek(Ident::peek_any) {
                let ident = Ident::parse_any(input)?;
                if ident == "in" {
                    input.parse::<Token![=]>()?;
                    let lit = input.parse::<LitStr>()?;
                    inputs.push(lit.value());
                } else if ident == "out" {
                    input.parse::<Token![=]>()?;
                    let lit = input.parse::<LitStr>()?;
                    outputs.push((lit.value(), false));
                } else if ident == "out_mut" {
                    input.parse::<Token![=]>()?;
                    let lit = input.parse::<LitStr>()?;
                    outputs.push((lit.value(), true));
                } else if ident == "arg" {
                    input.parse::<Token![=]>()?;
                    let lit = input.parse::<LitStr>()?;
                    args.push(syn::parse_str(&lit.value()).unwrap());
                } else if ident == "fields" {
                    input.parse::<Token![=]>()?;
                    let lit = input.parse::<LitStr>()?;
                    fields = Some(syn::parse_str(&lit.value()).unwrap());
                } else if ident == "init" {
                    input.parse::<Token![=]>()?;
                    let lit = input.parse::<LitStr>()?;
                    match lit.value().as_str() {
                        "none" => init = Initialization::None,
                        "streams" => init = Initialization::Streams,
                        "graph_and_streams" => init = Initialization::GraphAndStreams,
                        _ => {
                            return Err(Error::new(ident.span(), "Unexpected"));
                        }
                    }
                } else {
                    return Err(lookahead.error());
                }
                if !input.is_empty() {
                    input.parse::<Token![,]>()?;
                }
            } else {
                return Err(lookahead.error());
            }
        }
        Ok(Self {
            inputs,
            outputs,
            init,
            args,
            fields,
        })
    }
}

#[proc_macro_attribute]
pub fn datapet_node(args: TokenStream, input: TokenStream) -> TokenStream {
    let parsed_args = parse_macro_input!(args as Args);
    let ast = parse_macro_input!(input as ItemStruct);
    let inputs_type =
        syn::parse_str::<Type>(&format!("[NodeStream; {}]", parsed_args.inputs.len())).unwrap();
    let struct_ident = &ast.ident;
    let args = &parsed_args.args;
    let input_count = parsed_args.inputs.len();
    let output_count = parsed_args.outputs.len();
    let mut new_streams = Vec::with_capacity(output_count);
    let mut init_args = Vec::with_capacity(input_count + output_count + parsed_args.args.len());
    let mut variant_ids = Vec::with_capacity(output_count);
    let mut outputs = Vec::with_capacity(output_count);
    for input_index in 0..input_count {
        init_args.push(quote! {
            (
                graph
                    .get_stream(inputs[#input_index].record_type())
                    .unwrap_or_else(|| panic!(r#"stream "{}""#, inputs[#input_index].record_type())),
                inputs[#input_index].variant_id(),
            )
        });
    }
    for (output, mutable) in &parsed_args.outputs {
        let output_ident = if output == "-" { "" } else { &output };
        let input_index = parsed_args.inputs.iter().position(|input| input == output);
        if let Some(input_index) = input_index {
            if *mutable {
                init_args.push(quote! {
                    graph
                        .get_stream(inputs[#input_index].record_type())
                        .unwrap_or_else(|| panic!(r#"stream "{}""#, inputs[#input_index].record_type()))
                });
                let variant_id = format_ident!("{}_variant_id", output_ident);
                variant_ids.push(variant_id.clone());
                outputs.push(quote! {
                    NodeStream::new(
                        inputs[#input_index].record_type().clone(),
                        #variant_id,
                        NodeStreamSource::from(name.clone()),
                    )
                });
            } else {
                outputs.push(quote! {
                    inputs[#input_index].with_source(NodeStreamSource::from(name.clone()))
                });
            }
        } else {
            let stream_name = format_ident!("{}_stream_name", output_ident);
            let record_type = format_ident!("{}_record_type", output_ident);
            new_streams.push(Some(quote! {
                let #stream_name = name.sub(#output);
                let #record_type = StreamRecordType::from(#stream_name.clone());
                graph.new_stream(#record_type.clone());
            }));
            init_args.push(quote! {
                graph
                    .get_stream(&#record_type)
                    .unwrap_or_else(|| panic!(r#"stream "{}""#, &#record_type))
            });
            let variant_id = format_ident!("{}_variant_id", output_ident);
            variant_ids.push(variant_id.clone());
            outputs.push(quote! {
                NodeStream::new(
                    #record_type,
                    #variant_id,
                    NodeStreamSource::from(#stream_name),
                )
            });
        }
    }
    for arg in &parsed_args.args {
        let name = match arg {
            FnArg::Receiver(_) => panic!("Forbidden `self` argument"),
            FnArg::Typed(pat_type) => &pat_type.pat,
        };
        init_args.push(quote! { &#name });
    }
    let init = match parsed_args.init {
        Initialization::None => None,
        Initialization::Streams => Some(quote! {
            let [#(#variant_ids),*] = Self::initialize_streams(#(#init_args,)*);
        }),
        Initialization::GraphAndStreams => Some(quote! {
            let arg_from_graph = Self::initialize_graph(graph);
            let [#(#variant_ids),*] = Self::initialize_streams(#(#init_args,)* arg_from_graph);
        }),
    };
    let fields = &parsed_args.fields;
    quote!(
        #ast

        impl #struct_ident {
            fn new(
                graph: &mut GraphBuilder,
                name: FullyQualifiedName,
                inputs: #inputs_type,
                #(#args),*
            ) -> Self {
                #(#new_streams)*
                #init
                let outputs = [#(#outputs),*];
                Self {
                    name,
                    inputs,
                    outputs,
                    #fields
                }
            }
        }

        impl Node<#input_count, #output_count> for #struct_ident {
            fn inputs(&self) -> &[NodeStream; #input_count] {
                &self.inputs
            }

            fn outputs(&self) -> &[NodeStream; #output_count] {
                &self.outputs
            }
        }
    )
    .into()
}
