use datapet_lang::ast::Graph;
use datapet_lang::ast::GraphDefinition;
use datapet_lang::ast::Module;
use datapet_lang::ast::ModuleItem;
use datapet_lang::ast::StreamLine;
use datapet_lang::ast::StreamLineInput;
use datapet_lang::ast::StreamLineOutput;
use datapet_lang::ast::UseDeclaration;
use datapet_lang::ast::UseTree;
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

fn use_tree_decl(use_tree: &UseTree) -> TokenStream {
    match use_tree {
        UseTree::Glob(path) => {
            let path: syn::Path = syn::parse_str(path).expect("path");
            quote! { #path :: * }
        }
        UseTree::Group(path, group) => {
            let path: syn::Path = syn::parse_str(path).expect("path");
            let group_decl = group.iter().map(use_tree_decl);
            quote! { #path :: { #(#group_decl,)* } }
        }
        UseTree::Path(path) => {
            let path: syn::Path = syn::parse_str(path).expect("path");
            quote! { #path }
        }
    }
}

fn dtpt_use_declaration(use_declaration: &UseDeclaration) -> TokenStream {
    let use_decl = use_tree_decl(&use_declaration.use_tree);
    quote! {
        use #use_decl;
    }
}

fn dtpt_graph_definition(graph_definition: &GraphDefinition) -> TokenStream {
    let name = format_ident!("{}", graph_definition.signature.name);
    let input_count = graph_definition
        .signature
        .inputs
        .as_ref()
        .map_or(0, |inputs| inputs.len() + 1);
    let output_count = graph_definition
        .signature
        .outputs
        .as_ref()
        .map_or(0, |outputs| outputs.len() + 1);
    let params = graph_definition.signature.params.iter().map(|param| {
        let name = format_ident!("{}", param);
        quote! { #name: &str }
    });
    let (body, all_var_names, main_stream, named_streams) =
        dtpt_stream_lines(&graph_definition.stream_lines);
    let outputs = graph_definition
        .signature
        .outputs
        .as_ref()
        .map_or_else(Vec::new, |outputs| {
            Some(main_stream.expect("main stream"))
                .into_iter()
                .chain(outputs.iter().map(|name| {
                    if let Some(stream) = named_streams.get(name) {
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

            #body

            let outputs = [#(#outputs.clone(),)*];

            NodeCluster::new(
                name,
                vec![#(Box::new(#all_var_names),)*],
                inputs,
                outputs,
            )
        }
    }
}

fn dtpt_graph(graph: &Graph, id: usize) -> TokenStream {
    let name = format_ident!("dtpt_main_{}", id);
    let (body, all_var_names, main_stream, _named_streams) = dtpt_stream_lines(&graph.stream_lines);
    assert!(main_stream.is_none());
    quote! {
        pub fn #name(
            graph: &mut GraphBuilder,
            name: FullyQualifiedName,
        ) -> NodeCluster<0, 0> {
            #body

            NodeCluster::new(
                name.sub(stringify!(#name)),
                vec![#(Box::new(#all_var_names),)*],
                [],
                [],
            )
        }
    }
}

fn dtpt_stream_lines(
    stream_lines: &[StreamLine],
) -> (
    TokenStream,
    Vec<Ident>,
    Option<TokenStream>,
    HashMap<String, TokenStream>,
) {
    let mut all_var_names = Vec::new();
    let mut main_stream = None;
    let mut named_streams = HashMap::<String, TokenStream>::new();
    let body = stream_lines
        .iter()
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
                .iter()
                .enumerate()
                .map(|(filter_index, filter)| {
                    let var_name = &var_names[filter_index];
                    let name: syn::Path = syn::parse_str(&filter.filter.name).expect("filter name");
                    let inputs = filter
                        .inputs
                        .iter()
                        .map(|input| match input {
                            StreamLineInput::Main => {
                                let inputs = if filter_index == 0 {
                                    quote! { inputs }
                                } else {
                                    let preceding_var_name = &var_names[filter_index - 1];
                                    quote! { #preceding_var_name.outputs() }
                                };
                                quote! { #inputs[0].clone() }
                            }
                            StreamLineInput::Named(name) => {
                                let input = if let Some(stream) = named_streams.get(name) {
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
                        .iter()
                        .map(|param| {
                            let expr: syn::Expr = syn::parse_str(param).expect("param expr");
                            quote! { #expr }
                        })
                        .collect::<Vec<TokenStream>>();
                    for (extra_output_index, extra_output) in
                        filter.filter.extra_outputs.iter().enumerate()
                    {
                        match named_streams.entry(extra_output.clone()) {
                            Entry::Vacant(entry) => {
                                let output_index = extra_output_index + 1;
                                entry.insert(quote! { #var_name.outputs()[#output_index]});
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
            if let Some(output) = flow_line.output.as_ref() {
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
                    StreamLineOutput::Named(output) => match named_streams.entry(output.clone()) {
                        Entry::Vacant(entry) => {
                            entry.insert(quote! { #last_var_name.outputs()[0]});
                        }
                        Entry::Occupied(entry) => {
                            panic!("stream `{}` already registered", entry.key());
                        }
                    },
                }
            }
            all_var_names.extend(var_names);
            quote! {
                #(#filters)*
            }
        })
        .collect::<Vec<TokenStream>>();
    (
        quote! { #(#body)* },
        all_var_names,
        main_stream,
        named_streams,
    )
}

fn dtpt_mod_internal(input: proc_macro::TokenStream, crate_: Ident) -> proc_macro::TokenStream {
    let fragment: Fragment = match syn::parse(input) {
        Ok(res) => res,
        Err(err) => {
            return err.into_compile_error().into();
        }
    };
    let mut graph_id = 0;
    let module = TokenStream::from_iter(fragment.module.items.iter().map(|item| match item {
        ModuleItem::UseDeclaration(use_declaration) => dtpt_use_declaration(use_declaration),
        ModuleItem::GraphDefinition(graph_definition) => dtpt_graph_definition(graph_definition),
        ModuleItem::Graph(graph) => {
            let id = graph_id;
            graph_id += 1;
            dtpt_graph(graph, id)
        }
    }));
    let exports = TokenStream::from_iter(
        fragment
            .module
            .items
            .iter()
            .filter_map(|item| match item {
                ModuleItem::UseDeclaration(_) => None,
                ModuleItem::GraphDefinition(graph_definition) => graph_definition
                    .visible
                    .then_some(format_ident!("{}", graph_definition.signature.name)),
                ModuleItem::Graph(_) => None,
            })
            .map(|name| quote! { pub use __dtpt_private::#name; }),
    );
    let main = (graph_id > 0).then(|| {
        let main_names = (0..graph_id)
            .map(|id| format_ident!("dtpt_main_{}", id))
            .collect::<Vec<_>>();
        quote! {
            fn dtpt_main(
                mut graph: GraphBuilder,
            ) -> Graph {
                let root = FullyQualifiedName::default();

                let entry_nodes: Vec<Box<dyn DynNode>> = vec![
                    #(Box::new(__dtpt_private::#main_names(&mut graph, root)),)*
                ];
                graph.build(entry_nodes)
            }
        }
    });
    quote! {
        mod __dtpt_private {
            use #crate_::prelude::*;

            #module
        }

        #exports

        #main
    }
    .into()
}

#[proc_macro]
pub fn dtpt_mod_crate(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    dtpt_mod_internal(input, format_ident!("crate"))
}

#[proc_macro]
pub fn dtpt_mod(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    dtpt_mod_internal(input, format_ident!("datapet"))
}
