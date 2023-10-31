use datapet_lang::ast::ConnectedFilter;
use datapet_lang::ast::Graph;
use datapet_lang::ast::GraphDefinition;
use datapet_lang::ast::Module;
use datapet_lang::ast::ModuleItem;
use datapet_lang::ast::StreamLine;
use datapet_lang::ast::StreamLineInput;
use datapet_lang::ast::StreamLineOutput;
use datapet_lang::ast::UseDeclaration;
use datapet_lang::parser::module;
use itertools::Itertools;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;
use std::collections::hash_map::Entry;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::iter::{Enumerate, Peekable};
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

fn dtpt_use_declaration(use_declaration: &UseDeclaration) -> TokenStream {
    let use_tree: syn::UseTree = syn::parse_str(&use_declaration.use_tree).expect("use expr");
    quote! {
        use #use_tree;
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
    let (body, ordered_var_names, main_stream, mut named_streams) =
        dtpt_stream_lines(&graph_definition.stream_lines);
    let dangling_main_stream = main_stream.is_some();
    let outputs =
        graph_definition.signature.outputs.as_ref().map_or_else(
            || {
                if dangling_main_stream {
                    panic!("main stream is not connected");
                }
                Vec::new()
            },
            |outputs| {
                Some(main_stream.unwrap_or_else(|| {
                    if dangling_main_stream {
                        panic!("main stream is already connected");
                    } else {
                        panic!("main stream does not exist");
                    }
                }))
                .into_iter()
                .chain(outputs.iter().map(
                    |name| match named_streams.connect_stream(name.clone()) {
                        Ok(tokens) => tokens,
                        Err(name) => {
                            panic!("stream `{}` does not exist", name);
                        }
                    },
                ))
                .collect::<Vec<TokenStream>>()
            },
        );
    named_streams.check_all_streams_connected();
    quote! {
        pub fn #name<R: TypeResolver + Copy>(
            graph: &mut GraphBuilder<R>,
            name: FullyQualifiedName,
            inputs: [NodeStream; #input_count],
            #(#params,)*
        ) -> NodeCluster<#input_count, #output_count> {

            #body

            let outputs = [#(#outputs.clone(),)*];

            NodeCluster::new(
                name,
                vec![#(Box::new(#ordered_var_names),)*],
                inputs,
                outputs,
            )
        }
    }
}

fn dtpt_graph(graph: &Graph, id: usize) -> TokenStream {
    let name = format_ident!("dtpt_main_{}", id);
    let (body, ordered_var_names, main_stream, named_streams) =
        dtpt_stream_lines(&graph.stream_lines);
    named_streams.check_all_streams_connected();
    assert!(main_stream.is_none());
    quote! {
        pub fn #name<R: TypeResolver + Copy>(
            graph: &mut GraphBuilder<R>,
            name: FullyQualifiedName,
        ) -> NodeCluster<0, 0> {
            #body

            NodeCluster::new(
                name.sub(stringify!(#name)),
                vec![#(Box::new(#ordered_var_names),)*],
                [],
                [],
            )
        }
    }
}

struct NamedStreams {
    streams: HashMap<String, NamedStreamState>,
}

impl NamedStreams {
    fn new() -> Self {
        Self {
            streams: HashMap::new(),
        }
    }

    fn new_stream(&mut self, name: String, tokens: TokenStream) {
        match self.streams.entry(name) {
            Entry::Vacant(vacant) => {
                vacant.insert(NamedStreamState::Dangling(tokens));
            }
            Entry::Occupied(occupied) => {
                panic!("stream `{}` is duplicated", occupied.key());
            }
        }
    }

    fn connect_stream(&mut self, name: String) -> Result<TokenStream, String> {
        match self.streams.entry(name) {
            Entry::Vacant(vacant) => Err(vacant.into_key()),
            Entry::Occupied(mut occupied) => match occupied.get() {
                NamedStreamState::Dangling(_) => {
                    let tokens = occupied.get_mut().connect();
                    Ok(quote! { #tokens })
                }
                NamedStreamState::Connected => {
                    panic!("stream `{}` already connected", occupied.key());
                }
            },
        }
    }

    fn check_all_streams_connected(self) {
        for (name, state) in self.streams {
            match state {
                NamedStreamState::Dangling(_) => {
                    panic!("stream `{}` is not connected", name);
                }
                NamedStreamState::Connected => {}
            }
        }
    }
}

#[derive(Debug)]
enum NamedStreamState {
    Dangling(TokenStream),
    Connected,
}

impl NamedStreamState {
    fn connect(&mut self) -> TokenStream {
        let mut swapped = NamedStreamState::Connected;
        std::mem::swap(self, &mut swapped);
        if let NamedStreamState::Dangling(tokens) = swapped {
            tokens
        } else {
            unreachable!("Cannot connect a stream multiple times ");
        }
    }
}

fn dtpt_stream_lines(
    stream_lines: &[StreamLine],
) -> (TokenStream, Vec<Ident>, Option<TokenStream>, NamedStreams) {
    #[derive(PartialEq, Eq, Debug)]
    enum FlowLineState {
        Unstarted,
        Started,
        Done,
    }
    struct FlowLineIter<'a> {
        filter_iter: Peekable<Enumerate<std::slice::Iter<'a, ConnectedFilter>>>,
        output: Option<&'a StreamLineOutput>,
        state: FlowLineState,
        missing_inputs: BTreeSet<String>,
    }
    let mut named_streams = NamedStreams::new();
    let var_names = stream_lines
        .iter()
        .enumerate()
        .flat_map(|(flow_line_index, flow_line)| {
            flow_line
                .filters
                .iter()
                .enumerate()
                .map(move |(filter_index, filter)| {
                    if let Some(alias) = &filter.filter.alias {
                        ((flow_line_index, filter_index), format_ident!("{}", alias))
                    } else {
                        (
                            (flow_line_index, filter_index),
                            format_ident!("dtpt_filter_{}_{}", flow_line_index, filter_index),
                        )
                    }
                })
        })
        .collect::<BTreeMap<(usize, usize), Ident>>();
    let mut ordered_var_names = Vec::<Ident>::new();
    let mut flow_line_iters = stream_lines
        .iter()
        .map(|flow_line| FlowLineIter {
            filter_iter: flow_line.filters.iter().enumerate().peekable(),
            output: flow_line.output.as_ref(),
            state: FlowLineState::Unstarted,
            missing_inputs: BTreeSet::new(),
        })
        .collect::<Vec<FlowLineIter>>();
    let mut first_incomplete_line = 0;
    let mut body = Vec::<TokenStream>::new();
    let mut main_stream = None;
    loop {
        let mut blocked = true;
        let mut go_back_on_unstarted = false;
        let start_bound_of_this_loop = first_incomplete_line;
        for flow_line_index in start_bound_of_this_loop..flow_line_iters.len() {
            let flow_line_iter = &mut flow_line_iters[flow_line_index];
            match flow_line_iter.state {
                FlowLineState::Unstarted => {
                    if go_back_on_unstarted && first_incomplete_line != flow_line_index - 1 {
                        break;
                    }
                }
                FlowLineState::Started => {}
                FlowLineState::Done => {
                    if first_incomplete_line == flow_line_index {
                        first_incomplete_line += 1
                    }
                    continue;
                }
            }
            let mut new_named_streams = BTreeSet::<String>::new();
            if flow_line_iter.missing_inputs.is_empty() {
                blocked = false;
                if flow_line_iter.state == FlowLineState::Unstarted {
                    flow_line_iter.state = FlowLineState::Started;
                }
                while let Some((filter_index, filter)) = flow_line_iter.filter_iter.peek() {
                    let filter_index = *filter_index;
                    let var_name = &var_names[&(flow_line_index, filter_index)];
                    let name: syn::Path = syn::parse_str(&filter.filter.name).expect("filter name");
                    let inputs = filter
                        .inputs
                        .iter()
                        .filter_map(|input| match input {
                            StreamLineInput::Main => {
                                if filter_index == 0 {
                                    Some(quote! { inputs[0].clone() })
                                } else {
                                    let preceding_var_name =
                                        &var_names[&(flow_line_index, filter_index - 1)];
                                    Some(quote! { #preceding_var_name.outputs()[0].clone() })
                                }
                            }
                            StreamLineInput::Named(name) => {
                                match named_streams.connect_stream(name.clone()) {
                                    Ok(tokens) => Some(tokens),
                                    Err(name) => {
                                        flow_line_iter.missing_inputs.insert(name);
                                        None
                                    }
                                }
                            }
                        })
                        .collect::<Vec<TokenStream>>();
                    if !flow_line_iter.missing_inputs.is_empty() {
                        assert_ne!(inputs.len(), filter.inputs.len());
                        break;
                    }
                    assert_eq!(inputs.len(), filter.inputs.len());
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
                        let output_index = extra_output_index + 1;
                        let tokens = quote! { #var_name.outputs()[#output_index].clone() };
                        named_streams.new_stream(extra_output.clone(), tokens);
                        new_named_streams.insert(extra_output.clone());
                    }
                    body.push(quote! {
                        let #var_name = #name(
                            graph,
                            name.sub(stringify!(#var_name)),
                            [#(#inputs,)*],
                            #(#params,)*
                        );
                    });
                    ordered_var_names.push(var_name.clone());
                    go_back_on_unstarted = true;
                    flow_line_iter.filter_iter.next();
                    if flow_line_iter.filter_iter.peek().is_none() {
                        if let Some(output) = &flow_line_iter.output {
                            let last_var_name = &var_names[&(flow_line_index, filter_index)];
                            let tokens = quote! { #last_var_name.outputs()[0].clone() };
                            match output {
                                StreamLineOutput::Main => match &mut main_stream {
                                    main_stream @ None => {
                                        *main_stream = Some(tokens);
                                    }
                                    Some(_) => {
                                        panic!("main stream is duplicated");
                                    }
                                },
                                StreamLineOutput::Named(output) => {
                                    named_streams.new_stream(output.clone(), tokens);
                                    new_named_streams.insert(output.clone());
                                }
                            }
                        }

                        flow_line_iter.state = FlowLineState::Done;
                        flow_line_iter.missing_inputs.clear();
                        if first_incomplete_line == flow_line_index {
                            first_incomplete_line += 1;
                        }
                    }
                }
            }
            for flow_line_iter in &mut flow_line_iters[first_incomplete_line..] {
                match flow_line_iter.state {
                    FlowLineState::Started => {
                        for name in &new_named_streams {
                            flow_line_iter.missing_inputs.remove(name);
                        }
                    }
                    FlowLineState::Unstarted | FlowLineState::Done => {
                        break;
                    }
                }
            }
        }
        if first_incomplete_line >= flow_line_iters.len() {
            assert_eq!(first_incomplete_line, flow_line_iters.len());
            break;
        }
        if blocked {
            panic!(
                "blocked at {}",
                flow_line_iters
                    .iter_mut()
                    .filter_map(|iter| iter.filter_iter.peek())
                    .map(|filter| filter
                        .1
                        .filter
                        .alias
                        .as_ref()
                        .unwrap_or(&filter.1.filter.name))
                    .join(", ")
            )
        }
    }
    (
        quote! { #(#body)* },
        ordered_var_names,
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
                ModuleItem::GraphDefinition(graph_definition) => {
                    if graph_definition.visible {
                        Some(format_ident!("{}", graph_definition.signature.name))
                    } else {
                        None
                    }
                }
                ModuleItem::Graph(_) => None,
            })
            .map(|name| quote! { pub use __dtpt_private::#name; }),
    );
    let main = (graph_id > 0).then(|| {
        let main_names = (0..graph_id)
            .map(|id| format_ident!("dtpt_main_{}", id))
            .collect::<Vec<_>>();
        quote! {
            fn dtpt_main<R: TypeResolver + Copy>(
                mut graph: GraphBuilder<R>,
            ) -> Graph {
                let root = FullyQualifiedName::default();

                let entry_nodes: Vec<Box<dyn DynNode>> = vec![
                    #(Box::new(__dtpt_private::#main_names(&mut graph, root)),)*
                ];
                graph.build(entry_nodes)
            }
        }
    });
    (quote! {
        mod __dtpt_private {
            use #crate_::prelude::*;
            use truc::record::type_resolver::TypeResolver;

            #module
        }

        #exports

        #main
    })
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
