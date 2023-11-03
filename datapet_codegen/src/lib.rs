use datapet_lang::{
    ast::{
        ConnectedFilter, Graph, GraphDefinition, Module, ModuleItem, StreamLine, StreamLineInput,
        StreamLineOutput, UseDeclaration,
    },
    parser::module,
};
use inflector::Inflector;
use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};
use std::{
    borrow::Cow,
    collections::{btree_map::Entry, BTreeMap, BTreeSet},
    iter::{Enumerate, Peekable},
};

pub trait ErrorEmitter {
    fn emit_error(&mut self, part: &str, error: Cow<str>);
}

#[allow(clippy::result_unit_err)]
pub fn parse_module<'a>(
    input: &'a str,
    error_emitter: &mut dyn ErrorEmitter,
) -> Result<Module<'a>, ()> {
    let res = module(input);
    let (i, module) = match res {
        Ok(res) => res,
        Err(err) => {
            let part = match &err {
                datapet_lang::nom::Err::Incomplete(_) => input,
                datapet_lang::nom::Err::Error(err) | datapet_lang::nom::Err::Failure(err) => {
                    err.input
                }
            };
            error_emitter.emit_error(
                part,
                match &err {
                    datapet_lang::nom::Err::Incomplete(_) => "Incomplete",
                    datapet_lang::nom::Err::Error(err) | datapet_lang::nom::Err::Failure(err) => {
                        err.code.description()
                    }
                }
                .into(),
            );
            return Err(());
        }
    };
    if !i.is_empty() {
        error_emitter.emit_error(
            i,
            format!("did not consume the entire input, {:?}", i).into(),
        );
        return Err(());
    }
    Ok(module)
}

fn dtpt_use_declaration(use_declaration: &UseDeclaration) -> TokenStream {
    let use_tree: syn::UseTree = syn::parse_str(&use_declaration.use_tree).expect("use expr");
    quote! {
        use #use_tree;
    }
}

fn dtpt_graph_definition(
    graph_definition: &GraphDefinition,
    error_emitter: &mut dyn ErrorEmitter,
) -> TokenStream {
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
    let params = graph_definition
        .signature
        .params
        .iter()
        .map(|param| format_ident!("{}", param))
        .collect::<Vec<Ident>>();
    let params_type_name =
        format_ident!("{}Params", graph_definition.signature.name.to_pascal_case());
    let setup_handlebars = {
        let hb_params = graph_definition.signature.params.iter().map(|param| {
            let name = format_ident!("{}", param);
            quote! { handlebars_data.insert(#param, #name); }
        });
        quote! {
            let handlebars = Handlebars::new();
            let mut handlebars_data = BTreeMap::<&str, &str>::new();
            #(#hb_params)*
        }
    };
    let (body, ordered_var_names, main_stream, mut named_streams) =
        dtpt_stream_lines(&graph_definition.stream_lines, error_emitter);
    let outputs = match graph_definition.signature.outputs.as_ref() {
        None => {
            if let Some(anchor) = main_stream.as_ref().map(|(_, anchor)| anchor) {
                error_emitter.emit_error(anchor, "main stream is not connected".into());
            }
            Vec::new()
        }
        Some(outputs) => {
            if main_stream.is_none() {
                error_emitter.emit_error(
                    &graph_definition.signature.name,
                    "main stream not found".into(),
                );
            }
            let res = main_stream
                .into_iter()
                .map(|(main_stream, _)| main_stream)
                .chain(outputs.iter().filter_map(|name| {
                    match named_streams.try_connect_stream(name, error_emitter) {
                        Ok(Some(tokens)) => Some(tokens),
                        Ok(None) => {
                            error_emitter
                                .emit_error(name, format!("stream `{}` not found", name).into());
                            None
                        }
                        Err(()) => {
                            error_emitter.emit_error(
                                name,
                                format!("stream `{}` is already connected", name).into(),
                            );
                            None
                        }
                    }
                }))
                .collect::<Vec<TokenStream>>();
            if res.len() != outputs.len() + 1 {
                // Some were not found
                Vec::new()
            } else {
                res
            }
        }
    };
    named_streams.check_all_streams_connected(error_emitter);

    quote! {
        #[derive(Deserialize, Debug)]
        #[serde(deny_unknown_fields)]
        pub struct #params_type_name<'a> {
            #(#params: &'a str,)*
        }

        pub fn #name<R: TypeResolver + Copy>(
            graph: &mut GraphBuilder<R>,
            name: FullyQualifiedName,
            inputs: [NodeStream; #input_count],
            #params_type_name { #(#params,)* }: #params_type_name,
        ) -> NodeCluster<#input_count, #output_count> {

            #setup_handlebars

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

fn dtpt_graph(graph: &Graph, id: usize, error_emitter: &mut dyn ErrorEmitter) -> TokenStream {
    let name = format_ident!("dtpt_main_{}", id);
    let (body, ordered_var_names, main_stream, named_streams) =
        dtpt_stream_lines(&graph.stream_lines, error_emitter);
    named_streams.check_all_streams_connected(error_emitter);
    if let Some((_, anchor)) = main_stream {
        error_emitter.emit_error(anchor, "main stream cannot be connected".into());
    }
    quote! {
        pub fn #name<R: TypeResolver + Copy>(
            graph: &mut GraphBuilder<R>,
            name: FullyQualifiedName,
        ) -> NodeCluster<0, 0> {
            let handlebars = Handlebars::new();
            let handlebars_data = BTreeMap::<&str, &str>::new();

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

struct NamedStreams<'a> {
    streams: BTreeMap<String, NamedStreamState<'a>>,
}

impl<'a> NamedStreams<'a> {
    fn new() -> Self {
        Self {
            streams: BTreeMap::new(),
        }
    }

    fn new_stream(
        &mut self,
        name: &'a str,
        tokens: TokenStream,
        error_emitter: &mut dyn ErrorEmitter,
    ) {
        match self.streams.entry(name.to_owned()) {
            Entry::Vacant(vacant) => {
                vacant.insert(NamedStreamState::Dangling {
                    tokens,
                    anchor: name,
                });
            }
            Entry::Occupied(_) => {
                error_emitter.emit_error(name, format!("stream `{}` already exists", name).into());
            }
        }
    }

    fn try_connect_stream(
        &mut self,
        name: &str,
        error_emitter: &mut dyn ErrorEmitter,
    ) -> Result<Option<TokenStream>, ()> {
        if let Some(occupied) = self.streams.get_mut(name) {
            match occupied {
                NamedStreamState::Dangling { .. } => {
                    let tokens = occupied.connect();
                    Ok(Some(quote! { #tokens }))
                }
                NamedStreamState::Connected => {
                    error_emitter.emit_error(
                        name,
                        format!("stream `{}` is already connected", name).into(),
                    );
                    Err(())
                }
            }
        } else {
            Ok(None)
        }
    }

    fn check_all_streams_connected(self, error_emitter: &mut dyn ErrorEmitter) {
        for (name, state) in self.streams {
            match state {
                NamedStreamState::Dangling { anchor, .. } => {
                    error_emitter
                        .emit_error(anchor, format!("stream `{}` is not connected", name).into());
                }
                NamedStreamState::Connected => {}
            }
        }
    }
}

#[derive(Debug)]
enum NamedStreamState<'a> {
    Dangling {
        tokens: TokenStream,
        anchor: &'a str,
    },
    Connected,
}

impl<'a> NamedStreamState<'a> {
    fn connect(&mut self) -> TokenStream {
        let mut swapped = NamedStreamState::Connected;
        std::mem::swap(self, &mut swapped);
        if let NamedStreamState::Dangling { tokens, .. } = swapped {
            tokens
        } else {
            unreachable!("Cannot connect a stream multiple times ");
        }
    }
}

fn dtpt_stream_lines<'a>(
    stream_lines: &'a [StreamLine<'a>],
    error_emitter: &mut dyn ErrorEmitter,
) -> (
    TokenStream,
    Vec<Ident>,
    Option<(TokenStream, &'a str)>,
    NamedStreams<'a>,
) {
    #[derive(PartialEq, Eq, Debug)]
    enum FlowLineState {
        Unstarted,
        Started,
        Done,
    }
    struct FlowLineIter<'a> {
        filter_iter: Peekable<Enumerate<std::slice::Iter<'a, ConnectedFilter<'a>>>>,
        output: Option<&'a StreamLineOutput<'a>>,
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
    let mut main_stream = None::<(TokenStream, &'a str)>;
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
                            StreamLineInput::Main(_) => {
                                if filter_index == 0 {
                                    Some(quote! { inputs[0].clone() })
                                } else {
                                    let preceding_var_name =
                                        &var_names[&(flow_line_index, filter_index - 1)];
                                    Some(quote! { #preceding_var_name.outputs()[0].clone() })
                                }
                            }
                            StreamLineInput::Named(name) => {
                                match named_streams.try_connect_stream(name, error_emitter) {
                                    Ok(Some(tokens)) => Some(tokens),
                                    Ok(None) => {
                                        flow_line_iter
                                            .missing_inputs
                                            .insert(name.clone().into_owned());
                                        None
                                    }
                                    Err(()) => {
                                        error_emitter.emit_error(
                                            name,
                                            format!("stream `{}` is already connected", name)
                                                .into(),
                                        );
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
                    let params = &filter.filter.params;
                    let ron_params = quote! {
                        let ron_params_str = handlebars.render_template(#params, &handlebars_data).expect("handlebars");
                    };
                    for (extra_output_index, extra_output) in
                        filter.filter.extra_outputs.iter().enumerate()
                    {
                        let output_index = extra_output_index + 1;
                        let tokens = quote! { #var_name.outputs()[#output_index].clone() };
                        named_streams.new_stream(extra_output, tokens, error_emitter);
                        new_named_streams.insert(extra_output.clone().into_owned());
                    }
                    body.push(quote! {
                        let #var_name = {
                            #ron_params
                            #name(
                                graph,
                                name.sub(stringify!(#var_name)),
                                [#(#inputs,)*],
                                graph.params().from_ron_str(&ron_params_str).expect("params"),
                            )
                        };
                    });
                    ordered_var_names.push(var_name.clone());
                    go_back_on_unstarted = true;
                    flow_line_iter.filter_iter.next();
                    if flow_line_iter.filter_iter.peek().is_none() {
                        if let Some(output) = &flow_line_iter.output {
                            let last_var_name = &var_names[&(flow_line_index, filter_index)];
                            let tokens = quote! { #last_var_name.outputs()[0].clone() };
                            match output {
                                StreamLineOutput::Main(anchor) => match &mut main_stream {
                                    main_stream @ None => {
                                        *main_stream = Some((tokens, anchor));
                                    }
                                    Some(_) => {
                                        error_emitter.emit_error(
                                            anchor,
                                            "main stream already exists".into(),
                                        );
                                    }
                                },
                                StreamLineOutput::Named(output) => {
                                    named_streams.new_stream(output, tokens, error_emitter);
                                    new_named_streams.insert(output.clone().into_owned());
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
            for iter in &mut flow_line_iters {
                if let Some(filter) = iter.filter_iter.peek() {
                    error_emitter.emit_error(&filter.1.filter.name, "Unresolved inputs".into());
                }
            }
            break;
        }
    }
    (
        quote! { #(#body)* },
        ordered_var_names,
        main_stream,
        named_streams,
    )
}

pub fn generate_module(
    module: &Module,
    datapet_crate: &Ident,
    error_emitter: &mut dyn ErrorEmitter,
) -> TokenStream {
    let mut graph_id = 0;
    let content = TokenStream::from_iter(module.items.iter().map(|item| match item {
        ModuleItem::UseDeclaration(use_declaration) => dtpt_use_declaration(use_declaration),
        ModuleItem::GraphDefinition(graph_definition) => {
            dtpt_graph_definition(graph_definition, error_emitter)
        }
        ModuleItem::Graph(graph) => {
            let id = graph_id;
            graph_id += 1;
            dtpt_graph(graph, id, error_emitter)
        }
    }));
    let exports = TokenStream::from_iter(
        module
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
                let handlebars = Handlebars::new();
                let handlebars_data = BTreeMap::<&str, &str>::new();

                let root = FullyQualifiedName::default();

                let entry_nodes: Vec<Box<dyn DynNode>> = vec![
                    #(Box::new(__dtpt_private::#main_names(&mut graph, root)),)*
                ];
                graph.build(entry_nodes)
            }
        }
    });
    quote! {
        use handlebars::Handlebars;
        use std::collections::BTreeMap;

        mod __dtpt_private {
            use #datapet_crate::prelude::*;
            use handlebars::Handlebars;
            use serde::Deserialize;
            use std::collections::BTreeMap;
            use truc::record::type_resolver::TypeResolver;

            #content
        }

        #exports

        #main
    }
}
