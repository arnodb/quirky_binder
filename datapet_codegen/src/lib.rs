use annotate_snippets::{
    display_list::{DisplayList, FormatOptions},
    snippet::{Annotation, AnnotationType, Slice, Snippet, SourceAnnotation},
};
use datapet_lang::{
    ast::{
        ConnectedFilter, Graph, GraphDefinition, Module, ModuleItem, StreamLine, StreamLineInput,
        StreamLineOutput, UseDeclaration,
    },
    parser::module,
};
use itertools::Itertools;
use proc_macro2::{Ident, TokenStream};
use proc_macro_error::{abort_if_dirty, emit_error, proc_macro_error};
use quote::{format_ident, quote};
use std::{
    collections::{btree_map::Entry, BTreeMap, BTreeSet},
    iter::{Enumerate, Peekable},
};
use syn::{
    parse::{ParseStream, Parser},
    Error, LitStr,
};

struct ErrorEmitter<'a> {
    def_type: &'a Ident,
    input: &'a str,
}

impl<'a> ErrorEmitter<'a> {
    fn emit_error(&self, part: &str, error: &str) {
        emit_error!(
            self.def_type,
            "{}",
            DisplayList::from(snippet_for_input_and_part(self.input, part, error))
        );
    }
}

// Copied from https://github.com/botika/yarte
fn lines_offsets(s: &str) -> Vec<usize> {
    let mut lines = vec![0];
    let mut prev = 0;
    while let Some(len) = s[prev..].find('\n') {
        prev += len + 1;
        lines.push(prev);
    }
    lines
}

// Inspired by from https://github.com/botika/yarte
fn slice_spans<'a>(input: &'a str, part: &'a str) -> (usize, (usize, usize), (usize, usize)) {
    let (lo, hi) = {
        let a = input.as_ptr();
        let b = part.as_ptr();
        if a < b {
            (
                b as usize - a as usize,
                (b as usize - a as usize + part.len()).min(input.len()),
            )
        } else {
            panic!("not a part of input");
        }
    };

    let lines = lines_offsets(input);

    const CONTEXT: usize = 3;

    let lo_index = match lines.binary_search(&lo) {
        Ok(index) => index,
        Err(index) => index - 1,
    }
    .saturating_sub(CONTEXT);
    let lo_line = lines[lo_index];
    let hi_index = match lines.binary_search(&hi) {
        Ok(index) => index,
        Err(index) => index,
    };
    let hi_line = lines
        .get(hi_index + CONTEXT)
        .copied()
        .unwrap_or(input.len());
    (
        lo_index + 1,
        (lo_line, hi_line),
        (lo - lo_line, hi - lo_line),
    )
}

fn parse_def(input: ParseStream) -> Result<(Ident, LitStr, Ident), Error> {
    let def_type: Ident = input.parse()?;
    let datapet_crate = if def_type == "def" {
        format_ident!("datapet")
    } else if def_type == "datapet_def" {
        format_ident!("crate")
    } else {
        // Do not mention datapet_def which is for internal use.
        return Err(Error::new(def_type.span(), "expected 'def'"));
    };
    Ok((def_type, input.parse()?, datapet_crate))
}

fn snippet_for_input_and_part<'a>(input: &'a str, part: &'a str, label: &'a str) -> Snippet<'a> {
    let (line_start, (lo_line, hi_line), (lo, hi)) = slice_spans(input, part);
    let slice = Slice {
        source: &input[lo_line..hi_line],
        line_start,
        origin: None,
        annotations: vec![SourceAnnotation {
            label,
            range: (lo, hi),
            annotation_type: AnnotationType::Error,
        }],
        fold: false,
    };

    Snippet {
        title: Some(Annotation {
            id: None,
            label: None,
            annotation_type: AnnotationType::Error,
        }),
        footer: vec![],
        slices: vec![slice],
        opt: FormatOptions {
            // No color until https://github.com/rust-lang/rust-analyzer/issues/15443
            // has a proper fix.
            color: false,
            ..Default::default()
        },
    }
}

fn parse_module<'a>(
    def_type: &Ident,
    input_lit: &LitStr,
    input: &'a str,
) -> Result<Module<'a>, Error> {
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
            emit_error!(
                def_type,
                "{}",
                DisplayList::from(snippet_for_input_and_part(
                    input,
                    part,
                    match &err {
                        datapet_lang::nom::Err::Incomplete(_) => "Incomplete",
                        datapet_lang::nom::Err::Error(err)
                        | datapet_lang::nom::Err::Failure(err) => {
                            err.code.description()
                        }
                    }
                ))
            );
            return Err(Error::new(def_type.span(), "could not parse dtpt"));
        }
    };
    if !i.is_empty() {
        return Err(Error::new(
            input_lit.span(),
            format!("did not consume the entire input, {:?}", i),
        ));
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
    error_emitter: &ErrorEmitter,
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
    let params = graph_definition.signature.params.iter().map(|param| {
        let name = format_ident!("{}", param);
        quote! { #name: &str }
    });
    let (body, ordered_var_names, main_stream, mut named_streams) =
        dtpt_stream_lines(&graph_definition.stream_lines, error_emitter);
    let (main_stream, main_stream_anchor) = match main_stream {
        Some((a, b)) => (Some(a), Some(b)),
        None => (None, None),
    };
    let outputs = graph_definition.signature.outputs.as_ref().map_or_else(
        || {
            if let Some(anchor) = main_stream_anchor {
                error_emitter.emit_error(anchor, "main stream is not connected");
            }
            Vec::new()
        },
        |outputs| {
            if main_stream.is_none() {
                error_emitter.emit_error(&graph_definition.signature.name, "main stream not found");
            }
            let res = main_stream
                .into_iter()
                .chain(outputs.iter().filter_map(|name| {
                    match named_streams.try_connect_stream(name, error_emitter) {
                        Ok(Some(tokens)) => Some(tokens),
                        Ok(None) => {
                            error_emitter.emit_error(name, &format!("stream `{}` not found", name));
                            None
                        }
                        Err(()) => {
                            error_emitter.emit_error(
                                name,
                                &format!("stream `{}` is already connected", name),
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
        },
    );
    named_streams.check_all_streams_connected(error_emitter);

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

fn dtpt_graph(graph: &Graph, id: usize, error_emitter: &ErrorEmitter) -> TokenStream {
    let name = format_ident!("dtpt_main_{}", id);
    let (body, ordered_var_names, main_stream, named_streams) =
        dtpt_stream_lines(&graph.stream_lines, error_emitter);
    named_streams.check_all_streams_connected(error_emitter);
    if let Some((_, anchor)) = main_stream {
        error_emitter.emit_error(anchor, "main stream cannot be connected");
    }
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

struct NamedStreams<'a> {
    streams: BTreeMap<String, NamedStreamState<'a>>,
}

impl<'a> NamedStreams<'a> {
    fn new() -> Self {
        Self {
            streams: BTreeMap::new(),
        }
    }

    fn new_stream(&mut self, name: &'a str, tokens: TokenStream, error_emitter: &ErrorEmitter) {
        match self.streams.entry(name.to_owned()) {
            Entry::Vacant(vacant) => {
                vacant.insert(NamedStreamState::Dangling {
                    tokens,
                    anchor: name,
                });
            }
            Entry::Occupied(_) => {
                error_emitter.emit_error(name, &format!("stream `{}` already exists", name));
            }
        }
    }

    fn try_connect_stream(
        &mut self,
        name: &str,
        error_emitter: &ErrorEmitter,
    ) -> Result<Option<TokenStream>, ()> {
        if let Some(occupied) = self.streams.get_mut(name) {
            match occupied {
                NamedStreamState::Dangling { .. } => {
                    let tokens = occupied.connect();
                    Ok(Some(quote! { #tokens }))
                }
                NamedStreamState::Connected => {
                    error_emitter
                        .emit_error(name, &format!("stream `{}` is already connected", name));
                    Err(())
                }
            }
        } else {
            Ok(None)
        }
    }

    fn check_all_streams_connected(self, error_emitter: &ErrorEmitter) {
        for (name, state) in self.streams {
            match state {
                NamedStreamState::Dangling { anchor, .. } => {
                    error_emitter
                        .emit_error(anchor, &format!("stream `{}` is not connected", name));
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
    error_emitter: &ErrorEmitter,
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
                                            &format!("stream `{}` is already connected", name),
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
                        named_streams.new_stream(extra_output, tokens, error_emitter);
                        new_named_streams.insert(extra_output.clone().into_owned());
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
                                StreamLineOutput::Main(anchor) => match &mut main_stream {
                                    main_stream @ None => {
                                        *main_stream = Some((tokens, anchor));
                                    }
                                    Some(_) => {
                                        error_emitter
                                            .emit_error(anchor, "main stream already exists");
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

#[proc_macro]
#[proc_macro_error]
pub fn dtpt(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let (def_type, input_lit, datapet_crate) = match Parser::parse(parse_def, input) {
        Ok(res) => res,
        Err(err) => {
            abort_if_dirty();
            return err.into_compile_error().into();
        }
    };
    let input = input_lit.value();
    let module = match parse_module(&def_type, &input_lit, &input) {
        Ok(res) => res,
        Err(err) => {
            abort_if_dirty();
            return err.into_compile_error().into();
        }
    };
    let error_emitter = &ErrorEmitter {
        def_type: &def_type,
        input: &input,
    };
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
            use #datapet_crate::prelude::*;
            use truc::record::type_resolver::TypeResolver;

            #content
        }

        #exports

        #main
    })
    .into()
}
