use std::{
    collections::{btree_map::Entry, BTreeMap, BTreeSet},
    iter::{Enumerate, Peekable},
};

use inflector::Inflector;
use proc_macro2::{Ident, TokenStream};
use quirky_binder_lang::{
    ast::{
        ConnectedFilter, Graph, GraphDefinition, Module, ModuleItem, StreamLine, StreamLineInput,
        StreamLineOutput, UseDeclaration,
    },
    location::Location,
    parser::parse_module,
};
use quote::{format_ident, quote};

use crate::{CodegenError, QuirkyBinderErrorEmitter};

struct GraphNames {
    pub names: Vec<GraphName>,
    pub next_graph_id: usize,
}

struct GraphName {
    name: String,
    feature: Option<String>,
}

impl GraphNames {
    fn new() -> Self {
        Self {
            names: Vec::new(),
            next_graph_id: 0,
        }
    }

    fn add_name(&mut self, name: String, feature: Option<&str>) -> Result<(), String> {
        if self.names.iter().any(|n| n.name == name) {
            return Err(format!("Graph name {} already exists", name));
        }
        self.names.push(GraphName {
            name,
            feature: feature.map(ToString::to_string),
        });
        Ok(())
    }

    fn new_name(&mut self, feature: Option<&str>) -> String {
        loop {
            let name = format!("quirky_binder_main_{}", self.next_graph_id);
            self.next_graph_id += 1;
            if self.add_name(name.clone(), feature).is_ok() {
                return name;
            }
        }
    }
}

pub(crate) fn codegen_parse_module<'a>(
    input: &'a str,
    error_emitter: &mut QuirkyBinderErrorEmitter,
) -> Result<Module<'a>, CodegenError> {
    parse_module(input).map_err(|err| {
        let part = err.span;
        let error = err.kind.description();
        error_emitter.emit_error(part, error)
    })
}

fn quirky_binder_use_declaration(use_declaration: &UseDeclaration) -> TokenStream {
    let use_tree: syn::UseTree = syn::parse_str(use_declaration.use_tree).expect("use expr");
    quote! {
        use #use_tree;
    }
}

fn quirky_binder_graph_definition<'a>(
    graph_definition: &'a GraphDefinition,
    quirky_binder_crate: &Ident,
    error_emitter: &mut QuirkyBinderErrorEmitter<'a>,
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
    let (body, ordered_var_names, main_stream, mut named_streams) = quirky_binder_stream_lines(
        graph_definition.signature.name,
        &graph_definition.stream_lines,
        quirky_binder_crate,
        error_emitter,
    );
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
                    graph_definition.signature.name,
                    "main stream not found".into(),
                );
            }
            let res = main_stream
                .into_iter()
                .map(|(main_stream, _)| main_stream)
                .chain(outputs.iter().filter_map(
                    |name| match named_streams.try_connect_stream(name) {
                        Ok(tokens) => Some(tokens),
                        Err(ConnectError::NotFound) => {
                            error_emitter
                                .emit_error(name, format!("stream `{}` not found", name).into());
                            None
                        }
                        Err(ConnectError::AlreadyConnected) => {
                            error_emitter.emit_error(
                                name,
                                format!("stream `{}` is already connected", name).into(),
                            );
                            None
                        }
                    },
                ))
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
            #[serde(default)]
            _a: std::marker::PhantomData<&'a ()>,
        }

        pub fn #name<R: TypeResolver + Copy>(
            graph: &mut GraphBuilder<R>,
            name: FullyQualifiedName,
            inputs: [NodeStream; #input_count],
            #params_type_name { #(#params,)* _a }: #params_type_name,
        ) -> ChainResultWithTrace<NodeCluster<#input_count, #output_count>> {

            #setup_handlebars

            #body

            let outputs = [#(#outputs.clone(),)*];

            Ok(NodeCluster::new(
                name,
                vec![#(Box::new(#ordered_var_names),)*],
                inputs,
                outputs,
            ))
        }
    }
}

fn quirky_binder_graph<'a>(
    graph: &'a Graph<'a>,
    graph_names: &mut GraphNames,
    quirky_binder_crate: &Ident,
    error_emitter: &mut QuirkyBinderErrorEmitter<'a>,
) -> TokenStream {
    let Graph {
        annotations,
        stream_lines,
    } = graph;
    let name_str = if let Some(name) = annotations.name {
        match graph_names.add_name(name.to_owned(), annotations.feature) {
            Ok(()) => name.to_owned(),
            Err(err) => {
                error_emitter.emit_error(name, err.into());
                graph_names.new_name(annotations.feature)
            }
        }
    } else {
        graph_names.new_name(annotations.feature)
    };
    let name = format_ident!("{}", name_str);
    let (body, ordered_var_names, main_stream, named_streams) =
        quirky_binder_stream_lines(&name_str, stream_lines, quirky_binder_crate, error_emitter);
    named_streams.check_all_streams_connected(error_emitter);
    if let Some((_, anchor)) = main_stream {
        error_emitter.emit_error(anchor, "main stream cannot be connected".into());
    }
    let feature_gate = annotations
        .feature
        .map(|feature| quote![#[cfg(feature = #feature)]]);
    quote! {
        #feature_gate
        pub fn #name<R: TypeResolver + Copy>(
            graph: &mut GraphBuilder<R>,
            name: FullyQualifiedName,
        ) -> ChainResultWithTrace<NodeCluster<0, 0>> {
            let handlebars = Handlebars::new();
            let handlebars_data = BTreeMap::<&str, &str>::new();

            #body

            Ok(NodeCluster::new(
                name.sub(stringify!(#name)),
                vec![#(Box::new(#ordered_var_names),)*],
                [],
                [],
            ))
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
        error_emitter: &mut QuirkyBinderErrorEmitter,
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

    fn try_connect_stream(&mut self, name: &str) -> Result<TokenStream, ConnectError> {
        if let Some(occupied) = self.streams.get_mut(name) {
            occupied.connect().map_err(|err| {
                assert_eq!(ConnectError::AlreadyConnected, err);
                // Let the caller handle all errors because one treats NotFound in a special way.
                err
            })
        } else {
            Err(ConnectError::NotFound)
        }
    }

    fn check_all_streams_connected(self, error_emitter: &mut QuirkyBinderErrorEmitter) {
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

#[derive(PartialEq, Eq, Debug)]
enum ConnectError {
    NotFound,
    AlreadyConnected,
}

#[derive(Debug)]
enum NamedStreamState<'a> {
    Dangling {
        tokens: TokenStream,
        anchor: &'a str,
    },
    Connected,
}

impl NamedStreamState<'_> {
    fn connect(&mut self) -> Result<TokenStream, ConnectError> {
        let mut swapped = NamedStreamState::Connected;
        std::mem::swap(self, &mut swapped);
        if let NamedStreamState::Dangling { tokens, .. } = swapped {
            Ok(tokens)
        } else {
            Err(ConnectError::AlreadyConnected)
        }
    }
}

fn quirky_binder_stream_lines<'a>(
    caller: &str,
    stream_lines: &'a [StreamLine<'a>],
    quirky_binder_crate: &Ident,
    error_emitter: &mut QuirkyBinderErrorEmitter<'a>,
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
                            format_ident!(
                                "quirky_binder_filter_{}_{}",
                                flow_line_index,
                                filter_index
                            ),
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
                    let name: syn::Path = syn::parse_str(filter.filter.name).expect("filter name");
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
                                match named_streams.try_connect_stream(name) {
                                    Ok(tokens) => Some(tokens),
                                    Err(ConnectError::NotFound) => {
                                        flow_line_iter.missing_inputs.insert((*name).to_owned());
                                        None
                                    }
                                    Err(ConnectError::AlreadyConnected) => {
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
                        new_named_streams.insert((*extra_output).to_owned());
                    }
                    let source = error_emitter.source_file().map_or_else(
                        || quote! { std::file!() },
                        |source_file| quote! { #source_file },
                    );
                    let filter_location = {
                        let Location { line, col } =
                            error_emitter.part_to_location(filter.filter.name);
                        quote! { #quirky_binder_crate::chain::Location::new(#line, #col) }
                    };
                    body.push(quote! {
                        let #var_name = {
                            #ron_params
                            #name(
                                graph,
                                name.sub(stringify!(#var_name)),
                                [#(#inputs,)*],
                                graph.params().from_ron_str(&ron_params_str).expect("params"),
                            )
                        }
                            .with_trace_element(|| TraceElement::new(#source.into(), #caller.into(), #filter_location))?;
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
                                    new_named_streams.insert((*output).to_owned());
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
                    error_emitter.emit_error(filter.1.filter.name, "Unresolved inputs".into());
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

pub(crate) fn generate_module<'a>(
    module: &'a Module<'a>,
    quirky_binder_crate: &Ident,
    error_emitter: &mut QuirkyBinderErrorEmitter<'a>,
) -> Result<TokenStream, CodegenError> {
    let mut graph_names = GraphNames::new();
    let content = TokenStream::from_iter(module.items.iter().map(|item| match item {
        ModuleItem::UseDeclaration(use_declaration) => {
            quirky_binder_use_declaration(use_declaration)
        }
        ModuleItem::GraphDefinition(graph_definition) => {
            quirky_binder_graph_definition(graph_definition, quirky_binder_crate, error_emitter)
        }
        ModuleItem::Graph(graph) => {
            quirky_binder_graph(graph, &mut graph_names, quirky_binder_crate, error_emitter)
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
            .map(|name| quote! { pub use __quirky_binder_private::#name; }),
    );
    let main = (!graph_names.names.is_empty()).then(|| {
        let main_names = graph_names.names
            .iter()
            .map(|name|format_ident!("{}", name.name));
        let main_features = graph_names.names
            .iter()
            .map(|name| name.feature
                 .as_ref()
                 .map(|feature|quote![#[cfg(feature = #feature)]]));
        quote! {
            pub fn quirky_binder_main<R>(
                mut graph: GraphBuilder<R>,
            ) -> ChainResultWithTrace<Graph>
            where
                R: TypeResolver + Copy,
            {
                let handlebars = Handlebars::new();
                let handlebars_data = BTreeMap::<&str, &str>::new();

                let root = FullyQualifiedName::default();

                let entry_nodes: Vec<Box<dyn DynNode>> = vec![
                    #(
                        #main_features
                        Box::new(__quirky_binder_private::#main_names(&mut graph, root.sub(stringify!(#main_names)))?),
                    )*
                ];
                Ok(graph.build(entry_nodes))
            }

            pub fn quirky_binder_generate<R, NGB>(
                out_dir: &Path,
                new_graph_builder: NGB,
            ) -> Result<(), GraphGenerationError>
            where
                R: TypeResolver + Copy,
                NGB: Fn() -> GraphBuilder<R>,
            {
                let graph = quirky_binder_main(new_graph_builder())?;

                graph.generate(out_dir)?;

                Ok(())
            }
        }
    });
    error_emitter.error()?;
    Ok(quote! {
        use handlebars::Handlebars;
        use std::collections::BTreeMap;

        mod __quirky_binder_private {
            use #quirky_binder_crate::prelude::*;
            use handlebars::Handlebars;
            use serde::Deserialize;
            use std::collections::BTreeMap;
            use truc::record::type_resolver::TypeResolver;

            #content
        }

        #exports

        #main
    })
}
