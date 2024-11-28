use std::{borrow::Cow, collections::HashMap, fmt::Display, ops::Deref};

use codegen::{Module, Scope};
use itertools::Itertools;
use proc_macro2::TokenStream;
use quirky_binder_lang::location::Location;
use serde::Deserialize;

use self::error::{ChainError, ChainErrorWithTrace};
use crate::prelude::*;

pub mod error;

pub type ChainResultWithTrace<T> = Result<T, ChainErrorWithTrace>;

pub type ChainResult<T> = Result<T, ChainError>;

pub trait WithTraceElement {
    type WithTraceType;

    fn with_trace_element<Element>(self, element: Element) -> Self::WithTraceType
    where
        Element: Fn() -> TraceElement<'static>;
}

impl<T> WithTraceElement for ChainResult<T> {
    type WithTraceType = ChainResultWithTrace<T>;

    fn with_trace_element<Element>(self, element: Element) -> Self::WithTraceType
    where
        Element: Fn() -> TraceElement<'static>,
    {
        self.map_err(|err| err.with_trace_element(element))
    }
}

impl<T> WithTraceElement for ChainResultWithTrace<T> {
    type WithTraceType = ChainResultWithTrace<T>;

    fn with_trace_element<Element>(self, element: Element) -> Self::WithTraceType
    where
        Element: Fn() -> TraceElement<'static>,
    {
        self.map_err(|err| err.with_trace_element(element))
    }
}

#[derive(Debug)]
struct ChainThread {
    id: usize,
    name: FullyQualifiedName,
    thread_type: ChainThreadType,
    main: Option<FullyQualifiedName>,
    input_streams: Box<[NodeStream]>,
    output_streams: Box<[NodeStream]>,
    input_pipes: Option<Box<[usize]>>,
    output_pipes: Option<Box<[usize]>>,
}

#[derive(Clone, Copy, PartialEq, Eq, Default, Deserialize, Debug)]
pub enum ChainThreadType {
    #[default]
    Regular,
    Background,
}

#[derive(Clone)]
pub struct ChainSourceThread {
    pub thread_id: usize,
    pub stream_index: usize,
    pub piped: bool,
}

impl ChainSourceThread {
    pub fn format_input(
        &self,
        source_name: &NodeStreamSource,
        customizer: &ChainCustomizer,
        mutable: bool,
    ) -> TokenStream {
        let mutable_input = mutable.then(|| quote! {mut});
        if !self.piped {
            let input = syn::parse_str::<syn::Path>(&format!(
                "{}::{}",
                customizer.module_name, source_name
            ))
            .expect("chain_module");
            quote! {
                let #mutable_input input = #input(thread_control);
            }
        } else {
            let input = format_ident!("input_{}", self.stream_index);
            let error_type = customizer.error_type.to_name();
            quote! {
                let #mutable_input input = {
                    let rx = thread_control.#input.take().expect("input {stream_index}");
                    quirky_binder_support::iterator::sync::mpsc::Receive::<_, #error_type>::new(rx)
                };
            }
        }
    }
}

#[derive(new)]
pub struct Chain<'a> {
    customizer: &'a ChainCustomizer,
    scope: &'a mut Scope,
    #[new(default)]
    threads: Vec<ChainThread>,
    #[new(default)]
    thread_by_source: HashMap<NodeStreamSource, ChainSourceThread>,
    #[new(default)]
    pipe_count: usize,
}

impl<'a> Chain<'a> {
    pub fn stream_definition_fragments(
        &self,
        stream: &'a NodeStream,
    ) -> RecordDefinitionFragments<'a> {
        stream.definition_fragments(&self.customizer.streams_module_name)
    }

    pub fn sub_stream_definition_fragments(
        &self,
        stream: &'a NodeSubStream,
    ) -> RecordDefinitionFragments<'a> {
        stream.definition_fragments(&self.customizer.streams_module_name)
    }

    #[allow(clippy::too_many_arguments)]
    fn new_thread(
        &mut self,
        name: FullyQualifiedName,
        thread_type: ChainThreadType,
        input_streams: Box<[NodeStream]>,
        output_streams: Box<[NodeStream]>,
        input_pipes: Option<Box<[usize]>>,
        supersede_output_sources: bool,
        main: Option<FullyQualifiedName>,
    ) -> usize {
        for output_stream in &*output_streams {
            if let Some(ChainSourceThread { thread_id, .. }) =
                self.thread_by_source.get(output_stream.source())
            {
                if !supersede_output_sources {
                    let thread = &self.threads[*thread_id];
                    panic!(
                        r#"Thread "{}" is already the source of "{}""#,
                        thread.name,
                        output_stream.source()
                    );
                }
            } else if supersede_output_sources {
                panic!(
                    r#"Cannot find the source of "{}" in order to supersede it"#,
                    output_stream.source()
                );
            }
        }
        let thread_id = self.threads.len();
        for (i, output_stream) in output_streams.iter().enumerate() {
            self.thread_by_source.insert(
                output_stream.source().clone(),
                ChainSourceThread {
                    thread_id,
                    stream_index: i,
                    piped: false,
                },
            );
        }
        let output_pipes = if main.is_some() {
            Some(
                output_streams
                    .iter()
                    .map(|_| self.new_pipe())
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            )
        } else {
            None
        };
        self.threads.push(ChainThread {
            id: thread_id,
            name,
            thread_type,
            main,
            input_streams,
            output_streams,
            input_pipes,
            output_pipes,
        });
        let name = format!("thread_{}", thread_id);
        let module = self.scope.new_module(&name).vis("pub").scope();
        for (path, ty) in &self.customizer.custom_module_imports {
            module.import(path, ty);
        }
        thread_id
    }

    pub fn new_threaded_source(
        &mut self,
        name: &FullyQualifiedName,
        thread_type: ChainThreadType,
        inputs: &[NodeStream; 0],
        outputs: &[NodeStream],
    ) -> usize {
        self.new_thread(
            name.clone(),
            thread_type,
            inputs.to_vec().into_boxed_slice(),
            outputs.to_vec().into_boxed_slice(),
            None,
            false,
            Some(name.clone()),
        )
    }

    fn get_source_thread(&self, source: &NodeStreamSource) -> &ChainSourceThread {
        self.thread_by_source.get(source).unwrap_or_else(|| {
            panic!(
                r#"Thread for source "{}" not found, available sources are [{}]"#,
                source,
                self.thread_by_source.keys().join(", ")
            )
        })
    }

    pub fn get_thread_by_source(
        &mut self,
        input: &NodeStream,
        new_thread_name: &FullyQualifiedName,
        output: Option<&NodeStream>,
    ) -> ChainSourceThread {
        let source_thread = self.get_source_thread(input.source());
        let thread_id = source_thread.thread_id;
        let thread = &self.threads[thread_id];
        if let Some(output_pipes) = &thread.output_pipes {
            let input_pipe = output_pipes[source_thread.stream_index];
            let streams = Box::new([thread.output_streams[source_thread.stream_index].clone()]);
            let thread_id = self.new_thread(
                new_thread_name.clone(),
                ChainThreadType::Regular,
                streams.clone(),
                streams,
                Some(Box::new([input_pipe])),
                true,
                None,
            );
            if let Some(output) = output {
                self.update_thread_single_stream(thread_id, output);
            }
            ChainSourceThread {
                thread_id,
                stream_index: 0,
                piped: true,
            }
        } else {
            let thread = source_thread.clone();
            if let Some(output) = output {
                self.update_thread_single_stream(thread.thread_id, output);
            }
            thread
        }
    }

    fn update_thread_single_stream(&mut self, thread_id: usize, stream: &NodeStream) {
        let thread = self.threads.get_mut(thread_id).expect("thread");
        assert_eq!(thread.output_streams.len(), 1);
        self.thread_by_source
            .remove(thread.output_streams[0].source());
        thread.output_streams[0] = stream.clone();
        self.thread_by_source.insert(
            stream.source().clone(),
            ChainSourceThread {
                thread_id,
                stream_index: 0,
                piped: false,
            },
        );
    }

    fn new_pipe(&mut self) -> usize {
        let pipe = self.pipe_count;
        self.pipe_count = pipe + 1;
        pipe
    }

    fn pipe_single_thread(&mut self, source: &NodeStreamSource) -> usize {
        let source_thread = self.get_source_thread(source).clone();
        let thread = &mut self.threads[source_thread.thread_id];
        if let Some(output_pipes) = &thread.output_pipes {
            return output_pipes[source_thread.stream_index];
        }
        let pipe = self.new_pipe();
        let thread = &mut self.threads[source_thread.thread_id];
        let name = format!("thread_{}", source_thread.thread_id);
        let scope = self
            .scope
            .get_module_mut(&name)
            .expect("thread module")
            .scope();
        let mut import_scope = ImportScope::default();
        if thread.output_pipes.is_none() {
            assert_eq!(thread.output_streams.len(), 1);
            import_scope.add_import("fallible_iterator", "FallibleIterator");

            {
                let error_type = self.customizer.error_type.to_name();

                let input = source_thread.format_input(source, self.customizer, true);

                let pipe_def = quote! {
                    pub fn quirky_binder_pipe(mut thread_control: ThreadControl) -> impl FnOnce() -> Result<(), #error_type> {
                        move || {
                            let tx = thread_control.output_0.take().expect("output 0");
                            #input
                            while let Some(record) = input.next()? {
                                tx.send(Some(record))?;
                            }
                            tx.send(None)?;
                            Ok(())
                        }
                    }
                };
                scope.raw(&pipe_def.to_string());
            }

            thread.output_pipes = Some(Box::new([pipe]));
            thread.main = Some(FullyQualifiedName::new(name).sub("quirky_binder_pipe"));
        }
        import_scope.import(scope, self.customizer);
        pipe
    }

    pub fn pipe_inputs(
        &mut self,
        name: &FullyQualifiedName,
        inputs: &[NodeStream],
        outputs: &[NodeStream],
    ) -> usize {
        let input_pipes = inputs
            .iter()
            .map(|input| self.pipe_single_thread(input.source()))
            .collect::<Vec<usize>>();
        self.new_thread(
            name.clone(),
            ChainThreadType::Regular,
            inputs.to_vec().into_boxed_slice(),
            outputs.to_vec().into_boxed_slice(),
            Some(input_pipes.into_boxed_slice()),
            false,
            Some(name.clone()),
        )
    }

    pub fn set_thread_main(&mut self, thread_id: usize, main: FullyQualifiedName) {
        self.threads[thread_id].main = Some(main);
    }

    pub fn gen_chain(&mut self) {
        for thread in &self.threads {
            let name = format!("thread_{}", thread.id);
            let scope = self
                .scope
                .get_module_mut(&name)
                .expect("thread module")
                .scope();
            scope.import("std::sync", "Arc");
            scope.import(
                "quirky_binder_support::chain::configuration",
                "ChainConfiguration",
            );
            if thread.input_streams.len() > 0 {
                scope.import("std::sync::mpsc", "Receiver");
            }
            if thread.output_pipes.is_some() && thread.output_streams.len() > 0 {
                scope.import("std::sync::mpsc", "SyncSender");
            }
            let inputs = (0..thread.input_streams.len()).map(|i| format_ident!("input_{}", i));
            let input_types = thread.input_streams.iter().map(|input_stream| {
                let def = input_stream.definition_fragments(&self.customizer.streams_module_name);
                def.record()
            });
            let outputs = if thread.output_pipes.is_some() {
                Some((0..thread.output_streams.len()).map(|i| format_ident!("output_{}", i)))
            } else {
                None
            }
            .into_iter()
            .flatten();
            let output_types = if thread.output_pipes.is_some() {
                Some(thread.output_streams.iter().map(|output_stream| {
                    let def =
                        output_stream.definition_fragments(&self.customizer.streams_module_name);
                    def.record()
                }))
            } else {
                None
            }
            .into_iter()
            .flatten();
            let interrupt = match thread.thread_type {
                ChainThreadType::Regular => None,
                ChainThreadType::Background => Some(quote! {
                    pub interrupt: std::sync::Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
                }),
            };
            let struct_def = quote! {

                pub struct ThreadOuterControl {
                    #interrupt
                }

                pub struct ThreadControl {
                    pub chain_configuration: Arc<ChainConfiguration>,
                    #interrupt
                    #(pub #inputs: Option<Receiver<Option<#input_types>>>,)*
                    #(pub #outputs: Option<SyncSender<Option<#output_types>>>,)*
                }

            };
            scope.raw(&struct_def.to_string());
        }

        {
            let error_type = self.customizer.error_type.to_name();

            let channels = (0..self.pipe_count).map(|pipe| {
                let tx = format_ident!("tx_{}", pipe);
                let rx = format_ident!("rx_{}", pipe);
                quote! {
                    let (#tx, #rx) = std::sync::mpsc::sync_channel(42);
                }
            });

            let thread_controls = self
                .threads
                .iter()
                .enumerate()
                .map(|(thread_index, thread)| {
                    let thread_outer_control = format_ident!(
                        "{}thread_outer_control_{}",
                        match thread.thread_type {
                            ChainThreadType::Regular => "_",
                            ChainThreadType::Background => "",
                        },
                        thread.id
                    );
                    let thread_control = format_ident!("thread_control_{}", thread.id);
                    let thread_module = format_ident!("thread_{}", thread.id);
                    let interrupt = match thread.thread_type {
                        ChainThreadType::Regular => None,
                        ChainThreadType::Background => Some(quote! {
                            interrupt: std::sync::Arc::new((
                                std::sync::Mutex::new(false),
                                std::sync::Condvar::new(),
                            )),
                        }),
                    };
                    let interrupt_clone = interrupt.is_some().then(|| {
                        quote! {
                            interrupt: #thread_outer_control.interrupt.clone(),
                        }
                    });
                    let inputs = thread
                        .input_pipes
                        .as_ref()
                        .map(|input_pipes| {
                            input_pipes.iter().enumerate().map(|(index, pipe)| {
                                let input = format_ident!("input_{}", index);
                                let rx = format_ident!("rx_{}", pipe);
                                quote! { #input: Some(#rx), }
                            })
                        })
                        .into_iter()
                        .flatten();
                    let outputs = thread
                        .output_pipes
                        .as_ref()
                        .map(|output_pipes| {
                            output_pipes.iter().enumerate().map(|(index, pipe)| {
                                let output = format_ident!("output_{}", index);
                                let tx = format_ident!("tx_{}", pipe);
                                quote! { #output: Some(#tx), }
                            })
                        })
                        .into_iter()
                        .flatten();
                    let config_assignment = if thread_index + 1 < self.threads.len() {
                        quote! {
                            chain_configuration: chain_configuration.clone(),
                        }
                    } else {
                        quote! {
                            chain_configuration,
                        }
                    };
                    quote! {
                        let #thread_outer_control = #thread_module::ThreadOuterControl {
                            #interrupt
                        };
                        let #thread_control = #thread_module::ThreadControl {
                            #config_assignment
                            #interrupt_clone
                            #(#inputs)*
                            #(#outputs)*
                        };
                    }
                });

            let spawn_threads = self.threads.iter().map(|thread| {
                let join_thread = format_ident!("join_{}", thread.id);
                let thread_main =
                    syn::parse_str::<syn::Expr>(&thread.main.as_ref().expect("main").to_string())
                        .expect("thread_main");
                let thread_control = format_ident!("thread_control_{}", thread.id);
                quote! {
                    let #join_thread = std::thread::spawn(#thread_main(#thread_control));
                }
            });

            let join_regular_threads = self
                .threads
                .iter()
                .filter(|thread| thread.thread_type == ChainThreadType::Regular)
                .map(|thread| {
                    let join_thread = format_ident!("join_{}", thread.id);
                    quote! {
                        #join_thread.join().unwrap()?;
                    }
                });

            let interrupt_background_threads = self
                .threads
                .iter()
                .filter(|thread| thread.thread_type == ChainThreadType::Background)
                .map(|thread| {
                    let thread_outer_control = format_ident!("thread_outer_control_{}", thread.id);
                    quote! {{
                        let mut is_interrupted = #thread_outer_control.interrupt.0.lock().unwrap();
                        *is_interrupted = true;
                        #thread_outer_control.interrupt.1.notify_all();
                    }}
                });

            let join_background_threads = self
                .threads
                .iter()
                .filter(|thread| thread.thread_type == ChainThreadType::Background)
                .map(|thread| {
                    let join_thread = format_ident!("join_{}", thread.id);
                    quote! {
                        #join_thread.join().unwrap()?;
                    }
                });

            self.scope.import("std::sync", "Arc");
            self.scope.import(
                "quirky_binder_support::chain::configuration",
                "ChainConfiguration",
            );

            let main_attrs = if !self.customizer.main_attrs.is_empty() {
                let attrs = &self
                    .customizer
                    .main_attrs
                    .iter()
                    .map(|attr| format_ident!("{}", attr))
                    .collect::<Vec<_>>();
                quote! {#[#(#attrs),*]}
            } else {
                quote! {}
            };
            let main_name = format_ident!("{}", &self.customizer.main_name);
            let main_def = quote! {
                #main_attrs
                pub fn #main_name(chain_configuration: ChainConfiguration) -> Result<(), #error_type> {
                    #[allow(unused_variables)]
                    let chain_configuration = Arc::new(chain_configuration);

                    #(#channels)*

                    #(#thread_controls)*

                    #(#spawn_threads)*

                    #(#join_regular_threads)*

                    #(#interrupt_background_threads)*

                    #(#join_background_threads)*

                    Ok(())
                }
            };
            self.scope.raw(&main_def.to_string());
        }
    }

    fn get_or_new_module_scope<'i>(
        &mut self,
        path: impl IntoIterator<Item = &'i Box<str>>,
        chain_customizer: &ChainCustomizer,
        thread_id: usize,
    ) -> &mut Scope {
        let mut iter = path.into_iter();
        let customize_module = |module: &mut Module| {
            for (path, ty) in &chain_customizer.custom_module_imports {
                module.scope().import(path, ty);
            }
            let thread_module = format!("thread_{}", thread_id);
            module.scope().import("super", &thread_module).vis("pub");
        };
        if let Some(first) = iter.next() {
            let module = self.scope.get_or_new_module(first);
            (customize_module)(module);
            iter.fold(module, |m, n| {
                let module = m.get_or_new_module(n).vis("pub");
                (customize_module)(module);
                module
            })
            .scope()
        } else {
            self.scope
        }
    }

    pub fn implement_inline_node(
        &mut self,
        node: &dyn DynNode,
        input: &NodeStream,
        output: &NodeStream,
        inline_body: &TokenStream,
    ) {
        let name = node.name();

        let thread = self.get_thread_by_source(input, name, Some(output));

        let record = self.stream_definition_fragments(output).record();

        let fn_name = format_ident!("{}", **name.last().expect("local name"));
        let thread_module = format_ident!("thread_{}", thread.thread_id);
        let error_type = self.customizer.error_type.to_name();

        let input = thread.format_input(input.source(), self.customizer, false);

        let fn_def = quote! {
              pub fn #fn_name(#[allow(unused_mut)] mut thread_control: #thread_module::ThreadControl) -> impl FallibleIterator<Item = #record, Error = #error_type> {
                  #input
                  #inline_body
              }
        };

        let customizer = self.customizer;

        let mut import_scope = ImportScope::default();
        import_scope.add_import("fallible_iterator", "FallibleIterator");

        let scope = self.get_or_new_module_scope(
            name.iter().take(name.len() - 1),
            self.customizer,
            thread.thread_id,
        );

        scope.raw(&fn_def.to_string());

        import_scope.import(scope, customizer);
    }

    pub fn implement_node_thread(
        &mut self,
        node: &dyn DynNode,
        thread_id: usize,
        thread_body: &TokenStream,
    ) {
        let name = node.name();

        let fn_name = format_ident!("{}", **name.last().expect("local name"));
        let thread_module = format_ident!("thread_{}", thread_id);
        let error_type = self.customizer.error_type.to_name();

        let fn_def = quote! {
            pub fn #fn_name(#[allow(unused_mut)] mut thread_control: #thread_module::ThreadControl) -> impl FnOnce() -> Result<(), #error_type> {
                #thread_body
            }
        };

        let customizer = self.customizer;

        let mut import_scope = ImportScope::default();
        if !node.inputs().is_empty() {
            import_scope.add_import("fallible_iterator", "FallibleIterator");
        }

        let scope = self.get_or_new_module_scope(
            name.iter().take(name.len() - 1),
            self.customizer,
            thread_id,
        );

        scope.raw(&fn_def.to_string());

        import_scope.import(scope, customizer);
    }

    pub fn implement_path_update(
        &mut self,
        node: &dyn DynNode,
        input: &NodeStream,
        output: &NodeStream,
        path_streams: &[PathUpdateElement],
        preamble: Option<&TokenStream>,
        update_body: &TokenStream,
    ) {
        let (loops, first_access) = path_streams
            .iter()
            .rev()
            .fold(None, |tail, path_stream| {
                let out_record_definition =
                    self.sub_stream_definition_fragments(&path_stream.sub_output_stream);
                let in_record_definition =
                    self.sub_stream_definition_fragments(&path_stream.sub_input_stream);
                let input_record = in_record_definition.record();
                let record = out_record_definition.record();
                let unpacked_record_in = out_record_definition.unpacked_record_in();
                let record_and_unpacked_out = out_record_definition.record_and_unpacked_out();

                let access = path_stream.field.ident();
                let mut_access = path_stream.field.mut_ident();
                Some(if let Some((tail, sub_access)) = tail {
                    (
                        quote! {
                            // TODO optimize this code in truc
                            let converted = convert_vec_in_place::<#input_record, #record, _>(
                                #access,
                                |record, _| {
                                    let #record_and_unpacked_out {
                                        mut record,
                                        #sub_access,
                                    } = #record_and_unpacked_out::from((
                                        record,
                                        #unpacked_record_in { #sub_access: Vec::new() },
                                    ));
                                    #tail
                                    VecElementConversionResult::Converted(record)
                                },
                            );
                            *record.#mut_access() = converted;
                        },
                        access,
                    )
                } else {
                    (
                        quote! {
                            // TODO optimize this code in truc
                            let converted = convert_vec_in_place::<#input_record, #record, _>(
                                #access,
                                #update_body
                            );
                            *record.#mut_access() = converted;
                        },
                        access,
                    )
                })
            })
            .expect("loops");

        let def = self.stream_definition_fragments(output);
        let unpacked_record_in = def.unpacked_record_in();
        let record_and_unpacked_out = def.record_and_unpacked_out();

        let inline_body = quote! {
            use truc_runtime::convert::{convert_vec_in_place, VecElementConversionResult};

            #preamble

            input.map(move |record| {
                let #record_and_unpacked_out {
                    mut record,
                    #first_access,
                } = #record_and_unpacked_out::from((
                    record, #unpacked_record_in { #first_access: Vec::new() },
                ));
                #loops
                Ok(record)
            })
        };

        self.implement_inline_node(node, input, output, &inline_body);
    }
}

pub const DEFAULT_CHAIN_ROOT_MODULE_NAME: [&str; 2] = ["crate", "chain"];
pub const DEFAULT_CHAIN_STREAMS_MODULE_NAME: &str = "streams";
pub const DEFAULT_CHAIN_ERROR_TYPE: [&str; 2] = ["quirky_binder_support", "QuirkyBinderError"];
pub const DEFAULT_CHAIN_MAIN_NAME: &str = "main";

pub struct ChainCustomizer {
    pub streams_module_name: FullyQualifiedName,
    pub module_name: FullyQualifiedName,
    pub custom_module_imports: Vec<(String, String)>,
    pub error_type: FullyQualifiedName,
    pub main_name: String,
    pub main_attrs: Vec<String>,
}

impl ChainCustomizer {
    pub fn error_type_path(&self) -> String {
        self.error_type
            .iter()
            .take(self.error_type.len() - 1)
            .map(Deref::deref)
            .collect()
    }

    pub fn error_type_name(&self) -> String {
        self.error_type
            .iter()
            .last()
            .expect("error_type last")
            .to_string()
    }
}

impl Default for ChainCustomizer {
    fn default() -> Self {
        Self {
            streams_module_name: FullyQualifiedName::new_n(
                DEFAULT_CHAIN_ROOT_MODULE_NAME
                    .iter()
                    .chain([DEFAULT_CHAIN_STREAMS_MODULE_NAME].iter()),
            ),
            module_name: FullyQualifiedName::new_n(DEFAULT_CHAIN_ROOT_MODULE_NAME.iter()),
            custom_module_imports: vec![],
            error_type: FullyQualifiedName::new_n(DEFAULT_CHAIN_ERROR_TYPE.iter()),
            main_name: DEFAULT_CHAIN_MAIN_NAME.to_string(),
            main_attrs: Vec::default(),
        }
    }
}

#[derive(Default)]
pub struct ImportScope {
    fixed: Vec<(String, String)>,
    used: bool,
}

impl ImportScope {
    pub fn add_import(&mut self, path: &str, ty: &str) {
        self.fixed.push((path.to_string(), ty.to_string()));
    }

    pub fn import(mut self, scope: &mut Scope, customizer: &ChainCustomizer) {
        scope.import(&customizer.error_type_path(), &customizer.error_type_name());
        for (path, ty) in &self.fixed {
            scope.import(path, ty);
        }
        self.used = true;
    }
}

impl Drop for ImportScope {
    fn drop(&mut self) {
        assert!(self.used);
    }
}

#[derive(Clone, Debug)]
pub struct Trace<'a> {
    elements: Vec<TraceElement<'a>>,
}

impl<'a> Trace<'a> {
    pub fn new_leaf(element: TraceElement<'a>) -> Self {
        Trace {
            elements: vec![element],
        }
    }

    pub fn push(mut self, element: TraceElement<'a>) -> Self {
        self.elements.push(element);
        self
    }

    pub fn to_owned(&self) -> Trace<'static> {
        Trace {
            elements: self.elements.iter().map(TraceElement::to_owned).collect(),
        }
    }
}

impl<'a> Display for Trace<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (index, element) in self.elements.iter().enumerate() {
            f.write_fmt(format_args!("{:4}: {}", index, element))?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, new)]
pub struct TraceElement<'a> {
    source: Cow<'a, str>,
    name: Cow<'a, str>,
    location: Location,
}

impl<'a> TraceElement<'a> {
    pub fn to_owned(&self) -> TraceElement<'static> {
        TraceElement {
            source: self.source.clone().into_owned().into(),
            name: self.name.clone().into_owned().into(),
            location: self.location,
        }
    }
}

impl<'a> Display for TraceElement<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "{}\n             at {}:{}:{}\n",
            self.name, self.source, self.location.line, self.location.col
        ))
    }
}

#[macro_export]
macro_rules! trace_element {
    ($name:expr) => {
        || {
            TraceElement::new(
                std::file!().into(),
                ($name).into(),
                quirky_binder_lang::location::Location::new(
                    std::line!() as usize,
                    std::column!() as usize,
                ),
            )
            .to_owned()
        }
    };
}
