use std::{borrow::Cow, collections::HashMap, fmt::Display};

use itertools::Itertools;
use proc_macro2::TokenStream;
pub use quirky_binder_lang::location::Location;
use serde::Deserialize;
use syn::{Ident, Type};

use self::error::{ChainError, ChainErrorWithTrace};
use crate::{codegen::Module, prelude::*};

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
    nodes_data: Vec<(FullyQualifiedName, usize, usize)>,
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

#[derive(new)]
pub struct Chain<'a> {
    customizer: &'a ChainCustomizer,
    module: &'a mut Module,
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
        RecordDefinitionFragments::new(
            stream.record_type(),
            stream.variant_id(),
            &self.customizer.streams_module_name,
        )
    }

    pub fn sub_stream_definition_fragments(
        &self,
        stream: &'a NodeSubStream,
    ) -> RecordDefinitionFragments<'a> {
        RecordDefinitionFragments::new(
            stream.record_type(),
            stream.variant_id(),
            &self.customizer.streams_module_name,
        )
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
        let nodes_data = vec![(name.clone(), input_streams.len(), output_streams.len())];
        self.threads.push(ChainThread {
            id: thread_id,
            name,
            thread_type,
            main,
            nodes_data,
            input_streams,
            output_streams,
            input_pipes,
            output_pipes,
        });
        let name = format!("thread_{thread_id}");
        let module = self.module.get_or_new_module(&name);
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
        name: &FullyQualifiedName,
        output: Option<&NodeStream>,
    ) -> ChainSourceThread {
        let source_thread = self.get_source_thread(input.source());
        let thread_id = source_thread.thread_id;
        let thread = &self.threads[thread_id];
        if let Some(output_pipes) = &thread.output_pipes {
            let input_pipe = output_pipes[source_thread.stream_index];
            let streams: Box<[NodeStream]> =
                Box::new([thread.output_streams[source_thread.stream_index].clone()]);
            let output_streams = if output.is_some() {
                streams.clone()
            } else {
                Box::new([])
            };
            let thread_id = self.new_thread(
                name.clone(),
                ChainThreadType::Regular,
                streams,
                output_streams,
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
            let source_thread = source_thread.clone();
            let thread = &mut self.threads[source_thread.thread_id];
            assert!(!thread.nodes_data.iter().any(|(n, _, _)| n == name));
            thread
                .nodes_data
                .push((name.clone(), 1, if output.is_some() { 1 } else { 0 }));
            if let Some(output) = output {
                self.update_thread_single_stream(source_thread.thread_id, output);
            }
            source_thread
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

        assert_eq!(thread.output_streams.len(), 1);

        let error_type = self.customizer.error_type.to_full_name();

        let input = self.format_source_thread_input(
            &source_thread,
            source,
            true,
            NodeStatisticsOption::WithoutStatistics,
        );

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

        let pipe = self.new_pipe();
        let name = format!("thread_{}", source_thread.thread_id);
        let module = self.module.get_module(&name).expect("thread module");
        module.fragment(pipe_def.to_string());

        let thread = &mut self.threads[source_thread.thread_id];
        thread.output_pipes = Some(Box::new([pipe]));
        thread.main = Some(FullyQualifiedName::new(name).sub("quirky_binder_pipe"));

        let mut import_scope = ImportScope::default();
        import_scope.add_import("fallible_iterator", "FallibleIterator");
        import_scope.import(module);

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
            let inputs = (0..thread.input_streams.len()).map(|i| format_ident!("input_{}", i));
            let input_types = thread.input_streams.iter().map(|input_stream| {
                let def = self.stream_definition_fragments(input_stream);
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
                    let def = self.stream_definition_fragments(output_stream);
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
            let interrupt_impl = match thread.thread_type {
                ChainThreadType::Regular => None,
                ChainThreadType::Background => Some(quote! {
                    pub fn wait_until_interrupted(&self) {
                        let is_interrupted = self.interrupt.1.wait_while(
                            self.interrupt.0.lock().unwrap(),
                            |is_interrupted| !*is_interrupted,
                        ).unwrap();
                        debug_assert!(*is_interrupted);
                    }

                    pub fn wait_timeout_until_interrupted(&self, dur: std::time::Duration) -> bool {
                        let is_interrupted = self.interrupt.1.wait_timeout_while(
                            self.interrupt.0.lock().unwrap(),
                            dur,
                            |is_interrupted| !*is_interrupted,
                        ).unwrap().0;
                        *is_interrupted
                    }
                }),
            };
            let nodes_statuses = thread.nodes_data.iter().enumerate().map(
                |(node_index, (name, input_count, output_count))| {
                    let ident = format_ident!("node_{node_index}");
                    let full_name = name.to_full_name().to_string();
                    quote! {
                        #[doc = #full_name]
                        pub #ident: NodeStatus<#input_count, #output_count>,
                    }
                },
            );
            let nodes_status_items =
                thread
                    .nodes_data
                    .iter()
                    .enumerate()
                    .map(|(node_index, (name, _, _))| {
                        let ident = format_ident!("node_{node_index}");
                        let full_name = name.to_full_name().to_string();
                        quote! {
                            NodeStatusItem {
                                node_name: #full_name,
                                state: &self.#ident.state,
                                input_read: &self.#ident.input_read[..],
                                output_written: &self.#ident.output_written[..],
                            }
                        }
                    });
            let thread_structs_definitions = quote! {

                #[derive(Default)]
                pub struct ThreadStatus {
                    #(#nodes_statuses)*
                }

                impl DynThreadStatus for ThreadStatus {
                    fn node_statuses(&self) -> Box<dyn Iterator<Item = NodeStatusItem> + '_> {
                        Box::new([
                            #(#nodes_status_items,)*
                        ].into_iter())
                    }
                }

                pub struct ThreadOuterControl {
                    #interrupt
                    pub status: Arc<Mutex<ThreadStatus>>,
                }

                pub struct ThreadControl {
                    pub chain_configuration: Arc<ChainConfiguration>,
                    #interrupt
                    pub status: Arc<Mutex<ThreadStatus>>,
                    #(pub #inputs: Option<ThreadInput<#input_types>>,)*
                    #(pub #outputs: Option<ThreadOutput<#output_types>>,)*
                }

                impl ThreadControl {
                    #interrupt_impl
                }

            };
            let name = format!("thread_{}", thread.id);
            let module = self.module.get_module(&name).expect("thread module");
            module.import("std::sync", "Arc");
            module.import("std::sync", "Mutex");
            module.import("quirky_binder_support::prelude", "*");
            module.fragment(thread_structs_definitions.to_string());
        }

        let error_type = self.customizer.error_type.to_full_name();

        let chain_threads_structs_definitions = {
            let chain_statuses = self.threads.iter().map(|thread| {
                let thread_module = format_ident!("thread_{}", thread.id);
                let thread_status = format_ident!("thread_status_{}", thread.id);
                quote! {
                    pub #thread_status: Arc<Mutex<#thread_module::ThreadStatus>>,
                }
            });

            let chain_status_items = self.threads.iter().map(|thread| {
                let thread_status = format_ident!("thread_status_{}", thread.id);
                quote! {
                    self.#thread_status
                }
            });

            let chain_threads_joins = self.threads.iter().map(|thread| {
                let interrupt = match thread.thread_type {
                    ChainThreadType::Regular => None,
                    ChainThreadType::Background => {
                        self.module.import("std::sync", "Condvar");
                        let thread_interrupt = format_ident!("thread_interrupt_{}", thread.id);
                        Some(quote! {
                            pub #thread_interrupt: Arc<(Mutex<bool>, Condvar)>,
                        })
                    }
                };
                let thread_join = format_ident!("thread_join_{}", thread.id);
                quote! {
                    #interrupt
                    pub #thread_join: std::thread::JoinHandle<Result<(), #error_type>>,
                }
            });

            let join_result = if !self.threads.is_empty() {
                quote! {
                    let mut result = Ok(());
                }
            } else {
                quote! {
                    #[allow(clippy::let_and_return)]
                    let result = Ok(());
                }
            };

            let join_regular_threads = self
                .threads
                .iter()
                .filter(|thread| thread.thread_type == ChainThreadType::Regular)
                .map(|thread| {
                    let thread_join = format_ident!("thread_join_{}", thread.id);
                    quote! {
                        result = result.and(self.#thread_join.join().unwrap());
                    }
                });

            let interrupt_background_threads = self
                .threads
                .iter()
                .filter(|thread| thread.thread_type == ChainThreadType::Background)
                .map(|thread| {
                    let thread_interrupt = format_ident!("thread_interrupt_{}", thread.id);
                    quote! {{
                        let mut is_interrupted = self.#thread_interrupt.0.lock().unwrap();
                        *is_interrupted = true;
                        self.#thread_interrupt.1.notify_all();
                    }}
                });

            let join_background_threads = self
                .threads
                .iter()
                .filter(|thread| thread.thread_type == ChainThreadType::Background)
                .map(|thread| {
                    let thread_join = format_ident!("thread_join_{}", thread.id);
                    quote! {
                        result = result.and(self.#thread_join.join().unwrap());
                    }
                });

            quote! {
                pub struct ChainStatus {
                    #(#chain_statuses)*
                }

                impl ChainStatus {
                    pub fn statuses(&self) -> Box<dyn Iterator<Item = Arc<Mutex<dyn DynThreadStatus>>> + Send> {
                        Box::new([
                            #((#chain_status_items.clone() as Arc<Mutex<dyn DynThreadStatus>>),)*
                        ].into_iter())
                    }
                }

                #[must_use]
                pub struct ChainJoinHandle {
                    #(#chain_threads_joins)*
                }

                impl ChainJoinHandle {
                    pub fn join_all(self) -> Result<(), #error_type> {
                        #join_result

                        #(#join_regular_threads)*

                        #(#interrupt_background_threads)*

                        #(#join_background_threads)*

                        result
                    }
                }
            }
        };

        {
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
                            interrupt: thread_outer_control.interrupt.clone(),
                        }
                    });
                    let inputs = thread
                        .input_pipes
                        .as_ref()
                        .map(|input_pipes| {
                            input_pipes.iter().enumerate().map(|(index, pipe)| {
                                let input = format_ident!("input_{}", index);
                                let rx = format_ident!("rx_{}", pipe);
                                quote! { #input: Some(ThreadInput::new(#rx)), }
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
                                quote! { #output: Some(ThreadOutput::new(#tx)), }
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
                        let thread_status = Arc::new(Mutex::new(#thread_module::ThreadStatus::default()));
                        let thread_outer_control = #thread_module::ThreadOuterControl {
                            #interrupt
                            status: thread_status.clone(),
                        };
                        let thread_control = #thread_module::ThreadControl {
                            #config_assignment
                            #interrupt_clone
                            status: thread_status.clone(),
                            #(#inputs)*
                            #(#outputs)*
                        };
                    }
                });

            let thread_vars = self.threads.iter().map(|thread| {
                let thread_outer_control = format_ident!("thread_outer_control_{}", thread.id);
                let join_thread = format_ident!("thread_join_{}", thread.id);
                quote! { (#thread_outer_control, #join_thread) }
            });

            let spawn_threads = self.threads.iter().map(|thread| {
                let thread_main =
                    syn::parse_str::<syn::Expr>(&thread.main.as_ref().expect("main").to_string())
                        .expect("thread_main");
                quote! {
                    (
                        thread_outer_control,
                        std::thread::spawn(#thread_main(thread_control)),
                    )
                }
            });

            let result = {
                let status_members = self.threads.iter().map(|thread| {
                    let thread_outer_control = format_ident!("thread_outer_control_{}", thread.id);
                    let thread_status = format_ident!("thread_status_{}", thread.id);
                    quote! {
                        #thread_status: #thread_outer_control.status,
                    }
                });
                let join_members = self.threads.iter().map(|thread| {
                    let interrupt = match thread.thread_type {
                        ChainThreadType::Regular => None,
                        ChainThreadType::Background => {
                            let thread_outer_control =
                                format_ident!("thread_outer_control_{}", thread.id);
                            let thread_interrupt = format_ident!("thread_interrupt_{}", thread.id);
                            Some(quote! {
                                #thread_interrupt: #thread_outer_control.interrupt,
                            })
                        }
                    };
                    let thread_join = format_ident!("thread_join_{}", thread.id);
                    quote! {
                        #interrupt
                        #thread_join,
                    }
                });
                quote! {
                    let status = ChainStatus {
                        #(#status_members)*
                    };
                    let join_handle = ChainJoinHandle {
                        #(#join_members)*
                    };
                    Ok((status, join_handle))
                }
            };

            self.module.import("std::sync", "Arc");
            self.module.import("std::sync", "Mutex");
            self.module.import("quirky_binder_support::prelude", "*");

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
                #chain_threads_structs_definitions

                #main_attrs
                pub fn #main_name(chain_configuration: ChainConfiguration) -> Result<(ChainStatus, ChainJoinHandle), #error_type> {
                    #[allow(unused_variables)]
                    let chain_configuration = Arc::new(chain_configuration);

                    #(#channels)*

                    #(let #thread_vars = {
                        #thread_controls

                        #spawn_threads
                    };)*

                    #result
                }
            };
            self.module.fragment(main_def.to_string());
        }
    }

    fn get_or_new_module<'i>(
        &mut self,
        path: impl IntoIterator<Item = &'i Box<str>>,
        chain_customizer: &ChainCustomizer,
        thread_id: usize,
    ) -> &mut Module {
        let mut iter = path.into_iter();
        let customize_module = |module: &mut Module| {
            for (path, ty) in &chain_customizer.custom_module_imports {
                module.import(path, ty);
            }
            let thread_module = format!("thread_{thread_id}");
            module.import("super", &thread_module);
        };
        if let Some(first) = iter.next() {
            let module = self.module.get_or_new_module(first);
            (customize_module)(module);
            iter.fold(module, |m, n| {
                let module = m.get_or_new_module(n);
                (customize_module)(module);
                module
            })
        } else {
            self.module
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
        let error_type = self.customizer.error_type.to_full_name();

        let node_status_ident = self.node_status_ident(thread.thread_id, name);

        let input = self.format_source_thread_input(
            &thread,
            input.source(),
            false,
            NodeStatisticsOption::WithStatistics { node_name: name },
        );

        let fn_def = quote! {
            pub fn #fn_name(#[allow(unused_mut)] mut thread_control: #thread_module::ThreadControl) -> impl FallibleIterator<Item = #record, Error = #error_type> {
                let thread_status = thread_control.status.clone();

                #input

                let output = {
                    #inline_body
                };

                output
                    .inspect({
                        let thread_status = thread_status.clone();
                        move |_| {
                            thread_status
                                .lock()
                                .unwrap()
                                .#node_status_ident
                                .output_written[0] += 1;
                            Ok(())
                        }
                    })
                    .map_err({
                        use quirky_binder_support::prelude::*;
                        let thread_status = thread_status.clone();
                        move |err: #error_type| {
                            thread_status
                                .lock()
                                .unwrap()
                                .#node_status_ident
                                .state = NodeState::Error(err.to_string());
                            err
                        }
                    })
            }
        };

        let mut import_scope = ImportScope::default();
        import_scope.add_import("fallible_iterator", "FallibleIterator");

        let module = self.get_or_new_module(
            name.iter().take(name.len() - 1),
            self.customizer,
            thread.thread_id,
        );

        module.fragment(fn_def.to_string());

        import_scope.import(module);
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
        let error_type = self.customizer.error_type.to_full_name();

        let node_status_ident = self.node_status_ident(thread_id, name);

        let fn_def = quote! {
            pub fn #fn_name(#[allow(unused_mut)] mut thread_control: #thread_module::ThreadControl) -> impl FnOnce() -> Result<(), #error_type> {
                let thread_status = thread_control.status.clone();

                #[allow(unused_mut)]
                let mut thread_body = {
                    #thread_body
                };
                move || {
                    thread_body()
                        .map_err({
                            use quirky_binder_support::prelude::*;
                            let thread_status = thread_status.clone();
                            move |err: #error_type| {
                                thread_status
                                    .lock()
                                    .unwrap()
                                    .#node_status_ident
                                    .state = NodeState::Error(err.to_string());
                                err
                            }
                        })
                }
            }
        };

        let mut import_scope = ImportScope::default();
        if !node.inputs().is_empty() {
            import_scope.add_import("fallible_iterator", "FallibleIterator");
            import_scope.add_import("quirky_binder_support::prelude", "*");
        }

        let module =
            self.get_or_new_module(name.iter().take(name.len() - 1), self.customizer, thread_id);

        module.fragment(fn_def.to_string());

        import_scope.import(module);
    }

    pub fn implement_path_update(
        &mut self,
        node: &dyn DynNode,
        input: &NodeStream,
        output: &NodeStream,
        path_streams: &[PathUpdateElement],
        preamble: Option<&TokenStream>,
        build_leaf_body: impl FnOnce(Type, Type, Ident, Ident) -> TokenStream,
    ) {
        let preamble = quote! {
            #[allow(unused)]
            use truc_runtime::convert::{try_convert_vec_in_place, VecElementConversionResult};

            #preamble
        };

        let error_type = self.customizer.error_type.to_full_name();

        let mut iter = path_streams.iter().rev();

        let leaf_body = {
            let path_stream = iter.next().expect("leaf path stream");

            let out_record_definition =
                self.sub_stream_definition_fragments(&path_stream.sub_output_stream);
            let in_record_definition =
                self.sub_stream_definition_fragments(&path_stream.sub_input_stream);
            let input_record = in_record_definition.record();
            let record = out_record_definition.record();

            let access = path_stream.field.ident();
            let mut_access = path_stream.field.mut_ident();

            let leaf = build_leaf_body(input_record, record, access.clone(), mut_access);
            (leaf, access)
        };

        let (body, first_access) = iter.fold(leaf_body, |(tail, sub_access), path_stream| {
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
            let body = quote! {
                // TODO optimize this code in truc
                let converted = try_convert_vec_in_place::<#input_record, #record, _, #error_type>(
                    #access,
                    |record, _| -> Result<_, #error_type> {
                        let #record_and_unpacked_out {
                            mut record,
                            #sub_access,
                        } = #record_and_unpacked_out::from((
                            record,
                            #unpacked_record_in { #sub_access: Vec::new() },
                        ));
                        #tail
                        Ok(VecElementConversionResult::Converted(record))
                    },
                )?;
                *record.#mut_access() = converted;
            };
            (body, access)
        });

        let def = self.stream_definition_fragments(output);
        let unpacked_record_in = def.unpacked_record_in();
        let record_and_unpacked_out = def.record_and_unpacked_out();

        let inline_body = quote! {
            #preamble

            input.map(move |record| {
                let #record_and_unpacked_out {
                    mut record,
                    #first_access,
                } = #record_and_unpacked_out::from((
                    record, #unpacked_record_in { #first_access: Vec::new() },
                ));
                #body
                Ok(record)
            })
        };

        self.implement_inline_node(node, input, output, &inline_body);
    }

    fn node_status_ident(&self, thread_id: usize, node_name: &FullyQualifiedName) -> Ident {
        let node_index = self.threads[thread_id]
            .nodes_data
            .iter()
            .rposition(|(n, _, _)| n == node_name)
            .unwrap();
        format_ident!("node_{node_index}")
    }

    pub fn format_inline_input(
        &self,
        thread_id: usize,
        source_name: &NodeStreamSource,
        stream_index: usize,
        statistics_option: NodeStatisticsOption,
    ) -> TokenStream {
        let collect_statistics = match statistics_option {
            NodeStatisticsOption::WithStatistics { node_name } => {
                let node_status_ident = self.node_status_ident(thread_id, node_name);
                Some(quote! {
                    .inspect({
                        let thread_status = thread_status.clone();
                        move |_| {
                            thread_status
                                .lock()
                                .unwrap()
                                .#node_status_ident
                                .input_read[#stream_index] += 1;
                            Ok(())
                        }
                    })
                })
            }
            NodeStatisticsOption::WithoutStatistics => None,
        };
        let input = syn::parse_str::<syn::Path>(&format!(
            "{}::{}",
            self.customizer.module_name, source_name
        ))
        .expect("chain_module");
        quote! {
            #input(thread_control) #collect_statistics
        }
    }

    pub fn format_thread_input(
        &self,
        thread_id: usize,
        stream_index: usize,
        statistics_option: NodeStatisticsOption,
    ) -> TokenStream {
        let input = format_ident!("input_{}", stream_index);
        let error_type = self.customizer.error_type.to_full_name();
        let collect_statistics = match statistics_option {
            NodeStatisticsOption::WithStatistics { node_name } => {
                let node_status_ident = self.node_status_ident(thread_id, node_name);
                Some(quote! {
                    .try_inspect({
                        let thread_status = thread_status.clone();
                        move |_| {
                            thread_status
                                .lock()
                                .unwrap()
                                .#node_status_ident
                                .input_read[#stream_index] += 1;
                            Ok(())
                        }
                    })
                })
            }
            NodeStatisticsOption::WithoutStatistics => None,
        };
        quote! {
            thread_control
                .#input
                .take()
                .unwrap_or_else(|| panic!("input {}", #stream_index))
                .into_try_fallible_iter()
                .try_map_err(#error_type::from)
                #collect_statistics
        }
    }
    pub fn format_source_thread_input(
        &self,
        source_thread: &ChainSourceThread,
        source_name: &NodeStreamSource,
        mutable: bool,
        statistics_option: NodeStatisticsOption,
    ) -> TokenStream {
        let mutable_input = mutable.then(|| quote! {mut});
        let stream_index = source_thread.stream_index;
        // The function is only called in an inline node context
        assert_eq!(stream_index, 0);
        let input = if !source_thread.piped {
            self.format_inline_input(
                source_thread.thread_id,
                source_name,
                stream_index,
                statistics_option,
            )
        } else {
            self.format_thread_input(
                source_thread.thread_id,
                source_thread.stream_index,
                statistics_option,
            )
        };
        quote! {
            let #mutable_input input = #input;
        }
    }
}

pub const DEFAULT_CHAIN_ROOT_MODULE_NAME: [&str; 2] = ["crate", "chain"];
pub const DEFAULT_CHAIN_STREAMS_MODULE_NAME: &str = "streams";
pub const DEFAULT_CHAIN_ERROR_TYPE: [&str; 2] = ["anyhow", "Error"];
pub const DEFAULT_CHAIN_ERROR_MACRO: [&str; 2] = ["anyhow", "anyhow"];
pub const DEFAULT_CHAIN_MAIN_NAME: &str = "main";

pub struct ChainCustomizer {
    pub streams_module_name: FullyQualifiedName,
    pub module_name: FullyQualifiedName,
    pub custom_module_imports: Vec<(String, String)>,
    pub error_type: FullyQualifiedName,
    pub error_macro: FullyQualifiedName,
    pub main_name: String,
    pub main_attrs: Vec<String>,
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
            error_macro: FullyQualifiedName::new_n(DEFAULT_CHAIN_ERROR_MACRO.iter()),
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

    pub fn import(mut self, module: &mut Module) {
        for (path, ty) in &self.fixed {
            module.import(path, ty);
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

impl Display for Trace<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (index, element) in self.elements.iter().enumerate() {
            f.write_fmt(format_args!("{index:4}: {element}"))?;
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

impl TraceElement<'_> {
    pub fn to_owned(&self) -> TraceElement<'static> {
        TraceElement {
            source: self.source.clone().into_owned().into(),
            name: self.name.clone().into_owned().into(),
            location: self.location,
        }
    }
}

impl Display for TraceElement<'_> {
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
                $crate::chain::Location::new(std::line!() as usize, std::column!() as usize),
            )
            .to_owned()
        }
    };
}

pub enum NodeStatisticsOption<'a> {
    WithStatistics { node_name: &'a FullyQualifiedName },
    WithoutStatistics,
}
