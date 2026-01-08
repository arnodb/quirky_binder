use std::collections::HashMap;

use itertools::Itertools;
use proc_macro2::TokenStream;
pub use quirky_binder_lang::location::Location;
use serde::Deserialize;
use syn::{visit_mut::VisitMut, Ident, Type};

use crate::{chain::type_rewriter::StreamsRewriter, codegen::Module, prelude::*};

pub mod customizer;
pub mod error;
pub mod result;
pub mod trace;
pub mod type_rewriter;

/// Chain thread data.
#[derive(Debug)]
struct ChainThread {
    /// Identifier, so that it does not have to be passed separately.
    id: usize,
    /// The thread name.
    name: FullyQualifiedName,
    /// The thread type.
    thread_type: ChainThreadType,
    /// Name of the main function called by this thread.
    main: Option<FullyQualifiedName>,
    /// Nodes data.
    ///
    /// * node name
    /// * number of inputs
    /// * number of outputs
    ///
    /// It does not contain clusters.
    nodes_data: Vec<(FullyQualifiedName, usize, usize)>,
    /// Input streams.
    input_streams: Box<[NodeStream]>,
    /// Output streams.
    output_streams: Box<[NodeStream]>,
    /// Input pipes.
    input_pipes: Option<Box<[usize]>>,
    /// Output pipes.
    output_pipes: Option<Box<[usize]>>,
}

/// Thread type.
#[derive(Clone, Copy, PartialEq, Eq, Default, Deserialize, Debug)]
pub enum ChainThreadType {
    /// Regular chain thread, it eventually completes processing.
    #[default]
    Regular,
    /// Background thread, it has to be interrupted in order to complete the processing.
    Background,
}

/// Chain source data.
#[derive(Clone)]
pub struct ChainSourceThread {
    /// Identifier of the thread which is the source of the data.
    pub thread_id: usize,
    /// Which node is the source of the data.
    pub node_name: FullyQualifiedName,
    /// Which stream of this thread is the source of the data.
    pub stream_index: usize,
    /// Whether or not it's piped.
    pub piped: bool,
}

/// The main structure used to generate the chain.
#[derive(new)]
pub struct Chain<'a> {
    /// The chain customizer.
    customizer: &'a ChainCustomizer,
    /// The Rust module being generated.
    module: &'a mut Module,
    /// Thread information.
    ///
    /// Some functions return thread IDs as `usize`, they are indices in this vector.
    #[new(default)]
    threads: Vec<ChainThread>,
    /// Threads that are currently identified as the source for a stream.
    ///
    /// This structure is updated when new filters are added to the thread: the original source is
    /// removed (because it is consumed) and new ones are added (outputs of the added filters).
    #[new(default)]
    thread_by_source: HashMap<NodeStreamSource, ChainSourceThread>,
    /// Pipe sequence used to identify pipes in this chain.
    #[new(default)]
    pipe_count: usize,
    /// Links from one node's (tail) output to another node's (head) input.
    ///
    /// * node name of the tail
    /// * stream index of the tail
    /// * node name of the head
    /// * stream index of the head
    #[new(default)]
    nodes_links: Vec<(FullyQualifiedName, usize, FullyQualifiedName, usize)>,
}

impl<'a> Chain<'a> {
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
                    node_name: name.clone(),
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
        let link = (
            source_thread.node_name.clone(),
            source_thread.stream_index,
            name.clone(),
            0,
        );
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
            self.nodes_links.push(link);
            if let Some(output) = output {
                self.update_thread_single_stream(thread_id, name, output);
            }
            ChainSourceThread {
                thread_id,
                node_name: name.clone(),
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
            self.nodes_links.push(link);
            if let Some(output) = output {
                self.update_thread_single_stream(source_thread.thread_id, name, output);
            }
            source_thread
        }
    }

    fn update_thread_single_stream(
        &mut self,
        thread_id: usize,
        node_name: &FullyQualifiedName,
        stream: &NodeStream,
    ) {
        let thread = self.threads.get_mut(thread_id).expect("thread");
        assert_eq!(thread.output_streams.len(), 1);
        self.thread_by_source
            .remove(thread.output_streams[0].source());
        thread.output_streams[0] = stream.clone();
        self.thread_by_source.insert(
            stream.source().clone(),
            ChainSourceThread {
                thread_id,
                node_name: node_name.clone(),
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

    fn pipe_single_thread(
        &mut self,
        source: &NodeStreamSource,
        node_name: &FullyQualifiedName,
        input_index: usize,
    ) -> usize {
        let source_thread = self.get_source_thread(source).clone();
        let thread = &mut self.threads[source_thread.thread_id];

        let link = (
            source_thread.node_name.clone(),
            source_thread.stream_index,
            node_name.clone(),
            input_index,
        );

        self.nodes_links.push(link);

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

        let output = self.format_thread_output(
            source_thread.thread_id,
            0,
            NodeStatisticsOption::WithoutStatistics,
        );

        let pipe_def = quote! {
            pub fn quirky_binder_pipe(mut thread_control: ThreadControl) -> impl FnOnce() -> Result<(), #error_type> {
                move || {
                    let mut output = #output;
                    #input
                    while let Some(record) = input.next()? {
                        output.send(Some(record))?;
                    }
                    output.send(None)?;
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
        node_name: &FullyQualifiedName,
        inputs: &[NodeStream],
        outputs: &[NodeStream],
    ) -> usize {
        let input_pipes = inputs
            .iter()
            .enumerate()
            .map(|(input_index, input)| {
                self.pipe_single_thread(input.source(), node_name, input_index)
            })
            .collect::<Vec<usize>>();
        self.new_thread(
            node_name.clone(),
            ChainThreadType::Regular,
            inputs.to_vec().into_boxed_slice(),
            outputs.to_vec().into_boxed_slice(),
            Some(input_pipes.into_boxed_slice()),
            false,
            Some(node_name.clone()),
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
                    fn node_statuses(&self) -> Box<dyn Iterator<Item = NodeStatusItem<'_>> + '_> {
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
            let chain_nodes = self
                .threads
                .iter()
                .flat_map(|thread| {
                    thread.nodes_data.iter().map(|(name, _, _)| {
                        let name = name.to_full_name().to_string();
                        quote! {
                            NodeDescriptor {
                                name: #name,
                            }
                        }
                    })
                })
                .collect::<Vec<_>>();
            let chain_nodes_length = chain_nodes.len();

            let chain_edges = self
                .nodes_links
                .iter()
                .map(|(tail_name, tail_index, head_name, head_index)| {
                    let tail_name = tail_name.to_full_name().to_string();
                    let head_name = head_name.to_full_name().to_string();
                    quote! {
                        EdgeDescriptor {
                            tail_name: #tail_name,
                            tail_index: #tail_index,
                            head_name: #head_name,
                            head_index: #head_index,
                        }
                    }
                })
                .collect::<Vec<_>>();
            let chain_edges_length = chain_edges.len();

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

                impl DynChainStatus for ChainStatus {
                    fn nodes_length(&self) -> usize {
                        #chain_nodes_length
                    }

                    fn nodes(&self) -> impl Iterator<Item = NodeDescriptor<'_>> {
                        [
                            #(#chain_nodes,)*
                        ].into_iter()
                    }

                    fn edges_length(&self) -> usize {
                        #chain_edges_length
                    }

                    fn edges(&self) -> impl Iterator<Item = EdgeDescriptor<'_>> {
                        [
                            #(#chain_edges,)*
                        ].into_iter()
                    }

                    fn statuses(&self) -> impl Iterator<Item = Arc<Mutex<dyn DynThreadStatus>>> {
                        [
                            #((#chain_status_items.clone() as Arc<Mutex<dyn DynThreadStatus>>),)*
                        ].into_iter()
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

    fn rewrite_body(&self, body: &TokenStream, node: &dyn DynNode) -> syn::Block {
        let mut block = syn::parse2::<syn::Block>(quote! {{ #body }}).expect("Block");
        StreamsRewriter::new(node, self).visit_block_mut(&mut block);
        block
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
        let node_name = name.to_string();

        let input = self.format_source_thread_input(
            &thread,
            input.source(),
            false,
            NodeStatisticsOption::WithStatistics { node_name: name },
        );

        let inline_body = self.rewrite_body(inline_body, node);

        let fn_def = quote! {
            pub fn #fn_name(#[allow(unused_mut)] mut thread_control: #thread_module::ThreadControl) -> impl FallibleIterator<Item = #record, Error = #error_type> {
                let thread_status = thread_control.status.clone();

                #input

                let output = #inline_body;

                output
                    .inspect({
                        let thread_status = thread_status.clone();
                        move |_| {
                            let mut lock = thread_status.lock().unwrap();
                            lock.#node_status_ident.output_written[0] += 1;
                            Ok(())
                        }
                    })
                    .inspect_end({
                        let thread_status = thread_status.clone();
                        move || {
                            let mut lock = thread_status.lock().unwrap();
                            lock.#node_status_ident.state.transition_to(NodeState::Success, #node_name);
                            Ok(())
                        }
                    })
                    .map_err({
                        let thread_status = thread_status.clone();
                        move |err: #error_type| {
                            let mut lock = thread_status.lock().unwrap();
                            lock.#node_status_ident.state.transition_to(NodeState::Error(err.to_string()), #node_name);
                            err
                        }
                    })
            }
        };

        let mut import_scope = ImportScope::default();
        import_scope.add_import("quirky_binder_support", "prelude::*");
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

        let thread_body = self.rewrite_body(thread_body, node);

        let fn_def = quote! {
            pub fn #fn_name(#[allow(unused_mut)] mut thread_control: #thread_module::ThreadControl) -> impl FnOnce() -> Result<(), #error_type> {
                let thread_status = thread_control.status.clone();

                #[allow(unused_mut)]
                let mut thread_body = #thread_body;
                move || {
                    thread_body()
                        .map_err({
                            let thread_status = thread_status.clone();
                            move |err: #error_type| {
                                let mut lock = thread_status.lock().unwrap();
                                lock.#node_status_ident.state = NodeState::Error(err.to_string());
                                err
                            }
                        })?;
                    {
                        let mut lock = thread_status.lock().unwrap();
                        lock.#node_status_ident.state = NodeState::Success;
                    }
                    Ok(())
                }
            }
        };

        let mut import_scope = ImportScope::default();
        import_scope.add_import("quirky_binder_support::prelude", "*");
        if !node.inputs().is_empty() {
            import_scope.add_import("fallible_iterator", "FallibleIterator");
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

        let inline_body = quote! {
            #preamble

            input.map(move |record| {
                let Output0AndUnpackedOut {
                    mut record,
                    #first_access,
                } = Output0AndUnpackedOut::from((
                    record, UnpackedOutputIn0 { #first_access: Vec::new() },
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
                let node_name = node_name.to_string();
                Some(quote! {
                    .inspect({
                        let thread_status = thread_status.clone();
                        let mut started = false;
                        move |_| {
                            let mut lock = thread_status.lock().unwrap();
                            if !started {
                                started = true;
                                lock.#node_status_ident.state.transition_to(NodeState::Running, #node_name);
                            }
                            lock.#node_status_ident.input_read[#stream_index] += 1;
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
                let node_name = node_name.to_string();
                Some(quote! {
                    .try_inspect({
                        let thread_status = thread_status.clone();
                        let mut started = false;
                        move |_| {
                            let mut lock = thread_status.lock().unwrap();
                            if !started {
                                started = true;
                                lock.#node_status_ident.state.transition_to(NodeState::Running, #node_name);
                            }
                            lock.#node_status_ident.input_read[#stream_index] += 1;
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

    pub fn format_thread_output(
        &self,
        thread_id: usize,
        stream_index: usize,
        statistics_option: NodeStatisticsOption,
    ) -> TokenStream {
        let output = format_ident!("output_{}", stream_index);
        let error_type = self.customizer.error_type.to_full_name();
        let collect_statistics = match statistics_option {
            NodeStatisticsOption::WithStatistics { node_name } => {
                let node_status_ident = self.node_status_ident(thread_id, node_name);
                quote! {
                    {
                        let thread_status = thread_status.clone();
                        move |_| {
                            let mut lock = thread_status.lock().unwrap();
                            lock.#node_status_ident.output_written[#stream_index] += 1;
                            Ok(())
                        }
                    }
                }
            }
            NodeStatisticsOption::WithoutStatistics => {
                quote! { |_| Ok(()) }
            }
        };
        quote! {
            InstrumentedThreadOutput::<_, _, #error_type>::new(
                #collect_statistics,
                thread_control
                    .#output
                    .take()
                    .unwrap_or_else(|| panic!("output {}", #stream_index)),
            )
        }
    }
}

pub trait StreamCustomizer {
    fn stream_definition_fragments<'c>(
        &'c self,
        stream: &'c NodeStream,
    ) -> RecordDefinitionFragments<'c>;

    fn sub_stream_definition_fragments<'c>(
        &'c self,
        stream: &'c NodeSubStream,
    ) -> RecordDefinitionFragments<'c>;
}

impl<'a> StreamCustomizer for Chain<'a> {
    fn stream_definition_fragments<'c>(
        &'c self,
        stream: &'c NodeStream,
    ) -> RecordDefinitionFragments<'c> {
        self.customizer.stream_definition_fragments(stream)
    }

    fn sub_stream_definition_fragments<'c>(
        &'c self,
        stream: &'c NodeSubStream,
    ) -> RecordDefinitionFragments<'c> {
        self.customizer.sub_stream_definition_fragments(stream)
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

pub enum NodeStatisticsOption<'a> {
    WithStatistics { node_name: &'a FullyQualifiedName },
    WithoutStatistics,
}
