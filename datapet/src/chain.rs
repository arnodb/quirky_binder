use crate::{
    stream::{NodeStream, NodeStreamSource},
    support::FullyQualifiedName,
};
use codegen::{Function, Module, Scope};
use itertools::Itertools;
use proc_macro2::TokenStream;
use std::{collections::HashMap, ops::Deref};

#[derive(Debug)]
struct ChainThread {
    id: usize,
    name: FullyQualifiedName,
    main: Option<FullyQualifiedName>,
    input_streams: Box<[NodeStream]>,
    output_streams: Box<[NodeStream]>,
    input_pipes: Option<Box<[usize]>>,
    output_pipes: Option<Box<[usize]>>,
}

#[derive(Clone)]
pub struct ChainSourceThread {
    pub thread_id: usize,
    pub stream_index: usize,
    pub pipe: Option<usize>,
}

impl ChainSourceThread {
    pub fn format_input(
        &self,
        source_name: &NodeStreamSource,
        customizer: &ChainCustomizer,
        import_scope: &mut ImportScope,
    ) -> TokenStream {
        if self.pipe.is_none() {
            let input = syn::parse_str::<syn::Path>(&format!(
                "{}::{}",
                customizer.module_name, source_name
            ))
            .expect("chain_module");
            quote! {
                let input = #input(thread_control);
            }
        } else {
            import_scope.add_error_type();
            let input = format_ident!("input_{}", self.stream_index);
            let error_type = customizer.error_type.to_name();
            quote! {
                let input = {
                    let rx = thread_control.#input.take().expect("input {stream_index}");
                    datapet_support::iterator::sync::mpsc::Receive::<_, #error_type>::new(rx)
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
    pub fn new_thread(
        &mut self,
        name: FullyQualifiedName,
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
                    pipe: None,
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

    pub fn update_thread_single_stream(&mut self, thread_id: usize, stream: &NodeStream) {
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
                pipe: None,
            },
        );
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

    pub fn get_thread_id_and_module_by_source(
        &mut self,
        source: &NodeStreamSource,
        new_thread_name: &FullyQualifiedName,
    ) -> ChainSourceThread {
        let source_thread = self.get_source_thread(source);
        let thread_id = source_thread.thread_id;
        let thread = &self.threads[thread_id];
        if let Some(output_pipes) = &thread.output_pipes {
            let input_pipe = output_pipes[source_thread.stream_index];
            let streams = Box::new([thread.output_streams[source_thread.stream_index].clone()]);
            let thread_id = self.new_thread(
                new_thread_name.clone(),
                streams.clone(),
                streams,
                Some(Box::new([input_pipe])),
                true,
                None,
            );
            ChainSourceThread {
                thread_id,
                stream_index: 0,
                pipe: Some(input_pipe),
            }
        } else {
            source_thread.clone()
        }
    }

    fn new_pipe(&mut self) -> usize {
        let pipe = self.pipe_count;
        self.pipe_count = pipe + 1;
        pipe
    }

    pub fn pipe_single_thread(&mut self, source: &NodeStreamSource) -> usize {
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
            import_scope.add_import_with_error_type("fallible_iterator", "FallibleIterator");

            {
                let error_type = self.customizer.error_type.to_name();

                let input = source_thread.format_input(source, self.customizer, &mut import_scope);

                let pipe_def = quote! {
                    pub fn datapet_pipe(mut thread_control: ThreadControl) -> impl FnOnce() -> Result<(), #error_type> {
                        move || {
                            let tx = thread_control.output_0.take().expect("output 0");
                            #input
                            let mut input = input;
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
            thread.main = Some(FullyQualifiedName::new(name).sub("datapet_pipe"));
        }
        import_scope.import(scope, self.customizer);
        pipe
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
            let struct_def = quote! {
                pub struct ThreadControl {
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

            let thread_controls = self.threads.iter().map(|thread| {
                let thread_control = format_ident!("thread_control_{}", thread.id);
                let thread_module = format_ident!("thread_{}", thread.id);
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
                quote! {
                    let #thread_control = #thread_module::ThreadControl {
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
                quote! { let #join_thread = std::thread::spawn(#thread_main(#thread_control)); }
            });

            let join_threads = self.threads.iter().map(|thread| {
                let join_thread = format_ident!("join_{}", thread.id);
                quote! { #join_thread.join().unwrap()?; }
            });

            let main_def = quote! {
                pub fn main() -> Result<(), #error_type> {
                    #(#channels)*

                    #(#thread_controls)*

                    #(#spawn_threads)*

                    #(#join_threads)*

                    Ok(())
                }
            };
            self.scope.raw(&main_def.to_string());
        }
    }

    pub fn get_or_new_module_scope<'i>(
        &mut self,
        path: impl IntoIterator<Item = &'i Box<str>>,
        chain_customizer: &ChainCustomizer,
        thread_id: usize,
    ) -> &mut Scope {
        let mut iter = path.into_iter();
        let customize_module = |module: &mut Module| {
            for (path, ty) in &chain_customizer.custom_module_imports {
                module.import(path, ty);
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
}

pub const DEFAULT_CHAIN_ROOT_MODULE_NAME: [&str; 2] = ["crate", "chain"];
pub const DEFAULT_CHAIN_STREAMS_MODULE_NAME: &str = "streams";
pub const DEFAULT_CHAIN_ERROR_TYPE: [&str; 2] = ["datapet_support", "DatapetError"];

pub struct ChainCustomizer {
    pub streams_module_name: FullyQualifiedName,
    pub module_name: FullyQualifiedName,
    pub custom_module_imports: Vec<(String, String)>,
    pub error_type: FullyQualifiedName,
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
        }
    }
}

#[derive(Default)]
pub struct ImportScope {
    fixed: Vec<(String, String)>,
    import_error_type: bool,
    used: bool,
}

impl ImportScope {
    pub fn add_import(&mut self, path: &str, ty: &str) {
        self.fixed.push((path.to_string(), ty.to_string()));
    }

    pub fn add_import_with_error_type(&mut self, path: &str, ty: &str) {
        self.add_import(path, ty);
        self.add_error_type();
    }

    pub fn add_error_type(&mut self) {
        self.import_error_type = true;
    }

    pub fn import(mut self, scope: &mut Scope, customizer: &ChainCustomizer) {
        for (path, ty) in &self.fixed {
            scope.import(path, ty);
        }
        if self.import_error_type {
            scope.import(&customizer.error_type_path(), &customizer.error_type_name());
        }
        self.used = true;
    }
}

impl Drop for ImportScope {
    fn drop(&mut self) {
        assert!(self.used);
    }
}

pub fn fn_body<T: ToString>(body: T, the_fn: &mut Function) {
    for line in body.to_string().split('\n') {
        the_fn.line(line);
    }
}
