use datapet_lang::location::{Location, Span};
use gen::{codegen_parse_module, generate_module};
use itertools::Itertools;
use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};
use std::{borrow::Cow, collections::BTreeMap, ops::Deref, path::Path};

mod gen;

#[derive(PartialEq, Eq, Debug)]
pub enum CodegenError {
    ErrorEmitted,
    Error(String),
}

pub trait ErrorEmitter {
    fn emit_error(&mut self, error: Cow<str>) -> CodegenError;

    fn emit_dtpt_error(&mut self, src: &str, part: &str, error: Cow<str>);

    fn error(&mut self) -> Result<(), CodegenError>;
}

struct DtptErrorEmitter<'a> {
    src: &'a str,
    source_file: Option<&'a str>,
    error_emitter: &'a mut dyn ErrorEmitter,
}

impl<'a> DtptErrorEmitter<'a> {
    fn emit_error(&mut self, part: &str, error: Cow<str>) -> CodegenError {
        self.error_emitter.emit_dtpt_error(self.src, part, error);
        CodegenError::ErrorEmitted
    }

    fn error(&mut self) -> Result<(), CodegenError> {
        self.error_emitter.error()
    }

    fn source_file(&self) -> Option<&'a str> {
        self.source_file
    }

    fn part_to_location(&self, part: &'a str) -> Location {
        let span = Span::span_of_str(self.src, part);
        Location::from_source_and_span(self.src, span)
    }
}

pub fn parse_and_generate_module(
    src: &str,
    source_file: Option<&str>,
    datapet_crate: &Ident,
    error_emitter: &mut dyn ErrorEmitter,
) -> Result<TokenStream, CodegenError> {
    let mut dtpt_error_emitter = DtptErrorEmitter {
        src,
        source_file,
        error_emitter,
    };
    let module = codegen_parse_module(src, &mut dtpt_error_emitter)?;
    generate_module(&module, datapet_crate, &mut dtpt_error_emitter)
}

struct ModuleConfiguration {
    test: bool,
}

#[derive(Default)]
struct ModuleCode {
    code: Option<TokenStream>,
    sub_modules: BTreeMap<String, ModuleCode>,
}

impl ModuleCode {
    fn add(&mut self, path: &[String], path_index: usize, code: TokenStream) {
        match path.len() - path_index {
            0 => {
                if self.code.is_none() {
                    self.code = Some(code);
                } else {
                    panic!("module {} already exists", path.iter().join("::"));
                }
            }
            _ => self
                .sub_modules
                .entry(path[path_index].clone())
                .or_insert_with(ModuleCode::default)
                .add(path, path_index + 1, code),
        }
    }

    fn generate(
        &self,
        path: &[&str],
        config: &ModuleConfiguration,
    ) -> Result<TokenStream, CodegenError> {
        let code = &self.code;
        let main_generate = code.as_ref().map(|_| {
            quote! {
                let graph = dtpt_main(new_graph_builder(&[#(#path),*]))?;

                graph.generate(out_dir)?;
            }
        });
        let all_chains_generate = self.generate_all_chains(path.last().map(Deref::deref), config);
        let sub_modules = self
            .sub_modules
            .iter()
            .map(|(name, sub_module)| {
                let name_ident = format_ident!("{}", name);
                let sub_path = {
                    let mut vec = path.to_vec();
                    vec.push(name);
                    vec
                };
                let sub_code = sub_module.generate(&sub_path, config)?;
                Ok(quote! {
                    pub mod #name_ident {
                        use datapet::prelude::*;
                        use std::fs::File;
                        use std::io::Write;
                        use std::path::{Path, PathBuf};
                        use truc::record::type_resolver::{StaticTypeResolver, TypeResolver};

                        #sub_code
                    }
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let sub_generates = self
            .sub_modules
            .keys()
            .map(|name| {
                let name_ident = format_ident!("{}", name);
                Ok(quote! {
                    {
                        let sub_out_dir = Path::new(out_dir).join(#name);
                        std::fs::create_dir_all(&sub_out_dir)?;
                        #name_ident::dtpt_generate_deep(sub_out_dir.as_path(), new_graph_builder)?;
                    }
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(quote! {
            #code

            pub fn dtpt_generate_deep<R, NGB>(
                out_dir: &Path,
                new_graph_builder: NGB,
            ) -> Result<(), GraphGenerationError>
            where
                R: TypeResolver + Copy,
                NGB: Fn(&[&str]) -> GraphBuilder<R> + Copy,
            {
                #main_generate

                #all_chains_generate

                #(
                    #sub_generates

                )*

                Ok(())
            }

            #(
                #sub_modules

            )*
        })
    }

    fn generate_all_chains(&self, name: Option<&str>, config: &ModuleConfiguration) -> TokenStream {
        let local = self.code.as_ref().map(|_| {
            let test_name = name.map_or_else(
                || Cow::Borrowed("test"),
                |name| format!("test_{}", name).into(),
            );
            let test_local = config.test.then(|| {
                quote! {
                    writeln!(file, r#"
    #[test]
    fn {}() -> Result<(), DatapetError> {{
       main(ChainConfiguration::default())
    }}
"#, #test_name)?;
                }
            });
            quote! {
                writeln!(file, stringify!(include!("chain.rs");))?;

                #test_local
            }
        });
        let subs = self.sub_modules.keys().map(|sub_name| {
            let sub_path = Path::new(sub_name)
                .join("all_chains.rs")
                .to_string_lossy()
                .to_string();
            // Hack
            let allow_unused_imports = match sub_name.as_str() {
                "void" => Some(quote! {
                    writeln!(file, "#[allow(unused_imports)]")?;
                }),
                _ => None,
            };
            quote! {
                #allow_unused_imports
                writeln!(file, "mod {} {{", #sub_name)?;
                writeln!(file, stringify!(include!(#sub_path);))?;
                writeln!(file, "}}")?;
            }
        });
        quote! {{
            let mut file = File::create(out_dir.join("all_chains.rs"))?;

            #local

            #(#subs)*
        }}
    }
}

pub fn parse_and_generate_glob_modules(
    src: &str,
    pattern: &str,
    datapet_crate: &Ident,
    test: bool,
    error_emitter: &mut dyn ErrorEmitter,
) -> Result<TokenStream, CodegenError> {
    let prefix = {
        let path =
            Path::new(&std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR")).join(src);
        path
    };
    let absolute_pattern = {
        let mut path = prefix.clone();
        path.push(pattern);
        path
    };
    let files = glob::glob(&absolute_pattern.to_string_lossy())
        .map_err(|err| error_emitter.emit_error(format!("{}", err).into()))?;
    let mut tree = ModuleCode::default();
    let mut error = None;
    let mut file_count = 0;
    for file in files {
        let file =
            file.map_err(|err| error_emitter.emit_error(format!("Glob error {}", err).into()))?;

        let mut module_components = file
            .strip_prefix(&prefix)
            .map_err(|err| CodegenError::Error(err.to_string()))?
            .components()
            .map(|cmp| {
                let cmp = cmp.as_os_str().to_string_lossy();
                let mut last_underscore = false;
                cmp.chars()
                    .filter_map(|c| {
                        if c.is_alphanumeric() {
                            last_underscore = false;
                            Some(c)
                        } else if last_underscore {
                            None
                        } else {
                            last_underscore = true;
                            Some('_')
                        }
                    })
                    .collect::<String>()
            })
            .collect::<Vec<_>>();
        if module_components.iter().any(|cmp| cmp == "chain") {
            let error = error_emitter.emit_error("\"chain\" is reserved".into());
            return Err(error);
        }
        {
            let last_cmp = module_components.last_mut().unwrap();
            const SUFFIX: &str = "_dtpt";
            if last_cmp.ends_with(SUFFIX) {
                last_cmp.truncate(last_cmp.len() - SUFFIX.len())
            }
        }

        let src =
            std::fs::read_to_string(&file).map_err(|err| CodegenError::Error(err.to_string()))?;
        match parse_and_generate_module(
            &src,
            Some(&file.to_string_lossy()),
            datapet_crate,
            error_emitter,
        ) {
            Ok(code) => {
                tree.add(&module_components, 0, code);
            }
            Err(err) => match error {
                None | Some(CodegenError::ErrorEmitted) => {
                    error = Some(err);
                }
                Some(CodegenError::Error(_)) => {}
            },
        }

        file_count += 1;
    }
    if file_count == 0 {
        let error = error_emitter.emit_error(format!("Pattern {} did not match", pattern).into());
        return Err(error);
    }
    if let Some(err) = error {
        return Err(err);
    }
    let config = ModuleConfiguration { test };
    tree.generate(&[], &config)
}
