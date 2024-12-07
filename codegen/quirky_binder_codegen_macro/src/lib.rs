use std::borrow::Cow;

use annotate_snippets::display_list::DisplayList;
use proc_macro2::Ident;
use proc_macro_error::{abort_if_dirty, emit_error, proc_macro_error};
use quirky_binder_codegen::{
    parse_and_generate_file_module, parse_and_generate_glob_modules, parse_and_generate_module,
    CodegenError, ErrorEmitter,
};
use quirky_binder_lang::snippet::snippet_for_input_and_part;
use quote::format_ident;
use syn::{
    parenthesized,
    parse::{ParseStream, Parser},
    token, Error, LitStr,
};

mod decorators;

struct ProcMacroErrorEmitter<'a> {
    span: &'a Ident,
    dirty: bool,
}

impl<'a> ProcMacroErrorEmitter<'a> {
    fn handle_codegen_result<T>(&mut self, result: Result<T, CodegenError>) -> T {
        match result {
            Ok(res) => {
                assert!(!self.dirty);
                res
            }
            Err(CodegenError::ErrorEmitted) => {
                assert!(self.dirty);
                abort_if_dirty();
                unreachable!("should be aborted");
            }
            Err(CodegenError::Error(error)) => {
                self.emit_error(error.into());
                assert!(self.dirty);
                abort_if_dirty();
                unreachable!("should be aborted");
            }
        }
    }
}

impl<'a> ErrorEmitter for ProcMacroErrorEmitter<'a> {
    fn emit_error(&mut self, error: Cow<str>) -> CodegenError {
        emit_error!(self.span, "{}", error);
        self.dirty = true;
        CodegenError::ErrorEmitted
    }

    fn emit_quirky_binder_error(&mut self, src: &str, part: &str, error: Cow<str>) {
        emit_error!(
            self.span,
            "{}",
            DisplayList::from(snippet_for_input_and_part(&error, src, part))
        );
        self.dirty = true;
    }

    fn error(&mut self) -> Result<(), quirky_binder_codegen::CodegenError> {
        if self.dirty {
            Err(CodegenError::ErrorEmitted)
        } else {
            Ok(())
        }
    }
}

#[derive(PartialEq, Eq, Clone, Debug)]
enum Definition {
    Inline {
        qb: String,
    },
    Include {
        file: String,
    },
    IncludeGlob {
        src: String,
        pattern: String,
        test: bool,
    },
}

const INLINE_DEFINITION: &str = "inline";
const INCLUDE_DEFINITION: &str = "include";
const INCLUDE_GLOB_DEFINITION: &str = "include_glob";
const INCLUDE_GLOB_TEST_DEFINITION: &str = "include_glob_test";

fn parse_def(input: ParseStream) -> Result<(Definition, Ident), Error> {
    let def_type: Ident = input.parse()?;
    let def = match def_type.to_string().as_str() {
        INLINE_DEFINITION => {
            let content;
            parenthesized!(content in input);
            let qb: LitStr = content.parse()?;
            Definition::Inline { qb: qb.value() }
        }
        INCLUDE_GLOB_DEFINITION => {
            let content;
            parenthesized!(content in input);
            Definition::parse_include_glob(&content, false)?
        }
        INCLUDE_DEFINITION => {
            let content;
            parenthesized!(content in input);
            Definition::parse_include(&content)?
        }
        INCLUDE_GLOB_TEST_DEFINITION => {
            let content;
            parenthesized!(content in input);
            Definition::parse_include_glob(&content, true)?
        }
        _ => {
            return Err(Error::new(
                def_type.span(),
                format!(
                    "expected [{}]",
                    [
                        INLINE_DEFINITION,
                        INCLUDE_DEFINITION,
                        INCLUDE_GLOB_DEFINITION
                    ]
                    .join(", ")
                ),
            ));
        }
    };
    Ok((def, def_type))
}

impl Definition {
    fn parse_include(content: ParseStream) -> Result<Definition, Error> {
        let file: LitStr = content.parse()?;
        Ok(Definition::Include { file: file.value() })
    }

    fn parse_include_glob(content: ParseStream, test: bool) -> Result<Definition, Error> {
        let src: LitStr = content.parse()?;
        content.parse::<token::Comma>()?;
        let pattern: LitStr = content.parse()?;
        Ok(Definition::IncludeGlob {
            src: src.value(),
            pattern: pattern.value(),
            test,
        })
    }
}

fn quirky_binder_1(
    input: proc_macro::TokenStream,
    quirky_binder_crate: &Ident,
) -> proc_macro::TokenStream {
    let (def, span) = match Parser::parse(parse_def, input) {
        Ok(res) => res,
        Err(err) => {
            abort_if_dirty();
            return err.into_compile_error().into();
        }
    };

    let mut error_emitter = ProcMacroErrorEmitter {
        span: &span,
        dirty: false,
    };

    match def {
        Definition::Inline { qb } => {
            let result =
                parse_and_generate_module(&qb, None, quirky_binder_crate, &mut error_emitter);
            error_emitter.handle_codegen_result(result).into()
        }
        Definition::Include { file } => {
            let result =
                parse_and_generate_file_module(&file, quirky_binder_crate, &mut error_emitter);
            error_emitter.handle_codegen_result(result).into()
        }
        Definition::IncludeGlob { src, pattern, test } => {
            let result = parse_and_generate_glob_modules(
                &src,
                &pattern,
                quirky_binder_crate,
                test,
                &mut error_emitter,
            );
            error_emitter.handle_codegen_result(result).into()
        }
    }
}

#[proc_macro]
#[proc_macro_error]
pub fn quirky_binder(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    quirky_binder_1(input, &format_ident!("quirky_binder"))
}

#[proc_macro]
#[proc_macro_error]
pub fn quirky_binder_internal(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    quirky_binder_1(input, &format_ident!("crate"))
}

#[proc_macro]
pub fn tracking_allocator_static(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    decorators::tracking_allocator_static(input)
}

#[proc_macro_attribute]
pub fn tracking_allocator_main(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    decorators::tracking_allocator_main(attr, item)
}
