use annotate_snippets::display_list::DisplayList;
use datapet_codegen::{codegen_parse_module, ParseError};
use datapet_lang::snippet::snippet_for_input_and_part;
use proc_macro2::Ident;
use proc_macro_error::{abort_if_dirty, emit_error, proc_macro_error};
use quote::format_ident;
use std::borrow::Cow;
use syn::{
    parse::{ParseStream, Parser},
    Error, LitStr,
};

struct ProcMacroErrorEmitter<'a> {
    def_type: &'a Ident,
    input: &'a str,
}

impl<'a> datapet_codegen::ErrorEmitter for ProcMacroErrorEmitter<'a> {
    fn emit_error(&mut self, part: &str, error: Cow<str>) {
        emit_error!(
            self.def_type,
            "{}",
            DisplayList::from(snippet_for_input_and_part(&error, self.input, part))
        );
    }
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
    let mut error_emitter = ProcMacroErrorEmitter {
        def_type: &def_type,
        input: &input,
    };
    let module = match codegen_parse_module(&input, &mut error_emitter) {
        Ok(res) => res,
        Err(ParseError) => {
            abort_if_dirty();
            unreachable!("should be aborted");
        }
    };
    datapet_codegen::generate_module(&module, &datapet_crate, &mut error_emitter).into()
}
