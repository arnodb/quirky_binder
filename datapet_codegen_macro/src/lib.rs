use annotate_snippets::{
    display_list::{DisplayList, FormatOptions},
    snippet::{Annotation, AnnotationType, Slice, Snippet, SourceAnnotation},
};
use proc_macro2::Ident;
use proc_macro_error::{abort_if_dirty, emit_error, proc_macro_error};
use quote::format_ident;
use std::borrow::Cow;
use syn::{
    parse::{ParseStream, Parser},
    Error, LitStr,
};

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
        if a <= b {
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

fn snippet_for_input_and_part<'a>(label: &'a str, input: &'a str, part: &'a str) -> Snippet<'a> {
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
    let module = match datapet_codegen::parse_module(&input, &mut error_emitter) {
        Ok(res) => res,
        Err(()) => {
            abort_if_dirty();
            unreachable!("should be aborted");
        }
    };
    datapet_codegen::generate_module(&module, &datapet_crate, &mut error_emitter).into()
}
