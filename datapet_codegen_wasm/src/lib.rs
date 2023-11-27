mod utils;

use datapet_codegen::{codegen_parse_module, ParseError};
use quote::format_ident;
use serde::Serialize;
use std::borrow::Cow;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
extern "C" {
    fn alert(s: &str);
}

fn slice_spans<'a>(input: &'a str, part: &'a str) -> (usize, usize) {
    let (lo, hi) = {
        let a = input.as_ptr();
        let b = part.as_ptr();
        if a <= b {
            (
                b as usize - a as usize,
                (b as usize - a as usize + part.len()).min(input.len()),
            )
        } else {
            panic!("not a part of input {:?} {:?}", a, b);
        }
    };
    (lo, hi)
}

#[derive(Serialize, Debug)]
struct Span {
    start: usize,
    end: usize,
}

#[derive(Serialize, Debug)]
struct Error {
    error: String,
    span: Span,
}

fn span_for_input_and_part<'a>(input: &'a str, part: &'a str) -> Span {
    let (lo, hi) = slice_spans(input, part);
    Span { start: lo, end: hi }
}

struct WasmErrorEmitter<'a> {
    input: &'a str,
    errors: Vec<Error>,
}

impl<'a> datapet_codegen::ErrorEmitter for WasmErrorEmitter<'a> {
    fn emit_error(&mut self, part: &str, error: Cow<str>) {
        self.errors.push(Error {
            error: error.into_owned(),
            span: span_for_input_and_part(self.input, part),
        });
    }
}

#[derive(Serialize, Debug)]
pub struct WasmError {
    errors: Vec<Error>,
}

#[allow(clippy::from_over_into)]
impl Into<JsValue> for WasmError {
    fn into(self) -> JsValue {
        serde_wasm_bindgen::to_value(&self).expect("js value")
    }
}

#[wasm_bindgen]
pub fn dtpt(input: &str) -> Result<String, WasmError> {
    utils::set_panic_hook();
    let mut error_emitter = WasmErrorEmitter {
        input,
        errors: Vec::new(),
    };
    let module = match codegen_parse_module(input, &mut error_emitter) {
        Ok(res) => res,
        Err(ParseError) => {
            return Err(WasmError {
                errors: error_emitter.errors,
            });
        }
    };
    let tokens =
        datapet_codegen::generate_module(&module, &format_ident!("datapet"), &mut error_emitter);

    if !error_emitter.errors.is_empty() {
        return Err(WasmError {
            errors: error_emitter.errors,
        });
    }

    Ok(tokens.to_string())
}
