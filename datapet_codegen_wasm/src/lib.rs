mod utils;

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

fn span_for_input_and_part<'a>(input: &'a str, part: &'a str) -> Span {
    let (lo, hi) = slice_spans(input, part);
    Span { start: lo, end: hi }
}

struct WasmErrorEmitter<'a> {
    input: &'a str,
    errors: Errors,
}

impl<'a> datapet_codegen::ErrorEmitter for WasmErrorEmitter<'a> {
    fn emit_error(&mut self, part: &str, error: Cow<str>) {
        self.errors.0.push(Error {
            error: error.into_owned(),
            span: span_for_input_and_part(self.input, part),
        });
    }
}

#[derive(Serialize, Debug)]
pub struct Errors(Vec<Error>);

#[derive(Serialize, Debug)]
struct Error {
    error: String,
    span: Span,
}

#[allow(clippy::from_over_into)]
impl Into<JsValue> for Errors {
    fn into(self) -> JsValue {
        serde_wasm_bindgen::to_value(&self).expect("js value")
    }
}

#[wasm_bindgen]
pub fn dtpt(input: &str) -> Result<String, Errors> {
    utils::set_panic_hook();
    let mut error_emitter = WasmErrorEmitter {
        input,
        errors: Errors(Vec::new()),
    };
    let module = match datapet_codegen::parse_module(input, &mut error_emitter) {
        Ok(res) => res,
        Err(()) => {
            return Err(error_emitter.errors);
        }
    };
    let tokens =
        datapet_codegen::generate_module(&module, &format_ident!("datapet"), &mut error_emitter);

    if !error_emitter.errors.0.is_empty() {
        return Err(error_emitter.errors);
    }

    Ok(tokens.to_string())
}
