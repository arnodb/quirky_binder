mod utils;

use datapet_codegen::{parse_and_generate_module, CodegenError, ErrorEmitter};
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

struct WasmErrorEmitter {
    errors: Vec<Error>,
}

impl WasmErrorEmitter {
    fn is_dirty(&self) -> bool {
        !self.errors.is_empty()
    }

    fn handle_codegen_result<T>(mut self, result: Result<T, CodegenError>) -> Result<T, WasmError> {
        match result {
            Ok(res) => {
                assert!(!self.is_dirty());
                Ok(res)
            }
            Err(CodegenError::ErrorEmitted) => {
                assert!(self.is_dirty());
                Err(WasmError {
                    errors: self.errors,
                })
            }
            Err(CodegenError::Error(error)) => {
                self.emit_error(error.into());
                assert!(self.is_dirty());
                Err(WasmError {
                    errors: self.errors,
                })
            }
        }
    }
}

impl ErrorEmitter for WasmErrorEmitter {
    fn emit_error(&mut self, error: Cow<str>) -> CodegenError {
        self.errors.push(Error {
            error: error.into_owned(),
            span: Span { start: 0, end: 0 },
        });
        CodegenError::ErrorEmitted
    }

    fn emit_dtpt_error(&mut self, src: &str, part: &str, error: Cow<str>) {
        self.errors.push(Error {
            error: error.into_owned(),
            span: span_for_input_and_part(src, part),
        });
    }

    fn error(&mut self) -> Result<(), CodegenError> {
        if self.is_dirty() {
            Err(CodegenError::ErrorEmitted)
        } else {
            Ok(())
        }
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
const SOURCE_FILE: &str = "input";

#[wasm_bindgen]
pub fn dtpt(input: &str) -> Result<String, WasmError> {
    utils::set_panic_hook();
    let mut error_emitter = WasmErrorEmitter { errors: Vec::new() };
    let result = parse_and_generate_module(
        input,
        Some(SOURCE_FILE),
        &format_ident!("datapet"),
        &mut error_emitter,
    );
    let tokens = error_emitter.handle_codegen_result(result)?;
    Ok(tokens.to_string())
}
