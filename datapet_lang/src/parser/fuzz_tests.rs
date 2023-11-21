//! These tests do NOT guarantee that the parser implemented with `nom` is correct.
//!
//! They check that any data produced with the `nom` parsing model can be parsed with the defined
//! parser. It brings good confidence in our ability to transform some non LR grammars to LR
//! grammars. Which is already a good thing.

use annotate_snippets::display_list::DisplayList;
use antinom::rng::{AnarchyLevel, AntiNomRandRng};
use rand_chacha::rand_core::SeedableRng;

use crate::{parser::SpannedErrorKind, snippet::snippet_for_input_and_part};

use super::{fuzzer, SpannedError};

trait ToSpannedError<'a> {
    fn to_spanned_error(&self, input: &'a str) -> SpannedError<&'a str>;
}

impl<'a> ToSpannedError<'a> for SpannedError<&'a str> {
    fn to_spanned_error(&self, _input: &'a str) -> SpannedError<&'a str> {
        self.clone()
    }
}

impl<'a> ToSpannedError<'a> for nom::Err<SpannedError<&'a str>> {
    fn to_spanned_error(&self, input: &'a str) -> SpannedError<&'a str> {
        match self {
            nom::Err::Incomplete(_) => SpannedError {
                kind: SpannedErrorKind::NomIncomplete,
                span: &input[input.len()..],
            },
            nom::Err::Error(err) | nom::Err::Failure(err) => err.clone(),
        }
    }
}

const FUZZ_ITERATIONS: usize = 1000;

macro_rules! fuzz_test {
    ($seed: expr, $fuzzer:path, $parser:path, $anarchy_level:path) => {
        for _ in 0..({
            if $seed.is_none() {
                FUZZ_ITERATIONS
            } else {
                1
            }
        }) {
            let mut rng = AntiNomRandRng {
                rng: if let Some(seed) = $seed {
                    rand_chacha::ChaCha8Rng::from_seed(seed)
                } else {
                    rand_chacha::ChaCha8Rng::from_entropy()
                },
                anarchy_level: $anarchy_level,
            };
            println!("Seed: {:#04x?}", rng.rng.get_seed());
            let mut dtpt = String::new();
            $fuzzer(&mut rng, &mut dtpt);
            if let Err(err) = $parser(&dtpt) {
                let err = err.to_spanned_error(&dtpt);
                eprintln!(
                    "{}",
                    DisplayList::from(snippet_for_input_and_part(
                        &err.kind.description(),
                        &dtpt,
                        err.span
                    ))
                );
                panic!("parse error {}", err.kind.description());
            }
        }
    };
}

#[allow(unused)]
macro_rules! fuzz_test_compare {
    ($seed: expr, $fuzzer:path, $parser:expr, $parser2:expr, $anarchy_level:path) => {
        for _ in 0..({
            if $seed.is_none() {
                FUZZ_ITERATIONS
            } else {
                1
            }
        }) {
            let mut rng = AntiNomRandRng {
                rng: if let Some(seed) = $seed {
                    rand_chacha::ChaCha8Rng::from_seed(seed)
                } else {
                    rand_chacha::ChaCha8Rng::from_entropy()
                },
                anarchy_level: $anarchy_level,
            };
            println!("Seed: {:#04x?}", rng.rng.get_seed());
            let mut dtpt = String::new();
            $fuzzer(&mut rng, &mut dtpt);
            let dtpt_trimmed =
                dtpt.trim_matches(|c| c == ' ' || c == '\t' || c == '\n' || c == '\r');
            let res = $parser(dtpt_trimmed).map_err(|err| err.to_spanned_error(&dtpt));
            let res2 = $parser2(&dtpt_trimmed).map_err(|err| err.to_spanned_error(&dtpt));
            if res != res2 {
                if let Err(err) = &res {
                    let err = err.to_spanned_error(&dtpt);
                    eprintln!(
                        "{}",
                        DisplayList::from(snippet_for_input_and_part(
                            &err.kind.description(),
                            &dtpt,
                            err.span
                        ))
                    );
                }
                if let Err(err2) = &res2 {
                    let err2 = err2.to_spanned_error(&dtpt);
                    eprintln!(
                        "{}",
                        DisplayList::from(snippet_for_input_and_part(
                            &err2.kind.description(),
                            &dtpt,
                            err2.span
                        ))
                    );
                }
            }
            assert_eq!(res, res2);
        }
    };
}

#[test]
fn fuzz_use_declaration() {
    let seed = None;
    let fuzzer = fuzzer::use_declaration;

    let nom_parser = crate::parser::nom_impl::use_declaration;
    fuzz_test!(seed, fuzzer, nom_parser, AnarchyLevel::LawAndOrder);

    #[cfg(feature = "crafted_parser")]
    {
        use crate::{ast::UseDeclaration, parser::crafted_impl::lexer::lexer};

        fn crafted_parser(input: &str) -> Result<UseDeclaration, SpannedError<&str>> {
            let mut lexer = lexer(input);
            crate::parser::crafted_impl::use_declaration(&mut lexer)
        }

        fuzz_test!(seed, fuzzer, crafted_parser, AnarchyLevel::LawAndOrder);

        fuzz_test_compare!(
            seed,
            fuzzer,
            |input| nom_parser(input).map(|res| res.1),
            crafted_parser,
            AnarchyLevel::ALittleAnarchy
        );
    }
}

#[test]
fn fuzz_graph_definition_signature() {
    let seed = None;
    let fuzzer = fuzzer::graph_definition_signature;

    let nom_parser = crate::parser::nom_impl::graph_definition_signature;
    fuzz_test!(seed, fuzzer, nom_parser, AnarchyLevel::LawAndOrder);
}

#[test]
fn fuzz_opt_streams0() {
    let seed = None;
    let fuzzer = fuzzer::opt_streams0;

    let nom_parser = crate::parser::nom_impl::opt_streams0;
    fuzz_test!(seed, fuzzer, nom_parser, AnarchyLevel::LawAndOrder);

    #[cfg(feature = "crafted_parser")]
    {
        use crate::parser::crafted_impl::lexer::lexer;

        fn crafted_parser(input: &str) -> Result<Option<Vec<&str>>, SpannedError<&str>> {
            let mut lexer = lexer(input);
            crate::parser::crafted_impl::opt_streams0(&mut lexer)
        }

        fuzz_test!(seed, fuzzer, crafted_parser, AnarchyLevel::LawAndOrder);

        fuzz_test_compare!(
            seed,
            fuzzer,
            |input| nom_parser(input).map(|res| res.1),
            crafted_parser,
            AnarchyLevel::ALittleAnarchy
        );
    }
}

#[test]
fn fuzz_opt_streams1() {
    let seed = None;
    let fuzzer = fuzzer::opt_streams1;

    let nom_parser = crate::parser::nom_impl::opt_streams1;
    fuzz_test!(seed, fuzzer, nom_parser, AnarchyLevel::LawAndOrder);

    #[cfg(feature = "crafted_parser")]
    {
        use crate::parser::crafted_impl::lexer::lexer;

        fn crafted_parser(input: &str) -> Result<Option<Vec<&str>>, SpannedError<&str>> {
            let mut lexer = lexer(input);
            crate::parser::crafted_impl::opt_streams1(&mut lexer)
        }

        fuzz_test!(seed, fuzzer, crafted_parser, AnarchyLevel::LawAndOrder);

        fuzz_test_compare!(
            seed,
            fuzzer,
            |input| nom_parser(input).map(|res| res.1),
            crafted_parser,
            AnarchyLevel::ALittleAnarchy
        );
    }
}

#[test]
fn fuzz_code() {
    let seed = None;
    let fuzzer = fuzzer::code;

    let nom_parser = crate::parser::nom_impl::code;
    fuzz_test!(seed, fuzzer, nom_parser, AnarchyLevel::LawAndOrder);

    #[cfg(feature = "crafted_parser")]
    {
        use crate::parser::crafted_impl::lexer::lexer;

        fn crafted_parser(input: &str) -> Result<&str, SpannedError<&str>> {
            let mut lexer = lexer(input);
            crate::parser::crafted_impl::code(&mut lexer)
        }

        fuzz_test!(seed, fuzzer, crafted_parser, AnarchyLevel::LawAndOrder);

        fuzz_test_compare!(
            seed,
            fuzzer,
            |input| nom_parser(input).map(|res| res.1),
            crafted_parser,
            AnarchyLevel::ALittleAnarchy
        );
    }
}

#[test]
fn fuzz_simple_path() {
    let seed = None;
    let fuzzer = fuzzer::simple_path;

    let nom_parser = crate::parser::nom_impl::simple_path;
    fuzz_test!(seed, fuzzer, nom_parser, AnarchyLevel::LawAndOrder);

    #[cfg(feature = "crafted_parser")]
    {
        use crate::parser::crafted_impl::lexer::lexer;

        fn crafted_parser(input: &str) -> Result<&str, SpannedError<&str>> {
            let mut lexer = lexer(input);
            crate::parser::crafted_impl::simple_path(&mut lexer)
        }

        fuzz_test!(seed, fuzzer, crafted_parser, AnarchyLevel::LawAndOrder);

        fuzz_test_compare!(
            seed,
            fuzzer,
            |input| nom_parser(input).map(|res| res.1),
            crafted_parser,
            AnarchyLevel::ALittleAnarchy
        );
    }
}

#[test]
fn fuzz_identifier() {
    let seed = None;
    let fuzzer = fuzzer::identifier;

    let nom_parser = crate::parser::nom_impl::identifier;
    fuzz_test!(seed, fuzzer, nom_parser, AnarchyLevel::LawAndOrder);

    #[cfg(feature = "crafted_parser")]
    {
        use crate::parser::crafted_impl::lexer::lexer;

        fn crafted_parser(input: &str) -> Result<&str, SpannedError<&str>> {
            let mut lexer = lexer(input);
            crate::parser::crafted_impl::identifier(&mut lexer)
        }

        fuzz_test!(seed, fuzzer, crafted_parser, AnarchyLevel::LawAndOrder);

        fuzz_test_compare!(
            seed,
            fuzzer,
            |input| nom_parser(input).map(|res| res.1),
            crafted_parser,
            AnarchyLevel::ALittleAnarchy
        );
    }
}
