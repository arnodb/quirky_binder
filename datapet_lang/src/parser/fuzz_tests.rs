//! These tests do NOT guarantee that the parser implemented with `nom` is correct.
//!
//! They check that any data produced with the `nom` parsing model can be parsed with the defined
//! parser. It brings good confidence in our ability to transform some non LR grammars to LR
//! grammars. Which is already a good thing.
use annotate_snippets::display_list::DisplayList;
use antinom::rng::{AnarchyLevel, AntiNomRandRng};
use rand_chacha::rand_core::SeedableRng;
use rstest::rstest;

use crate::{ast::GraphDefinitionSignature, snippet::snippet_for_input_and_part};

use super::{fuzzer, SpannedResult};

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
                let error = match &err {
                    nom::Err::Incomplete(_) => "incomplete".into(),
                    nom::Err::Error(err) | nom::Err::Failure(err) => err.kind.description(),
                };
                let part = match &err {
                    nom::Err::Incomplete(_) => &dtpt,
                    nom::Err::Error(err) | nom::Err::Failure(err) => err.span,
                };
                eprintln!(
                    "{}",
                    DisplayList::from(snippet_for_input_and_part(&error, &dtpt, part))
                );
                panic!("parse error {}", error);
            }
        }
    };
}

#[rstest]
#[case(crate::parser::graph_definition_signature, None)]
fn fuzz_graph_definition_signature<F>(#[case] parser: F, #[case] seed: Option<[u8; 32]>)
where
    F: Fn(&str) -> SpannedResult<&str, GraphDefinitionSignature>,
{
    fuzz_test!(
        seed,
        fuzzer::graph_definition_signature,
        parser,
        AnarchyLevel::LawAndOrder
    );
}

#[rstest]
#[case(crate::parser::identifier, None)]
fn fuzz_identifier<F>(#[case] parser: F, #[case] seed: Option<[u8; 32]>)
where
    F: Fn(&str) -> SpannedResult<&str, &str>,
{
    fuzz_test!(seed, fuzzer::identifier, parser, AnarchyLevel::LawAndOrder);
}
