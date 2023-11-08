use annotate_snippets::display_list::DisplayList;
use antinom::rng::AntiNomRandRng;
use rand_chacha::rand_core::SeedableRng;
use rstest::rstest;

use crate::{ast::GraphDefinitionSignature, snippet::snippet_for_input_and_part};

use super::{fuzzer, SpannedResult};

const FUZZ_ITERATIONS: usize = 1000;

macro_rules! fuzz_test {
    ($seed: expr, $fuzzer:path, $parser:path) => {
        for _ in 0..FUZZ_ITERATIONS {
            let mut rng = AntiNomRandRng {
                rng: if let Some(seed) = $seed {
                    rand_chacha::ChaCha8Rng::from_seed(seed)
                } else {
                    rand_chacha::ChaCha8Rng::from_entropy()
                },
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
    fuzz_test!(seed, fuzzer::graph_definition_signature, parser);
}

#[rstest]
#[case(crate::parser::identifier, None)]
fn fuzz_identifier<F>(#[case] parser: F, #[case] seed: Option<[u8; 32]>)
where
    F: Fn(&str) -> SpannedResult<&str, &str>,
{
    fuzz_test!(seed, fuzzer::identifier, parser);
}
