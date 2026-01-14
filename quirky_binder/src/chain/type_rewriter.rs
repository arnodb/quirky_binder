use syn::{visit_mut::VisitMut, Path, PathArguments, PathSegment, Type, TypePath};

use crate::{chain::StreamCustomizer, graph::node::DynNode};

#[derive(new)]
pub(crate) struct StreamsRewriter<'a, SC>
where
    SC: StreamCustomizer,
{
    node: &'a dyn DynNode,
    customizer: &'a SC,
}

impl<'a, SC> StreamsRewriter<'a, SC>
where
    SC: StreamCustomizer,
{
    fn maybe_prefix<'b>(input: &'b str, prefix: &'b str) -> (bool, &'b str) {
        input
            .strip_prefix(prefix)
            .map(|tail| (true, tail))
            .unwrap_or((false, input))
    }

    fn rewrite_ident(ident: &syn::Ident, node: &dyn DynNode, customizer: &SC) -> Option<syn::Type> {
        let name = ident.to_string();

        let (unpacked, tail) = Self::maybe_prefix(&name, "Unpacked");

        let (streams, is_output, tail) = if let Some(tail) = tail.strip_prefix("Input") {
            (node.inputs(), false, tail)
        } else if let Some(tail) = tail.strip_prefix("Output") {
            (node.outputs(), true, tail)
        } else {
            return None;
        };

        let (is_in, tail) = Self::maybe_prefix(tail, "In");

        let (index, tail) = {
            let end = tail
                .find(|c: char| !c.is_ascii_digit())
                .unwrap_or(tail.len());
            if let Ok(index) = tail[0..end].parse::<usize>() {
                (index, &tail[end..])
            } else {
                return None;
            }
        };

        let (and_unpacked_out, tail) = Self::maybe_prefix(tail, "AndUnpackedOut");

        if !tail.is_empty() {
            return None;
        }

        let stream = streams.get(index);
        stream.and_then(|stream| {
            let fragments = customizer.stream_definition_fragments(stream);
            match (unpacked, is_output, is_in, and_unpacked_out) {
                // <Input|Output><X>
                (false, _, false, false) => Some(fragments.record()),
                // Unpacked<Input|Output><X>
                (true, _, false, false) => Some(fragments.unpacked_record()),
                // UnpackedOutputIn<X>
                (true, true, true, false) => Some(fragments.unpacked_record_in()),
                // <Input|Output><X>AndUnpackedOut
                (false, _, false, true) => Some(fragments.record_and_unpacked_out()),
                _ => None,
            }
        })
    }
}

impl<'a, SC> VisitMut for StreamsRewriter<'a, SC>
where
    SC: StreamCustomizer,
{
    fn visit_path_mut(&mut self, i: &mut Path) {
        let Path {
            leading_colon,
            segments,
        } = i;
        if leading_colon.is_some() {
            return;
        }
        let mut seg_iter = segments.iter();
        if let Some(o) = seg_iter
            .next()
            .and_then(|PathSegment { ident, arguments }| {
                if arguments != &PathArguments::None {
                    return None;
                }
                Self::rewrite_ident(ident, self.node, self.customizer).and_then(|new_path_prefix| {
                    if let Type::Path(TypePath { mut path, .. }) = new_path_prefix {
                        path.segments.extend(seg_iter.cloned());
                        Some(path)
                    } else {
                        None
                    }
                })
            })
        {
            *i = o;
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::{collections::BTreeMap, fmt::Write};

    use super::*;
    use crate::{chain::ChainCustomizer, prelude::*};

    #[test]
    fn fuzz_rewrite_ident() {
        use rand::Rng;
        use rand_chacha::rand_core::SeedableRng;

        #[derive(Debug)]
        struct TestNode {
            name: FullyQualifiedName,
            inputs: Vec<NodeStream>,
            outputs: Vec<NodeStream>,
        }

        const MAX_STREAMS: usize = 42;
        const ITERATIONS: usize = 10000;

        impl TestNode {
            fn new() -> Self {
                Self {
                    name: FullyQualifiedName::new("foo"),
                    inputs: (0..MAX_STREAMS)
                        .map(|i| {
                            NodeStream::new(
                                FullyQualifiedName::new("input_record_type").into(),
                                (i * 3).into(),
                                BTreeMap::new(),
                                FullyQualifiedName::new("source").into(),
                                true,
                                StreamFacts::default(),
                            )
                        })
                        .collect(),
                    outputs: (0..MAX_STREAMS)
                        .map(|i| {
                            NodeStream::new(
                                FullyQualifiedName::new("output_record_type").into(),
                                (i * 7).into(),
                                BTreeMap::new(),
                                FullyQualifiedName::new("source").into(),
                                true,
                                StreamFacts::default(),
                            )
                        })
                        .collect(),
                }
            }
        }

        impl DynNode for TestNode {
            fn name(&self) -> &FullyQualifiedName {
                &self.name
            }

            fn inputs(&self) -> &[NodeStream] {
                &self.inputs
            }

            fn outputs(&self) -> &[NodeStream] {
                &self.outputs
            }

            fn gen_chain(&self, _graph: &Graph, _chain: &mut Chain) {
                unreachable!();
            }
        }

        let mut rng = rand_chacha::ChaCha8Rng::from_entropy();
        println!("Seed: {:02x?}", rng.get_seed());

        let chain_customizer = ChainCustomizer::default();
        let node = TestNode::new();

        let mut valid = 0;

        for _ in 0..ITERATIONS {
            let unpacked: bool = rng.gen();
            let is_output: bool = rng.gen();
            let is_in: bool = rng.gen();
            let number: Option<usize> = rng
                .gen::<bool>()
                .then(|| rng.gen_range(0..MAX_STREAMS + 12));
            let and_unpacked_out: bool = rng.gen();
            let extra: bool = rng.gen();

            let name = {
                let mut name = String::new();

                if unpacked {
                    name.write_str("Unpacked").unwrap();
                }

                if !is_output {
                    name.write_str("Input").unwrap();
                } else {
                    name.write_str("Output").unwrap();
                }

                if is_in {
                    name.write_str("In").unwrap();
                }

                if let Some(number) = number {
                    write!(name, "{}", number).unwrap();
                }

                if and_unpacked_out {
                    name.write_str("AndUnpackedOut").unwrap();
                }

                if extra {
                    name.write_str("Extra").unwrap();
                }

                name
            };

            let rewritten = StreamsRewriter::rewrite_ident(
                &syn::parse_str(&name).unwrap(),
                &node,
                &chain_customizer,
            )
            .map(|ty| quote![#ty].to_string());

            let expect_invalid = match number {
                None => true,
                Some(number) if number >= MAX_STREAMS => true,
                _ => false,
            } || is_in && !unpacked
                || is_in && !is_output
                || unpacked && and_unpacked_out
                || extra;

            if !expect_invalid {
                valid += 1;

                let mut expected_name = "crate :: chain :: streams :: ".to_owned();

                if !is_output {
                    expected_name.write_str("input_record_type").unwrap();
                } else {
                    expected_name.write_str("output_record_type").unwrap();
                }

                expected_name.write_str(" :: Qb").unwrap();

                if unpacked {
                    expected_name.write_str("Unpacked").unwrap();
                }

                expected_name.write_str("Record").unwrap();

                if is_in {
                    expected_name.write_str("In").unwrap();
                }

                if !is_output {
                    write!(expected_name, "{}", number.unwrap() * 3).unwrap();
                } else {
                    write!(expected_name, "{}", number.unwrap() * 7).unwrap();
                }

                if and_unpacked_out {
                    expected_name.write_str("AndUnpackedOut").unwrap();
                }

                assert_eq!(rewritten, Some(expected_name), "rewrite {}", name);
            } else {
                assert_eq!(rewritten, None, "rewrite {}", name);
            }
        }

        println!("Valids: {} / {}", valid, ITERATIONS);
        let range = (ITERATIONS * 7 / 100)..=(ITERATIONS / 10);
        assert!(
            range.contains(&valid),
            "valid {} <= {} <= {}",
            range.start(),
            valid,
            range.end()
        );
    }
}
