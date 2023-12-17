use nom::{
    branch::alt,
    bytes::complete::{escaped, is_not, tag, take_while1},
    character::complete::{
        alpha1, alphanumeric1, anychar, char, multispace0, multispace1, satisfy,
    },
    combinator::{consumed, cut, eof, opt, peek, recognize},
    error::ParseError,
    multi::{many0, many0_count, many1_count, many_till},
    sequence::{delimited, pair, preceded, terminated, tuple},
    AsChar, IResult, InputTakeAtPosition, Parser,
};

use crate::ast::{
    ConnectedFilter, Filter, Graph, GraphDefinition, GraphDefinitionSignature, Module, StreamLine,
    StreamLineInput, StreamLineOutput, UseDeclaration,
};

use super::{assemble_inputs, SpannedError, SpannedErrorKind};

pub type SpannedResult<I, O> = IResult<I, O, SpannedError<I>>;

pub fn module(input: &str) -> SpannedResult<&str, Module> {
    ps(many_till(
        ts(alt((
            use_declaration.map(Into::into),
            graph_definition.map(Into::into),
            graph.map(Into::into),
        ))),
        eof,
    ))
    .map(|(items, _)| Module { items })
    .parse(input)
}

pub fn use_declaration(input: &str) -> SpannedResult<&str, UseDeclaration> {
    preceded(
        ts(keyword("use")),
        cut(terminated(use_tree, ps(token(";")))),
    )
    .map(|use_tree| UseDeclaration { use_tree })
    .parse(input)
}

fn use_tree(input: &str) -> SpannedResult<&str, &str> {
    recognize(alt((
        recognize(pair(
            simple_path,
            cut(alt((
                recognize(pair(ds(token("::")), cut(use_sub_tree))),
                recognize(opt(tuple((
                    multispace1,
                    keyword("as"),
                    cut(pair(
                        |input| {
                            multispace1(input).map_err(|err| {
                                err.map(|_: SpannedError<&str>| SpannedError {
                                    kind: SpannedErrorKind::Identifier,
                                    span: fake_lex(input),
                                })
                            })
                        },
                        identifier,
                    )),
                )))),
            ))),
        )),
        recognize(pair(ts(token("::")), cut(use_sub_tree))),
        recognize(use_sub_tree),
    )))
    .parse(input)
}

fn use_sub_tree(input: &str) -> SpannedResult<&str, &str> {
    recognize(alt((
        recognize(token("*")),
        recognize(tuple((
            ts(token("{")),
            cut(terminated(
                opt(terminated(
                    pair(
                        ts(use_tree),
                        many0_count(preceded(ts(token(",")), ts(use_tree))),
                    ),
                    opt(ts(token(","))),
                )),
                token("}"),
            )),
        ))),
    )))
    .parse(input)
}

fn graph_definition(input: &str) -> SpannedResult<&str, GraphDefinition> {
    tuple((
        opt(keyword("pub")),
        ps(graph_definition_signature),
        cut(ps(stream_lines)),
    ))
    .map(|(visibility, signature, stream_lines)| GraphDefinition {
        signature,
        stream_lines,
        visible: visibility.is_some(),
    })
    .parse(input)
}

pub fn graph_definition_signature(input: &str) -> SpannedResult<&str, GraphDefinitionSignature> {
    tuple((
        opt_streams0,
        ps(identifier),
        cut(pair(ps(params), ps(opt_streams0))),
    ))
    .map(
        |(inputs, name, (params, outputs))| GraphDefinitionSignature {
            inputs,
            name,
            params,
            outputs,
        },
    )
    .parse(input)
}

fn params(input: &str) -> SpannedResult<&str, Vec<&str>> {
    preceded(
        ts(token("(")),
        cut(terminated(
            opt(terminated(
                pair(
                    ts(identifier_or_fail),
                    many0(preceded(ts(token(",")), ts(identifier_or_fail))),
                ),
                opt(ts(token(","))),
            )),
            token(")"),
        )),
    )
    .map(|maybe_params| {
        maybe_params.map_or_else(Vec::new, |(first, more)| {
            let mut all = more;
            all.insert(0, first);
            all
        })
    })
    .parse(input)
}

fn graph(input: &str) -> SpannedResult<&str, Graph> {
    stream_lines
        .map(|stream_lines| Graph { stream_lines })
        .parse(input)
}

fn stream_lines(input: &str) -> SpannedResult<&str, Vec<StreamLine>> {
    preceded(tag("{"), ps(many_till(ts(stream_line), tag("}"))))
        .map(|(stream_lines, _)| stream_lines)
        .parse(input)
}

fn stream_line(input: &str) -> SpannedResult<&str, StreamLine> {
    preceded(tag("("), opened_stream_line)(input)
}

fn opened_stream_line(input: &str) -> SpannedResult<&str, StreamLine> {
    pair(
        ps(pair(stream_line_inputs, ps(filter))
            .map(|(inputs, filter)| ConnectedFilter { inputs, filter })),
        terminated(
            ps(many_till(
                ts(stream_line_filter),
                alt((
                    terminated(tag("-"), ps(peek(tag(")"))))
                        .map(|main_output: &str| Some(StreamLineOutput::Main(main_output))),
                    preceded(
                        tag("-"),
                        ps(preceded(tag(">"), cut(ds(identifier))))
                            .map(StreamLineOutput::Named)
                            .map(Some),
                    ),
                    peek(tag(")")).map(|_| None),
                )),
            )),
            tag(")"),
        ),
    )
    .map(|(first_filter, (mut filters, output))| {
        filters.insert(0, first_filter);
        StreamLine { filters, output }
    })
    .parse(input)
}

fn stream_line_inputs(input: &str) -> SpannedResult<&str, Vec<StreamLineInput>> {
    opt(alt((
        preceded(tag("<"), cut(pair(ds(identifier), filter_input_streams)))
            .map(|(main_input, extra_streams)| (Some(main_input), extra_streams)),
        filter_input_streams.map(|extra_streams| (None, extra_streams)),
    ))
    .map(|(main_input, (main_stream, extra_streams))| {
        assemble_inputs(
            main_input.map_or_else(
                || StreamLineInput::Main(main_stream),
                StreamLineInput::Named,
            ),
            extra_streams,
        )
    }))
    .map(|inputs| inputs.unwrap_or_default())
    .parse(input)
}

fn stream_line_filter(input: &str) -> SpannedResult<&str, ConnectedFilter> {
    pair(filter_input_streams, ps(filter))
        .map(|((main_stream, extra_streams), filter)| ConnectedFilter {
            inputs: assemble_inputs(StreamLineInput::Main(main_stream), extra_streams),
            filter,
        })
        .parse(input)
}

fn filter_input_streams(input: &str) -> SpannedResult<&str, (&str, Vec<&str>)> {
    pair(tag("-"), ps(opt_streams1))
        .map(|(main, streams)| (main, streams.unwrap_or_default()))
        .parse(input)
}

pub fn filter(input: &str) -> SpannedResult<&str, Filter> {
    tuple((
        simple_path,
        ps(opt(preceded(token("#"), cut(ps(identifier))))),
        cut(ps(filter_params)),
        ps(opt_streams1),
    ))
    .map(|(name, alias, params, extra_streams)| Filter {
        name,
        alias,
        params,
        extra_outputs: extra_streams.unwrap_or_default(),
    })
    .parse(input)
}

fn filter_params(input: &str) -> SpannedResult<&str, &str> {
    recognize(preceded(
        token("("),
        terminated(
            many0_count(alt((code, recognize(is_not("\"'()[]{}"))))),
            token(")"),
        ),
    ))
    .parse(input)
}

pub fn code(input: &str) -> SpannedResult<&str, &str> {
    recognize(many1_count(alt((special_code, is_not("\"'()[]{}")))))
        .parse(input)
        .map(|(tail, c)| {
            (
                tail,
                c.trim_matches(|c| c == ' ' || c == '\t' || c == '\n' || c == '\r'),
            )
        })
        .map_err(|err| {
            err.map(|err| match err.kind {
                SpannedErrorKind::Nom(_) | SpannedErrorKind::NomIncomplete => SpannedError {
                    kind: SpannedErrorKind::Code,
                    span: fake_lex(input),
                },
                _ => err,
            })
        })
}

fn special_code<'a>(input: &'a str) -> SpannedResult<&'a str, &'a str> {
    recognize(alt((
        consumed(preceded(
            char('"'),
            tuple((opt(escaped(is_not("\"\\"), '\\', anychar)), opt(char('"'))))
                .map(|(_, close)| close.is_some()),
        ))
        .and_then(|(span, full): (&'a str, bool)| {
            if full {
                Ok(((span, full), &span[span.len()..]))
            } else {
                Err(nom::Err::Failure(SpannedError {
                    kind: SpannedErrorKind::UnterminatedString,
                    span: &span[0..1],
                }))
            }
        }),
        consumed(preceded(
            char('\''),
            tuple((opt(escaped(is_not("\'\\"), '\\', anychar)), opt(char('\''))))
                .map(|(_, close)| close.is_some()),
        ))
        .and_then(|(span, full): (&'a str, bool)| {
            if full {
                Ok(((span, full), &span[span.len()..]))
            } else {
                Err(nom::Err::Failure(SpannedError {
                    kind: SpannedErrorKind::UnterminatedChar,
                    span: &span[0..1],
                }))
            }
        }),
        recognize(preceded(
            peek(char('(')),
            delimited(
                char('('),
                many0_count(alt((code, recognize(is_not("\"'()[]{}"))))),
                char(')'),
            )
            .or(|input| {
                Err(nom::Err::Failure(SpannedError {
                    kind: SpannedErrorKind::UnbalancedCode,
                    span: fake_lex(input),
                }))
            }),
        )),
        recognize(preceded(
            peek(char('[')),
            delimited(
                char('['),
                many0_count(alt((code, recognize(is_not("\"'()[]{}"))))),
                char(']'),
            )
            .or(|input| {
                Err(nom::Err::Failure(SpannedError {
                    kind: SpannedErrorKind::UnbalancedCode,
                    span: fake_lex(input),
                }))
            }),
        )),
        recognize(preceded(
            peek(char('{')),
            delimited(
                char('{'),
                many0_count(alt((code, recognize(is_not("\"'()[]{}"))))),
                char('}'),
            )
            .or(|input| {
                Err(nom::Err::Failure(SpannedError {
                    kind: SpannedErrorKind::UnbalancedCode,
                    span: fake_lex(input),
                }))
            }),
        )),
    )))
    .parse(input)
}

pub fn opt_streams0(input: &str) -> SpannedResult<&str, Option<Vec<&str>>> {
    opt(preceded(
        ts(token("[")),
        cut(terminated(
            opt(terminated(
                pair(
                    ts(identifier_or_fail),
                    many0(preceded(ts(token(",")), ts(identifier_or_fail))),
                ),
                opt(ts(token(","))),
            )),
            token("]"),
        )),
    ))
    .map(|maybe_zero_streams| {
        maybe_zero_streams.map(|maybe_more_streams| {
            maybe_more_streams.map_or_else(Vec::new, |(first, more)| {
                let mut all = more;
                all.insert(0, first);
                all
            })
        })
    })
    .parse(input)
}

pub fn opt_streams1(input: &str) -> SpannedResult<&str, Option<Vec<&str>>> {
    opt(preceded(
        ts(token("[")),
        cut(terminated(
            terminated(
                pair(
                    ts(identifier_or_fail),
                    many0(preceded(ts(token(",")), ts(identifier_or_fail))),
                ),
                opt(ts(token(","))),
            ),
            token("]"),
        )),
    ))
    .map(|maybe_zero_streams| {
        maybe_zero_streams.map(|(first, more)| {
            let mut all = more;
            all.insert(0, first);
            all
        })
    })
    .parse(input)
}

pub fn simple_path(input: &str) -> SpannedResult<&str, &str> {
    recognize(tuple((
        opt(ts(tag("::"))),
        identifier_or_fail,
        many0_count(pair(ps(tag("::")), ps(identifier_or_fail))),
    )))
    .parse(input)
}

fn identifier_or_fail(input: &str) -> SpannedResult<&str, &str> {
    identifier
        .or(not_an_identifier
            .and_then(|input| {
                Err(nom::Err::Failure(SpannedError {
                    kind: SpannedErrorKind::Identifier,
                    span: fake_lex(input),
                }))
            })
            .or(|input| {
                Err(nom::Err::Error(SpannedError {
                    kind: SpannedErrorKind::Identifier,
                    span: fake_lex(input),
                }))
            }))
        .parse(input)
}

pub fn identifier(input: &str) -> SpannedResult<&str, &str> {
    recognize(tuple((
        alt((alpha1, tag("_"))),
        many0_count(alt((alphanumeric1, tag("_")))),
        peek(alt((
            recognize(satisfy(|c| {
                !c.is_alphabetic() && !c.is_numeric() && c != '_'
            })),
            recognize(eof),
        ))),
    )))
    .parse(input)
    .and_then(|(tail, ident)| match ident {
        "as" | "pub" | "use" => Err(nom::Err::Error(())),
        ident => Ok((tail, ident)),
    })
    .map_err(|err| {
        err.map(|_: ()| SpannedError {
            kind: SpannedErrorKind::Identifier,
            span: fake_lex(input),
        })
    })
}

pub fn not_an_identifier(input: &str) -> SpannedResult<&str, &str> {
    // See fake_lex, but may differ in the future
    alt((
        take_while1::<_, _, SpannedError<&str>>(|c: char| {
            c.is_alphabetic() || c.is_numeric() || c == '_'
        }),
        tag("::"),
    ))
    .parse(input)
    .and_then(|(tail, ident)| match ident {
        "as" | "pub" | "use" | "::" => Err(nom::Err::Error(SpannedError {
            kind: SpannedErrorKind::Identifier,
            span: fake_lex(input),
        })),
        ident => Ok((tail, ident)),
    })
}

fn token<'a>(token: &'static str) -> impl Parser<&'a str, &'a str, SpannedError<&'a str>> {
    let mut parser = tag(token);
    move |input| {
        parser.parse(input).map_err(|err| {
            err.map(|_: SpannedError<&str>| SpannedError {
                kind: SpannedErrorKind::Token(token),
                span: fake_lex(input),
            })
        })
    }
}

fn keyword<'a>(kw: &'static str) -> impl Parser<&'a str, &'a str, SpannedError<&'a str>> {
    let mut parser = terminated(
        tag(kw),
        alt((
            recognize(peek(satisfy(|c: char| {
                !(c.is_alphabetic() || c.is_numeric() || c == '_')
            }))),
            eof,
        )),
    );
    move |input| {
        parser.parse(input).map_err(|err| {
            err.map(|_: SpannedError<&str>| SpannedError {
                kind: SpannedErrorKind::Token(kw),
                span: fake_lex(input),
            })
        })
    }
}

fn fake_lex(input: &str) -> &str {
    alt((
        take_while1::<_, _, SpannedError<&str>>(|c: char| {
            c.is_alphabetic() || c.is_numeric() || c == '_'
        }),
        recognize(preceded(
            char('"'),
            tuple((opt(escaped(is_not("\"\\"), '\\', anychar)), opt(char('"')))),
        )),
        recognize(preceded(
            char('\''),
            tuple((opt(escaped(is_not("'\\"), '\\', anychar)), opt(char('\'')))),
        )),
        tag("::"),
    ))
    .parse(input)
    .map_or_else(
        |_| &input[0..{ input.chars().next().map_or(0, |c| c.len_utf8()) }],
        |(_, a)| a,
    )
}

fn ds<I, O, E: ParseError<I>, F>(parser: F) -> impl FnMut(I) -> IResult<I, O, E>
where
    I: InputTakeAtPosition,
    <I as InputTakeAtPosition>::Item: AsChar + Clone,
    F: Parser<I, O, E>,
{
    delimited(multispace0, parser, multispace0)
}

fn ps<I, O, E: ParseError<I>, F>(parser: F) -> impl FnMut(I) -> IResult<I, O, E>
where
    I: InputTakeAtPosition,
    <I as InputTakeAtPosition>::Item: AsChar + Clone,
    F: Parser<I, O, E>,
{
    preceded(multispace0, parser)
}

fn ts<I, O, E: ParseError<I>, F>(parser: F) -> impl FnMut(I) -> IResult<I, O, E>
where
    I: InputTakeAtPosition,
    <I as InputTakeAtPosition>::Item: AsChar + Clone,
    F: Parser<I, O, E>,
{
    terminated(parser, multispace0)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    macro_rules! assert_span_at_distance {
        ($input:ident, $span:ident, $expected:expr, $expected_distance:expr) => {
            assert_span_at_distance!($input, $span, $expected, $expected_distance, "span")
        };
        ($input:ident, $span:ident, $expected:expr, $expected_distance:expr, $what:literal) => {
            assert_eq!(
                $span, $expected,
                r#"{} expected to be "{}" but was "{}""#,
                $what, $expected, $span
            );
            assert_eq!(
                $span.as_ptr() as usize - $input.as_ptr() as usize,
                $expected_distance,
                "{} expected to be at distance {}, but was at {}",
                $what,
                $expected_distance,
                $span.as_ptr() as usize - $input.as_ptr() as usize
            );
        };
    }

    #[rstest]
    #[case("use foo;", "foo")]
    #[case("use foo\u{20};", "foo")]
    #[case("use ::foo;", "::foo")]
    #[case("use ::foo\u{20};", "::foo")]
    #[case("use foo::*;", "foo::*")]
    #[case("use foo::*\u{20};", "foo::*")]
    #[case("use foo as bar;", "foo as bar")]
    #[case("use foo as bar\u{20};", "foo as bar")]
    #[case("use ::{foo};", "::{foo}")]
    #[case("use ::{foo}\u{20};", "::{foo}")]
    fn test_valid_use_declaration(#[case] input: &str, #[case] expected_use_tree: &str) {
        let (tail, ud) = assert_matches!(
            use_declaration(input),
            Ok((tail, signature)) => (tail, signature)
        );
        assert_span_at_distance!(input, tail, "", input.len(), "tail");
        assert_eq!(ud.use_tree, expected_use_tree);
    }

    #[rstest]
    #[case("use", SpannedErrorKind::Token("{"), "", "use".as_bytes().len())]
    #[case("use foo::{bar::,mar};", SpannedErrorKind::Token("{"), ",", "use foo::{bar::".as_bytes().len())]
    #[case("use foo as;", SpannedErrorKind::Identifier, ";", "use foo as".as_bytes().len())]
    #[case("use foo as\u{20};", SpannedErrorKind::Identifier, ";", "use foo as\u{20}".as_bytes().len())]
    #[case("use foo::{::,};", SpannedErrorKind::Token("{"), ",", "use foo::{::".as_bytes().len())]
    #[case("use foo::as bar;", SpannedErrorKind::Token("{"), "as", "use foo::".as_bytes().len())]
    #[case("use ::{::{foo};", SpannedErrorKind::Token("}"), ";", "use ::{::{foo]".as_bytes().len())]
    #[case("use foo asbar;", SpannedErrorKind::Token(";"), "asbar", "use foo ".as_bytes().len())]
    #[case("use ::{foo bar};", SpannedErrorKind::Token("}"), "bar", "use ::{foo ".as_bytes().len())]
    fn test_invalid_use_declaration(
        #[case] input: &str,
        #[case] expected_kind: SpannedErrorKind,
        #[case] expected_span: &str,
        #[case] expected_distance: usize,
    ) {
        let (kind, span) = assert_matches!(
            use_declaration(input),
            Err(nom::Err::Error(SpannedError { kind, span }))
            | Err(nom::Err::Failure(SpannedError { kind, span })) => (kind, span)
        );
        assert_eq!(kind, expected_kind);
        assert_span_at_distance!(input, span, expected_span, expected_distance);
    }

    #[test]
    fn test_valid_graph_definition_signature() {
        let input = "[a, b] foo_123(foo, bar) [c]";
        let (tail, signature) = assert_matches!(
            graph_definition_signature(input),
            Ok((tail, signature)) => (tail, signature)
        );
        assert_span_at_distance!(input, tail, "", input.len(), "tail");
        assert_eq!(
            signature,
            GraphDefinitionSignature {
                inputs: Some(vec!["a", "b"]),
                name: "foo_123",
                params: vec!["foo", "bar"],
                outputs: Some(vec!["c"]),
            }
        );
    }

    #[test]
    fn test_graph_definition_signature_name_missing() {
        let input = ", ";
        let (kind, span) = assert_matches!(
            graph_definition_signature(input),
            Err(nom::Err::Error(SpannedError { kind, span })) => (kind, span)
        );
        assert_eq!(kind, SpannedErrorKind::Identifier);
        assert_span_at_distance!(input, span, ",", 0);
    }

    #[test]
    fn test_graph_definition_signature_input_identifier_missing() {
        let input = "[, ";
        let (kind, span) = assert_matches!(
            graph_definition_signature(input),
            Err(nom::Err::Failure(SpannedError { kind, span })) => (kind, span)
        );
        assert_eq!(kind, SpannedErrorKind::Token("]"));
        assert_span_at_distance!(input, span, ",", 1);
    }

    #[rstest]
    #[case("foo{bar", SpannedErrorKind::UnbalancedCode, "{", "foo".as_bytes().len())]
    #[case("foo{(bar", SpannedErrorKind::UnbalancedCode, "(", "foo{".as_bytes().len())]
    #[case("(foo\"bar", SpannedErrorKind::UnterminatedString, "\"", "(foo".as_bytes().len())]
    #[case("foo'bar", SpannedErrorKind::UnterminatedChar, "'", "foo".as_bytes().len())]
    #[case("foo'bar\\", SpannedErrorKind::UnterminatedChar, "'", "foo".as_bytes().len())]
    fn test_invalid_code(
        #[case] input: &str,
        #[case] expected_kind: SpannedErrorKind,
        #[case] expected_span: &str,
        #[case] expected_distance: usize,
    ) {
        let (kind, span) = assert_matches!(
            code(input),
            Err(nom::Err::Failure(SpannedError { kind, span })) => (kind, span)
        );
        assert_eq!(kind, expected_kind);
        assert_span_at_distance!(input, span, expected_span, expected_distance);
    }

    #[rstest]
    #[case("[2foo]", SpannedErrorKind::Identifier, "2foo", "[".as_bytes().len())]
    #[case("[foo, 2bar]", SpannedErrorKind::Identifier, "2bar", "[foo, ".as_bytes().len())]
    #[case("[, foo]", SpannedErrorKind::Token("]"), ",", "[".as_bytes().len())]
    #[case("[foo bar]", SpannedErrorKind::Token("]"), "bar", "[foo ".as_bytes().len())]
    fn test_invalid_opt_streams0(
        #[case] input: &str,
        #[case] expected_kind: SpannedErrorKind,
        #[case] expected_span: &str,
        #[case] expected_distance: usize,
    ) {
        let (kind, span) = assert_matches!(
            opt_streams0(input),
            Err(nom::Err::Failure(SpannedError { kind, span })) => (kind, span)
        );
        assert_eq!(kind, expected_kind);
        assert_span_at_distance!(input, span, expected_span, expected_distance);
    }

    #[rstest]
    #[case("[2foo]", SpannedErrorKind::Identifier, "2foo", "[".as_bytes().len())]
    #[case("[foo, 2bar]", SpannedErrorKind::Identifier, "2bar", "[foo, ".as_bytes().len())]
    #[case("[, foo]", SpannedErrorKind::Identifier, ",", "[".as_bytes().len())]
    #[case("[foo bar]", SpannedErrorKind::Token("]"), "bar", "[foo ".as_bytes().len())]
    fn test_invalid_opt_streams1(
        #[case] input: &str,
        #[case] expected_kind: SpannedErrorKind,
        #[case] expected_span: &str,
        #[case] expected_distance: usize,
    ) {
        let (kind, span) = assert_matches!(
            opt_streams1(input),
            Err(nom::Err::Failure(SpannedError { kind, span })) => (kind, span)
        );
        assert_eq!(kind, expected_kind);
        assert_span_at_distance!(input, span, expected_span, expected_distance);
    }

    #[rstest]
    #[case("::2bar", SpannedErrorKind::Identifier, "2bar", "::".as_bytes().len())]
    #[case("foo::2bar", SpannedErrorKind::Identifier, "2bar", "foo::".as_bytes().len())]
    #[case(":: :: foo", SpannedErrorKind::Identifier, "::", ":: ".as_bytes().len())]
    fn test_invalid_simple_path(
        #[case] input: &str,
        #[case] expected_kind: SpannedErrorKind,
        #[case] expected_span: &str,
        #[case] expected_distance: usize,
    ) {
        let (kind, span) = assert_matches!(
            simple_path(input),
            Err(nom::Err::Error(SpannedError { kind, span }))
            | Err(nom::Err::Failure(SpannedError { kind, span })) => (kind, span)
        );
        assert_eq!(kind, expected_kind);
        assert_span_at_distance!(input, span, expected_span, expected_distance);
    }

    #[test]
    fn test_valid_identifier() {
        let input = "foo_123";
        let (tail, ident) = assert_matches!(
            identifier(input),
            Ok((tail, ident)) => (tail, ident)
        );
        assert_span_at_distance!(input, tail, "", input.len(), "tail");
        assert_eq!(ident, "foo_123");
    }

    #[test]
    fn test_valid_identifier_underscore() {
        let input = "_";
        let (tail, ident) = assert_matches!(
            identifier(input),
            Ok((tail, ident)) => (tail, ident)
        );
        assert_span_at_distance!(input, tail, "", input.len(), "tail");
        assert_eq!(ident, "_");
    }

    #[test]
    fn test_identifier_extra_alphabetic_char() {
        let input = "café";
        let (kind, span) = assert_matches!(
            identifier(input),
            Err(nom::Err::Error(SpannedError { kind, span })) => (kind, span)
        );
        assert_eq!(kind, SpannedErrorKind::Identifier);
        assert_span_at_distance!(input, span, "café", 0);
    }

    #[test]
    fn test_identifier_extra_non_alphabetic_char() {
        let input = "caf(";
        let (tail, ident) = assert_matches!(
            identifier(input),
            Ok((tail, ident)) => (tail, ident)
        );
        assert_span_at_distance!(input, tail, "(", 3, "tail");
        assert_eq!(ident, "caf");
    }
}
