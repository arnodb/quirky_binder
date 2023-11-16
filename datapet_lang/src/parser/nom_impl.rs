use nom::{
    branch::alt,
    bytes::complete::{escaped, is_not, tag, take_while1},
    character::complete::{
        alpha1, alphanumeric1, anychar, char, multispace0, multispace1, satisfy,
    },
    combinator::{cut, eof, opt, peek, recognize},
    error::ParseError,
    multi::{many0, many0_count, many1, many_till, separated_list0, separated_list1},
    sequence::{delimited, pair, preceded, terminated, tuple},
    AsChar, IResult, InputTakeAtPosition, Parser,
};

use crate::ast::{
    ConnectedFilter, Filter, Graph, GraphDefinition, GraphDefinitionSignature, Module, StreamLine,
    StreamLineInput, StreamLineOutput, UseDeclaration,
};

use super::{SpannedError, SpannedErrorKind};

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
        terminated(token("use"), multispace1),
        cut(terminated(use_tree, ps(token(";")))),
    )
    .map(|use_tree| UseDeclaration {
        use_tree: use_tree.into(),
    })
    .parse(input)
}

fn use_tree(input: &str) -> SpannedResult<&str, &str> {
    recognize(alt((
        recognize(pair(
            simple_path,
            cut(alt((
                recognize(pair(ds(token("::")), use_sub_tree)),
                recognize(opt(tuple((
                    multispace1,
                    token("as"),
                    cut(pair(multispace1, alt((identifier, tag("_"))))),
                )))),
            ))),
        )),
        recognize(pair(opt(ts(token("::"))), use_sub_tree)),
    )))
    .parse(input)
}

fn use_sub_tree(input: &str) -> SpannedResult<&str, &str> {
    recognize(alt((
        recognize(token("*")),
        recognize(tuple((
            ts(token("{")),
            cut(pair(
                opt(tuple((
                    ts(use_tree),
                    many0(pair(ts(token(",")), ts(use_tree))),
                    opt(ts(token(","))),
                ))),
                token("}"),
            )),
        ))),
    )))
    .parse(input)
}

fn graph_definition(input: &str) -> SpannedResult<&str, GraphDefinition> {
    tuple((
        opt(tag("pub")),
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
            inputs: inputs.map(|inputs| inputs.into_iter().map(Into::into).collect()),
            name: name.into(),
            params: params.into_iter().map(Into::into).collect(),
            outputs: outputs.map(|outputs| outputs.into_iter().map(|s| (*s).into()).collect()),
        },
    )
    .parse(input)
}

fn params(input: &str) -> SpannedResult<&str, Vec<&str>> {
    delimited(
        token("("),
        ps(separated_list0(token(","), ds(identifier))),
        token(")"),
    )
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
                        .map(|main_output: &str| Some(StreamLineOutput::Main(main_output.into()))),
                    preceded(
                        tag("-"),
                        ps(preceded(tag(">"), cut(ds(identifier))))
                            .map(Into::into)
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
                || StreamLineInput::Main(main_stream.into()),
                |main_input| StreamLineInput::Named(main_input.into()),
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
            inputs: assemble_inputs(StreamLineInput::Main(main_stream.into()), extra_streams),
            filter,
        })
        .parse(input)
}

fn assemble_inputs<'a>(
    first: StreamLineInput<'a>,
    extra_streams: Vec<&'a str>,
) -> Vec<StreamLineInput<'a>> {
    Some(first)
        .into_iter()
        .chain(
            extra_streams
                .into_iter()
                .map(Into::into)
                .map(StreamLineInput::Named),
        )
        .collect()
}

fn filter_input_streams(input: &str) -> SpannedResult<&str, (&str, Vec<&str>)> {
    pair(tag("-"), ps(opt_streams1))
        .map(|(main, streams)| (main, streams.unwrap_or_default()))
        .parse(input)
}

fn filter(input: &str) -> SpannedResult<&str, Filter> {
    tuple((
        simple_path,
        ps(opt(preceded(token("#"), cut(ps(identifier))))),
        ps(filter_params),
        ps(opt_streams1),
    ))
    .map(|(name, alias, params, extra_streams)| Filter {
        name: name.into(),
        alias: alias.map(Into::into),
        params: params.into(),
        extra_outputs: extra_streams.map_or_else(Vec::new, |extra_outputs| {
            extra_outputs.into_iter().map(Into::into).collect()
        }),
    })
    .parse(input)
}

fn filter_params(input: &str) -> SpannedResult<&str, &str> {
    recognize(preceded(
        char('('),
        cut(terminated(
            many0(alt((code, recognize(is_not("\"'()[]{}"))))),
            char(')'),
        )),
    ))(input)
}

fn code(input: &str) -> SpannedResult<&str, &str> {
    recognize(many1(alt((special_code, is_not("\"'()[]{},;")))))(input)
}

fn special_code(input: &str) -> SpannedResult<&str, &str> {
    recognize(alt((
        recognize(preceded(
            char('"'),
            cut(terminated(escaped(is_not("\""), '\\', anychar), char('"'))),
        )),
        recognize(preceded(
            char('\''),
            cut(terminated(escaped(is_not("\'"), '\\', anychar), char('\''))),
        )),
        recognize(preceded(
            char('('),
            cut(terminated(
                many0(alt((code, recognize(is_not("\"'()[]{}"))))),
                char(')'),
            )),
        )),
        recognize(preceded(
            char('['),
            cut(terminated(
                many0(alt((code, recognize(is_not("\"'()[]{}"))))),
                char(']'),
            )),
        )),
        recognize(preceded(
            char('{'),
            cut(terminated(
                many0(alt((code, recognize(is_not("\"'()[]{}"))))),
                char('}'),
            )),
        )),
    )))(input)
}

fn opt_streams0(input: &str) -> SpannedResult<&str, Option<Vec<&str>>> {
    opt(preceded(token("["), cut(opened_streams0))).parse(input)
}

fn opened_streams0(input: &str) -> SpannedResult<&str, Vec<&str>> {
    terminated(ps(separated_list0(token(","), ds(identifier))), token("]")).parse(input)
}

fn opt_streams1(input: &str) -> SpannedResult<&str, Option<Vec<&str>>> {
    opt(preceded(token("["), cut(opened_streams1))).parse(input)
}

fn opened_streams1(input: &str) -> SpannedResult<&str, Vec<&str>> {
    terminated(
        ps(separated_list1(token(","), ds(cut(identifier)))),
        token("]"),
    )
    .parse(input)
}

pub fn simple_path(input: &str) -> SpannedResult<&str, &str> {
    recognize(tuple((
        opt(tag("::")),
        ps(identifier),
        many0(pair(ps(tag("::")), ps(identifier))),
    )))
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
    .map_err(|err| {
        err.map(|_: SpannedError<&str>| SpannedError {
            kind: SpannedErrorKind::Identifier,
            span: fake_lex(input),
        })
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

fn fake_lex(input: &str) -> &str {
    take_while1::<_, _, nom::error::Error<&str>>(|c: char| {
        c.is_alphabetic() || c.is_numeric() || c == '_'
    })(input)
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
                inputs: Some(vec!["a".into(), "b".into()]),
                name: "foo_123".into(),
                params: vec!["foo".into(), "bar".into()],
                outputs: Some(vec!["c".into()]),
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
