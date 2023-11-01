use nom::{
    branch::alt,
    bytes::complete::{escaped, is_not, tag},
    character::complete::{alpha1, alphanumeric1, anychar, char, multispace0, multispace1},
    combinator::{cut, eof, opt, recognize},
    error::ParseError,
    multi::{many0, many0_count, many1, many_till, separated_list0, separated_list1},
    sequence::{delimited, pair, preceded, terminated, tuple},
    AsChar, IResult, InputTakeAtPosition, Parser,
};

use crate::ast::{
    ConnectedFilter, Filter, Graph, GraphDefinition, GraphDefinitionSignature, Module, StreamLine,
    StreamLineInput, StreamLineOutput, UseDeclaration,
};

pub fn module(input: &str) -> IResult<&str, Module> {
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

fn use_declaration(input: &str) -> IResult<&str, UseDeclaration> {
    delimited(
        terminated(tag("use"), multispace1),
        cut(code),
        cut(ps(tag(";"))),
    )
    .map(|use_tree| UseDeclaration {
        use_tree: use_tree.into(),
    })
    .parse(input)
}

fn graph_definition(input: &str) -> IResult<&str, GraphDefinition> {
    tuple((
        opt(tag("pub")),
        ps(graph_definition_signature),
        ps(stream_lines),
    ))
    .map(|(visibility, signature, stream_lines)| GraphDefinition {
        signature,
        stream_lines,
        visible: visibility.is_some(),
    })
    .parse(input)
}

fn graph_definition_signature(input: &str) -> IResult<&str, GraphDefinitionSignature> {
    tuple((opt_streams0, ps(identifier), ps(params), ps(opt_streams0)))
        .map(|(inputs, name, params, outputs)| GraphDefinitionSignature {
            inputs: inputs.map(|inputs| inputs.into_iter().map(Into::into).collect()),
            name: name.into(),
            params: params.into_iter().map(Into::into).collect(),
            outputs: outputs.map(|outputs| outputs.into_iter().map(|s| (*s).into()).collect()),
        })
        .parse(input)
}

fn params(input: &str) -> IResult<&str, Vec<&str>> {
    delimited(
        tag("("),
        ps(separated_list0(tag(","), ds(identifier))),
        tag(")"),
    )(input)
}

fn graph(input: &str) -> IResult<&str, Graph> {
    stream_lines
        .map(|stream_lines| Graph { stream_lines })
        .parse(input)
}

fn stream_lines(input: &str) -> IResult<&str, Vec<StreamLine>> {
    preceded(tag("{"), ps(many_till(ts(stream_line), tag("}"))))
        .map(|(stream_lines, _)| stream_lines)
        .parse(input)
}

fn stream_line(input: &str) -> IResult<&str, StreamLine> {
    preceded(tag("("), opened_stream_line)(input)
}

fn opened_stream_line(input: &str) -> IResult<&str, StreamLine> {
    pair(
        ps(pair(stream_line_inputs, ps(filter))
            .map(|(inputs, filter)| ConnectedFilter { inputs, filter })),
        ps(many_till(
            ts(stream_line_filter),
            alt((
                terminated(
                    pair(
                        tag("-"),
                        ps(opt(preceded(
                            tag(">"),
                            cut(ps(identifier).map(Into::into).map(StreamLineOutput::Named)),
                        ))),
                    ),
                    ps(tag(")")),
                )
                .map(|(main_output, output)| {
                    Some(output.unwrap_or_else(|| StreamLineOutput::Main(main_output.into())))
                }),
                tag(")").map(|_| None),
            )),
        )),
    )
    .map(|(first_filter, (mut filters, output))| {
        filters.insert(0, first_filter);
        StreamLine { filters, output }
    })
    .parse(input)
}

fn stream_line_inputs(input: &str) -> IResult<&str, Vec<StreamLineInput>> {
    opt(pair(
        opt(preceded(tag("<"), cut(ds(identifier)))),
        filter_input_streams,
    )
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

fn stream_line_filter(input: &str) -> IResult<&str, ConnectedFilter> {
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

fn filter_input_streams(input: &str) -> IResult<&str, (&str, Vec<&str>)> {
    pair(tag("-"), ps(opt_streams1))
        .map(|(main, streams)| (main, streams.unwrap_or_default()))
        .parse(input)
}

fn filter(input: &str) -> IResult<&str, Filter> {
    tuple((
        simple_path,
        ps(opt(preceded(tag("#"), cut(ps(identifier))))),
        ps(filter_params),
        ps(opt_streams1),
    ))
    .map(|(name, alias, params, extra_streams)| Filter {
        name: name.into(),
        alias: alias.map(Into::into),
        params: params.into_iter().map(Into::into).collect(),
        extra_outputs: extra_streams.map_or_else(Vec::new, |extra_outputs| {
            extra_outputs.into_iter().map(Into::into).collect()
        }),
    })
    .parse(input)
}

fn filter_params(input: &str) -> IResult<&str, Vec<&str>> {
    delimited(
        tag("("),
        ps(separated_list0(tag(","), ds(filter_param))),
        tag(")"),
    )(input)
}

fn filter_param(input: &str) -> IResult<&str, &str> {
    code(input)
}

fn code(input: &str) -> IResult<&str, &str> {
    recognize(many1(alt((special_code, is_not("\"'()[]{},;")))))(input)
}

fn special_code(input: &str) -> IResult<&str, &str> {
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

fn opt_streams0(input: &str) -> IResult<&str, Option<Vec<&str>>> {
    opt(preceded(tag("["), cut(opened_streams0)))(input)
}

fn opened_streams0(input: &str) -> IResult<&str, Vec<&str>> {
    terminated(ps(separated_list0(tag(","), ds(identifier))), tag("]"))(input)
}

fn opt_streams1(input: &str) -> IResult<&str, Option<Vec<&str>>> {
    opt(preceded(tag("["), cut(opened_streams1)))(input)
}

fn opened_streams1(input: &str) -> IResult<&str, Vec<&str>> {
    terminated(ps(separated_list1(tag(","), ds(identifier))), tag("]"))(input)
}

fn simple_path(input: &str) -> IResult<&str, &str> {
    recognize(tuple((
        opt(tag("::")),
        ps(identifier),
        many0(pair(ps(tag("::")), ps(identifier))),
    )))(input)
}

fn identifier(input: &str) -> IResult<&str, &str> {
    recognize(pair(
        alt((alpha1, tag("_"))),
        many0_count(alt((alphanumeric1, tag("_")))),
    ))(input)
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

#[test]
fn test_graph_definition_signature() {
    let (input, code) = assert_matches!(
        graph_definition_signature(", "),
        Err(nom::Err::Error(nom::error::Error { input, code })) => (input, code)
    );
    assert_eq!(input, ", ");
    assert_eq!(code, nom::error::ErrorKind::Tag);

    let (input, code) = assert_matches!(
        graph_definition_signature("[ , "),
        Err(nom::Err::Failure(nom::error::Error { input, code })) => (input, code)
    );
    assert_eq!(input, ", ");
    assert_eq!(code, nom::error::ErrorKind::Tag);
}
