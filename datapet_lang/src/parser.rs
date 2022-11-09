use nom::{
    branch::alt,
    bytes::complete::tag,
    character::complete::{alpha1, alphanumeric1, multispace0},
    combinator::{cut, eof, opt, recognize},
    error::ParseError,
    multi::{many0, many0_count, many_till, separated_list0, separated_list1},
    sequence::{delimited, pair, preceded, terminated, tuple},
    AsChar, IResult, InputTakeAtPosition, Parser,
};

use crate::ast::{
    ConnectedFilter, Filter, FilterParam, GraphDefinition, GraphDefinitionSignature, Module,
    StreamLine, StreamLineInput, StreamLineOutput,
};

pub fn module(input: &str) -> IResult<&str, Module> {
    ps(many_till(ts(graph_definition), eof))
        .map(|(graph_definitions, _)| Module { graph_definitions })
        .parse(input)
}

fn graph_definition(input: &str) -> IResult<&str, GraphDefinition> {
    pair(graph_definition_signature, ps(stream_lines))
        .map(|(signature, stream_lines)| GraphDefinition {
            signature,
            stream_lines,
        })
        .parse(input)
}

fn graph_definition_signature(input: &str) -> IResult<&str, GraphDefinitionSignature> {
    tuple((opt_streams0, ps(identifier), ps(params), ps(opt_streams0)))
        .map(|(inputs, name, params, outputs)| GraphDefinitionSignature {
            inputs: inputs.map(|inputs| inputs.into_iter().map(ToString::to_string).collect()),
            name: name.to_string(),
            params: params.into_iter().map(ToString::to_string).collect(),
            outputs: outputs.map(|outputs| outputs.into_iter().map(ToString::to_string).collect()),
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
                delimited(
                    tag("-"),
                    ps(opt(preceded(
                        tag(">"),
                        cut(ps(identifier)
                            .map(ToString::to_string)
                            .map(StreamLineOutput::Named)),
                    ))),
                    ps(tag(")")),
                )
                .map(|output| Some(output.unwrap_or(StreamLineOutput::Main))),
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
    .map(|(main_input, extra_streams)| {
        assemble_inputs(
            main_input.map_or_else(
                || StreamLineInput::Main,
                |main_input| StreamLineInput::Named(main_input.to_string()),
            ),
            extra_streams,
        )
    }))
    .map(|inputs| inputs.unwrap_or_default())
    .parse(input)
}

fn stream_line_filter(input: &str) -> IResult<&str, ConnectedFilter> {
    pair(filter_input_streams, ps(filter))
        .map(|(extra_streams, filter)| ConnectedFilter {
            inputs: assemble_inputs(StreamLineInput::Main, extra_streams),
            filter,
        })
        .parse(input)
}

fn assemble_inputs(first: StreamLineInput, extra_streams: Vec<&str>) -> Vec<StreamLineInput> {
    Some(first)
        .into_iter()
        .chain(
            extra_streams
                .into_iter()
                .map(ToString::to_string)
                .map(StreamLineInput::Named),
        )
        .collect()
}

fn filter_input_streams(input: &str) -> IResult<&str, Vec<&str>> {
    preceded(tag("-"), ps(opt_streams1))
        .map(|streams| streams.unwrap_or_default())
        .parse(input)
}

fn filter(input: &str) -> IResult<&str, Filter> {
    tuple((
        recognize(tuple((
            opt(tag("::")),
            ps(identifier),
            many0(pair(ps(tag("::")), ps(identifier))),
        ))),
        ps(opt(preceded(tag("#"), cut(ps(identifier))))),
        ps(filter_params),
        ps(opt_streams1),
    ))
    .map(|(name, alias, params, extra_streams)| Filter {
        name: name.to_string(),
        alias: alias.map(ToString::to_string),
        params,
        extra_outputs: extra_streams.map_or_else(Vec::new, |extra_outputs| {
            extra_outputs.into_iter().map(ToString::to_string).collect()
        }),
    })
    .parse(input)
}

fn filter_params(input: &str) -> IResult<&str, Vec<FilterParam>> {
    delimited(
        tag("("),
        ps(separated_list0(tag(","), ds(filter_param))),
        tag(")"),
    )(input)
}

fn filter_param(input: &str) -> IResult<&str, FilterParam> {
    alt((
        preceded(
            tag("["),
            cut(terminated(
                ps(separated_list0(tag(","), ds(identifier)).map(|vec| {
                    FilterParam::Array(vec.into_iter().map(ToString::to_string).collect())
                })),
                tag("]"),
            )),
        ),
        identifier.map(|single| FilterParam::Single(single.to_string())),
    ))(input)
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
