use antinom::{
    branch::alt,
    bytes::complete::tag,
    character::complete::{alpha1, alphanumeric1, multispace0},
    combinator::{cut, opt, recognize},
    multi::{many0, separated_list0},
    rng::AntiNomRng,
    sequence::{delimited, pair, preceded, terminated, tuple},
    Buffer, Generator,
};

const MAX_FILTER_STREAMS: u8 = 5;
const MAX_IDENTIFIER_FRAGMENT_LENGTH: u8 = 3;
const MAX_IDENTIFIER_FRAGMENTS: u8 = 3;
const MAX_PARAMS: u8 = 5;
const MAX_SPACES: u8 = 3;

pub fn graph_definition_signature<R>(rng: &mut R, buffer: &mut String)
where
    R: AntiNomRng,
{
    tuple((
        opt_streams0,
        ps(identifier),
        cut(pair(ps(params), ps(opt_streams0))),
    ))
    .gen(rng, buffer)
}

pub fn params<R>(rng: &mut R, buffer: &mut String)
where
    R: AntiNomRng,
{
    delimited(
        token("("),
        ps(separated_list0(token(","), ds(identifier), MAX_PARAMS)),
        token(")"),
    )
    .gen(rng, buffer)
}

pub fn opt_streams0<R>(rng: &mut R, buffer: &mut String)
where
    R: AntiNomRng,
{
    opt(preceded(token("["), cut(opened_streams0))).gen(rng, buffer)
}

pub fn opened_streams0<R>(rng: &mut R, buffer: &mut String)
where
    R: AntiNomRng,
{
    terminated(
        ps(separated_list0(
            token(","),
            ds(identifier),
            MAX_FILTER_STREAMS,
        )),
        token("]"),
    )
    .gen(rng, buffer)
}

pub fn identifier<R>(rng: &mut R, buffer: &mut String)
where
    R: AntiNomRng,
{
    recognize(tuple((
        alt((alpha1(MAX_IDENTIFIER_FRAGMENT_LENGTH), tag("_"))),
        many0(
            alt((alphanumeric1(MAX_IDENTIFIER_FRAGMENT_LENGTH), tag("_"))),
            MAX_IDENTIFIER_FRAGMENTS - 1,
        ),
    )))
    .gen(rng, buffer)
}

fn token<R, B, T>(token: T) -> impl Generator<R, B>
where
    R: AntiNomRng,
    B: Buffer,
    T: AsRef<B::Slice> + Clone,
{
    tag(token)
}

fn ds<R, B, F>(f: F) -> impl Generator<R, B>
where
    R: AntiNomRng,
    B: Buffer,
    B::Char: From<u8>,
    F: Generator<R, B>,
{
    delimited(multispace0(MAX_SPACES), f, multispace0(MAX_SPACES))
}

fn ps<R, B, F>(f: F) -> impl Generator<R, B>
where
    R: AntiNomRng,
    B: Buffer,
    B::Char: From<u8>,
    F: Generator<R, B>,
{
    preceded(multispace0(MAX_SPACES), f)
}
