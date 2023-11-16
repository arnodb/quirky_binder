use antinom::{
    branch::alt,
    bytes::complete::tag,
    character::complete::{alpha1, alphanumeric1, multispace0, multispace1},
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
const MAX_SCOPE_USE_TREES: u8 = 7;
const MAX_SIMPLE_PATH_ITEMS: u8 = 3;
const MAX_SPACES: u8 = 3;

pub fn use_declaration<R>(rng: &mut R, buffer: &mut String)
where
    R: AntiNomRng,
{
    preceded(
        terminated(token("use"), multispace1(MAX_SPACES)),
        cut(terminated(
            |rng: &mut R, buffer: &mut String| use_tree(rng, buffer, 0),
            ps(token(";")),
        )),
    )
    .gen(rng, buffer)
}

pub fn use_tree<R>(rng: &mut R, buffer: &mut String, depth: usize)
where
    R: AntiNomRng,
{
    if depth >= 42 {
        buffer.push_slice("very_deep");
        return;
    }
    recognize(alt((
        recognize(pair(
            simple_path,
            cut(alt((
                recognize(pair(
                    ds(token("::")),
                    cut(|rng: &mut R, buffer: &mut String| use_sub_tree(rng, buffer, depth)),
                )),
                recognize(opt(tuple((
                    multispace1(MAX_SPACES),
                    token("as"),
                    cut(pair(multispace1(MAX_SPACES), identifier)),
                )))),
            ))),
        )),
        recognize(pair(
            ts(token("::")),
            cut(|rng: &mut R, buffer: &mut String| use_sub_tree(rng, buffer, depth)),
        )),
        recognize(|rng: &mut R, buffer: &mut String| use_sub_tree(rng, buffer, depth)),
    )))
    .gen(rng, buffer)
}

pub fn use_sub_tree<R>(rng: &mut R, buffer: &mut String, depth: usize)
where
    R: AntiNomRng,
{
    recognize(alt((
        recognize(token("*")),
        recognize(tuple((
            ts(token("{")),
            cut(pair(
                opt(tuple((
                    ts(|rng: &mut R, buffer: &mut String| use_tree(rng, buffer, depth + 1)),
                    many0(
                        pair(
                            ts(token(",")),
                            ts(|rng: &mut R, buffer: &mut String| use_tree(rng, buffer, depth + 1)),
                        ),
                        MAX_SCOPE_USE_TREES - 1,
                    ),
                    opt(ts(token(","))),
                ))),
                token("}"),
            )),
        ))),
    )))
    .gen(rng, buffer)
}

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

pub fn simple_path<R>(rng: &mut R, buffer: &mut String)
where
    R: AntiNomRng,
{
    recognize(tuple((
        opt(tag("::")),
        ps(identifier),
        many0(pair(ps(tag("::")), ps(identifier)), MAX_SIMPLE_PATH_ITEMS),
    )))
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
    .gen(rng, buffer);
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

fn ts<R, B, F>(f: F) -> impl Generator<R, B>
where
    R: AntiNomRng,
    B: Buffer,
    B::Char: From<u8>,
    F: Generator<R, B>,
{
    terminated(f, multispace0(MAX_SPACES))
}
