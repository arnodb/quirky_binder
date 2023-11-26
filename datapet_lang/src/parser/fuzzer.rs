use antinom::{
    branch::alt,
    bytes::complete::tag,
    character::complete::{alpha1, alphanumeric1, char, multispace0, multispace1},
    combinator::{cut, opt, recognize},
    multi::{many0, many0_count, many1_count, separated_list0},
    rng::AntiNomRng,
    sequence::{delimited, pair, preceded, terminated, tuple},
    Buffer, Generator,
};

const MAX_CODE_CHARS: u8 = 8;
const MAX_FILTER_PARAMS: u8 = 8;
const MAX_FILTER_STREAMS: u8 = 5;
const MAX_IDENTIFIER_FRAGMENT_LENGTH: u8 = 3;
const MAX_IDENTIFIER_FRAGMENTS: u8 = 3;
const MAX_PARAMS: u8 = 5;
const MAX_SCOPE_USE_TREES: u8 = 7;
const MAX_SIMPLE_PATH_ITEMS: u8 = 3;
const MAX_SPACES: u8 = 3;
const MAX_SPECIAL_CODE: u8 = 3;
const MAX_USE_DEPTH: usize = 12;

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
    if depth >= MAX_USE_DEPTH {
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
            cut(terminated(
                opt(terminated(
                    pair(
                        ts(|rng: &mut R, buffer: &mut String| use_tree(rng, buffer, depth + 1)),
                        many0_count(
                            preceded(
                                ts(token(",")),
                                ts(|rng: &mut R, buffer: &mut String| {
                                    use_tree(rng, buffer, depth + 1)
                                }),
                            ),
                            MAX_SCOPE_USE_TREES - 1,
                        ),
                    ),
                    opt(ts(token(","))),
                )),
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

pub fn filter<R>(rng: &mut R, buffer: &mut String)
where
    R: AntiNomRng,
{
    tuple((
        simple_path,
        ps(opt(preceded(token("#"), cut(ps(identifier))))),
        cut(ps(filter_params)),
        ps(opt_streams1),
    ))
    .gen(rng, buffer)
}

pub fn filter_params<R>(rng: &mut R, buffer: &mut String)
where
    R: AntiNomRng,
{
    recognize(preceded(
        token("("),
        terminated(
            many0_count(
                alt((
                    code,
                    recognize(is_not(NO_QUOTE_APOSTROPHE_BRACKETS, MAX_CODE_CHARS)),
                )),
                MAX_FILTER_PARAMS,
            ),
            token(")"),
        ),
    ))
    .gen(rng, buffer)
}

pub fn opt_streams0<R>(rng: &mut R, buffer: &mut String)
where
    R: AntiNomRng,
{
    opt(preceded(
        ts(token("[")),
        cut(terminated(
            opt(terminated(
                pair(
                    ts(identifier),
                    many0(
                        preceded(ts(token(",")), ts(identifier)),
                        MAX_FILTER_STREAMS - 1,
                    ),
                ),
                opt(ts(token(","))),
            )),
            token("]"),
        )),
    ))
    .gen(rng, buffer)
}

pub fn opt_streams1<R>(rng: &mut R, buffer: &mut String)
where
    R: AntiNomRng,
{
    opt(preceded(
        ts(token("[")),
        cut(terminated(
            terminated(
                pair(
                    ts(identifier),
                    many0(
                        preceded(ts(token(",")), ts(identifier)),
                        MAX_FILTER_STREAMS - 1,
                    ),
                ),
                opt(ts(token(","))),
            ),
            token("]"),
        )),
    ))
    .gen(rng, buffer)
}

const ALL_CHARACTERS: &str = concat!(
    "\u{20}!\"#$%&'()*+,-./",
    "0123456789",
    ":;<=>?@",
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
    "[\\]^_`",
    "abcdefghijklmnopqrstuvwxyz",
    "{|}~"
);

const NO_SPECIAL_CHARACTERS: &str = concat!(
    "\u{20}!#$%&*+,-./",
    "0123456789",
    ":;<=>?@",
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
    "\\^_`",
    "abcdefghijklmnopqrstuvwxyz",
    "|~"
);

const NO_QUOTE_BACKSLASH: &str = concat!(
    "\u{20}!#$%&'()*+,-./",
    "0123456789",
    ":;<=>?@",
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
    "[]^_`",
    "abcdefghijklmnopqrstuvwxyz",
    "{|}~"
);

const NO_APOSTROPHE_BACKSLASH: &str = concat!(
    "\u{20}!\"#$%&()*+,-./",
    "0123456789",
    ":;<=>?@",
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
    "[]^_`",
    "abcdefghijklmnopqrstuvwxyz",
    "{|}~"
);

const NO_QUOTE_APOSTROPHE_BRACKETS: &str = concat!(
    "\u{20}!#$%&*+,-./",
    "0123456789",
    ":;<=>?@",
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
    "\\^_`",
    "abcdefghijklmnopqrstuvwxyz",
    "|~"
);

pub fn one_of<R, B>(list: &'static str) -> impl Generator<R, B>
where
    R: AntiNomRng,
    B: Buffer,
    B::Char: From<u8>,
{
    move |rng: &mut R, buffer: &mut B| {
        let i = rng.gen_range(0..list.len());
        buffer.push(list.as_bytes()[i].into());
    }
}

pub fn is_not<R, B>(list: &'static str, max_length: u8) -> impl Generator<R, B>
where
    R: AntiNomRng,
    B: Buffer,
    B::Char: From<u8>,
{
    move |rng: &mut R, buffer: &mut B| {
        for _ in 1..max_length {
            let i = rng.gen_range(0..list.len());
            buffer.push(list.as_bytes()[i].into());
        }
    }
}

pub fn escaped<R, B, F, G>(
    mut normal: F,
    control_char: char,
    mut escapable: G,
    max_iter: u8,
) -> impl Generator<R, B>
where
    R: AntiNomRng,
    B: Buffer,
    B::Char: From<char>,
    F: Generator<R, B>,
    G: Generator<R, B>,
{
    move |rng: &mut R, buffer: &mut B| {
        let iters = rng.gen_range(0..max_iter);
        for _ in 0..iters {
            let norm = rng.gen_bool();
            if norm {
                normal.gen(rng, buffer);
            } else {
                buffer.push(control_char.into());
                escapable.gen(rng, buffer);
            }
        }
    }
}

pub fn code<R>(rng: &mut R, buffer: &mut String)
where
    R: AntiNomRng,
{
    recognize(many1_count(
        alt((special_code, is_not(NO_SPECIAL_CHARACTERS, MAX_CODE_CHARS))),
        MAX_SPECIAL_CODE,
    ))
    .gen(rng, buffer)
}

pub fn special_code<R>(rng: &mut R, buffer: &mut String)
where
    R: AntiNomRng,
{
    recognize(alt((
        recognize(preceded(
            char('"'),
            cut(terminated(
                opt(escaped(
                    is_not(NO_QUOTE_BACKSLASH, 3),
                    '\\',
                    one_of(ALL_CHARACTERS),
                    3,
                )),
                char('"'),
            )),
        )),
        recognize(preceded(
            char('\''),
            cut(terminated(
                opt(escaped(
                    is_not(NO_APOSTROPHE_BACKSLASH, 3),
                    '\\',
                    one_of(ALL_CHARACTERS),
                    3,
                )),
                char('\''),
            )),
        )),
        recognize(delimited(
            char('('),
            many0_count(
                alt((
                    code,
                    recognize(is_not(NO_QUOTE_APOSTROPHE_BRACKETS, MAX_CODE_CHARS)),
                )),
                MAX_SPECIAL_CODE,
            ),
            char(')'),
        )),
        recognize(delimited(
            char('['),
            many0_count(
                alt((
                    code,
                    recognize(is_not(NO_QUOTE_APOSTROPHE_BRACKETS, MAX_CODE_CHARS)),
                )),
                MAX_SPECIAL_CODE,
            ),
            char(']'),
        )),
        recognize(delimited(
            char('{'),
            many0_count(
                alt((
                    code,
                    recognize(is_not(NO_QUOTE_APOSTROPHE_BRACKETS, MAX_CODE_CHARS)),
                )),
                MAX_SPECIAL_CODE,
            ),
            char('}'),
        )),
    )))
    .gen(rng, buffer)
}

pub fn simple_path<R>(rng: &mut R, buffer: &mut String)
where
    R: AntiNomRng,
{
    recognize(tuple((
        opt(ts(tag("::"))),
        identifier,
        many0_count(pair(ps(tag("::")), ps(identifier)), MAX_SIMPLE_PATH_ITEMS),
    )))
    .gen(rng, buffer)
}

pub fn identifier<R>(rng: &mut R, buffer: &mut String)
where
    R: AntiNomRng,
{
    let mut id = String::new();
    recognize(tuple((
        alt((alpha1(MAX_IDENTIFIER_FRAGMENT_LENGTH), tag("_"))),
        many0_count(
            alt((alphanumeric1(MAX_IDENTIFIER_FRAGMENT_LENGTH), tag("_"))),
            MAX_IDENTIFIER_FRAGMENTS - 1,
        ),
    )))
    .gen(rng, &mut id);
    buffer.push_slice(&id);
    // Make sure we didn't push a keyword
    match id.as_str() {
        "as" | "use" => {
            let anarchy = rng.anarchy();
            if !anarchy {
                buffer.push('_');
            }
        }
        _ => {}
    }
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
