use logos::Logos;
use peekmore::{PeekMore, PeekMoreIterator};

#[derive(PartialEq, Eq, Clone, Debug, Logos)]
#[logos(skip r"[ \t\n\r\f]+")]
pub enum Token<'a> {
    #[token("as")]
    As(&'a str),
    #[token(">")]
    CloseAngle(&'a str),
    #[token(")")]
    CloseBracket(&'a str),
    #[token("}")]
    CloseCurly(&'a str),
    #[token("]")]
    CloseSquare(&'a str),
    #[token("::")]
    Colon2(&'a str),
    #[token(",")]
    Comma(&'a str),
    #[token("-")]
    Dash(&'a str),
    #[regex(r"[A-Z_a-z][A-Z_a-z0-9]*")]
    Ident(&'a str),
    #[token("#")]
    Hash(&'a str),
    #[token("<")]
    OpenAngle(&'a str),
    #[token("(")]
    OpenBracket(&'a str),
    #[token("{")]
    OpenCurly(&'a str),
    #[token("[")]
    OpenSquare(&'a str),
    #[token(r"pub")]
    Pub(&'a str),
    #[regex(r#"'(\\.|[^\\'])*'"#)]
    QuotedChar(&'a str),
    #[regex(r###""|r#*""###, Token::parse_quoted_string)]
    QuotedString(&'a str),
    #[token(r";")]
    SemiColon(&'a str),
    #[token("*")]
    Star(&'a str),
    #[token(r"use")]
    Use(&'a str),
    // That is to catch invalid identifiers like "1foo" or "fooé" in one block instead of
    // breaking at the first non recognized character.
    #[regex(r"\w+", priority = 0)]
    NotAnIdent(&'a str),
    // Prevent falling into UnrecognizedToken
    #[regex(r#"[^ \t\n\r\f\w'"]"#)]
    Char(&'a str),
    UnrecognizedToken(&'a str),
}

impl<'a> Token<'a> {
    pub fn as_str(&self) -> &'a str {
        match self {
            Self::As(span) => span,
            Self::CloseAngle(span) => span,
            Self::CloseBracket(span) => span,
            Self::CloseCurly(span) => span,
            Self::CloseSquare(span) => span,
            Self::Colon2(span) => span,
            Self::Comma(span) => span,
            Self::Dash(span) => span,
            Self::Ident(span) => span,
            Self::Hash(span) => span,
            Self::OpenAngle(span) => span,
            Self::OpenBracket(span) => span,
            Self::OpenCurly(span) => span,
            Self::OpenSquare(span) => span,
            Self::Pub(span) => span,
            Self::QuotedChar(span) => span,
            Self::QuotedString(span) => span,
            Self::SemiColon(span) => span,
            Self::Star(span) => span,
            Self::Use(span) => span,
            // Other
            Self::NotAnIdent(span) => span,
            Self::Char(span) => span,
            Self::UnrecognizedToken(span) => span,
        }
    }

    fn parse_quoted_string(lexer: &mut logos::Lexer<'a, Self>) -> Option<&'a str> {
        let slice = lexer.slice();
        let found_end = match slice.chars().next() {
            Some('"') => {
                let rem = lexer.remainder();
                let mut end = 0;
                let found_end = loop {
                    let mut rem_chars = rem[end..].chars();
                    match rem_chars.next() {
                        Some('\\') => {
                            if let Some(escaped) = rem_chars.next() {
                                end += 1 + escaped.len_utf8();
                            } else {
                                end += 1;
                                break false;
                            }
                        }
                        Some('"') => {
                            end += 1;
                            break true;
                        }
                        Some(c) => {
                            end += c.len_utf8();
                        }
                        None => {
                            break false;
                        }
                    }
                };
                // Bump anyway (unclosed string)
                lexer.bump(end);
                found_end
            }
            Some('r') => {
                // Adaptation of implementation by matklad:
                // https://github.com/matklad/fall/blob/527ab331f82b8394949041bab668742868c0c282/lang/rust/syntax/src/rust.fall#L1294-L1324
                // Who needs more then 25 hashes anyway? :)
                let q_hashes = concat!('"', "######", "######", "######", "######", "######");
                let closing = &q_hashes[..lexer.slice().len() - 1]; // skip initial 'r'

                let end = lexer.remainder().find(closing);
                if let Some(end) = end {
                    lexer.bump(end + closing.len());
                } else {
                    lexer.bump(lexer.remainder().len());
                }
                end.is_some()
            }
            _ => false,
        };
        found_end.then(|| lexer.slice())
    }
}

pub struct Lexer<'a, I>
where
    I: Iterator<Item = Token<'a>>,
{
    input: &'a str,
    tokens: PeekMoreIterator<I>,
}

impl<'a, I> Lexer<'a, I>
where
    I: Iterator<Item = Token<'a>>,
{
    pub fn input(&self) -> &'a str {
        self.input
    }

    pub fn input_slice(&self, start: &'a str, end: &'a str) -> &'a str {
        let input_start = self.input.as_ptr() as usize;
        let input_end = input_start + self.input.len();
        let start = start.as_ptr() as usize;
        let end = end.as_ptr() as usize + end.len();
        if start >= input_start && end <= input_end {
            &self.input[(start - input_start)..(end - input_start)]
        } else {
            panic!("slices out of range");
        }
    }

    pub fn tokens(&mut self) -> &mut PeekMoreIterator<I> {
        &mut self.tokens
    }
}

pub fn lexer(input: &str) -> Lexer<impl Iterator<Item = Token>> {
    let tokens = Token::lexer(input)
        .spanned()
        .map(|(res, range)| res.unwrap_or_else(|()| Token::UnrecognizedToken(&input[range])));
    Lexer {
        input,
        tokens: tokens.peekmore(),
    }
}

#[cfg(test)]
mod tests {
    use logos::{Logos, Span};
    use rstest::rstest;

    use super::Token;

    #[rstest]
    #[case(r#"'\''"#, vec![(Ok(Token::QuotedChar(r#"'\''"#)), 0..4)])]
    #[case(r#"'\K\\'"#, vec![(Ok(Token::QuotedChar(r#"'\K\\'"#)), 0..6)])]
    #[case(r#"'\K\\')"#, vec![(Ok(Token::QuotedChar(r#"'\K\\'"#)), 0..6), (Ok(Token::CloseBracket(")")), 6..7)])]
    fn test_quoted_char(#[case] input: &str, #[case] expected: Vec<(Result<Token, ()>, Span)>) {
        let tokens = Token::lexer(input)
            .spanned()
            .collect::<Vec<(Result<Token, _>, Span)>>();
        assert_eq!(tokens, expected);
    }

    #[rstest]
    #[case(r#""\"""#, vec![(Ok(Token::QuotedString(r#""\"""#)), 0..4)])]
    #[case(r#""\K\\""#, vec![(Ok(Token::QuotedString(r#""\K\\""#)), 0..6)])]
    #[case(r#""\K\\")"#, vec![(Ok(Token::QuotedString(r#""\K\\""#)), 0..6), (Ok(Token::CloseBracket(")")), 6..7)])]
    #[case(r#""foo"#, vec![(Err(()), 0..4)])]
    fn test_quoted_string(#[case] input: &str, #[case] expected: Vec<(Result<Token, ()>, Span)>) {
        let tokens = Token::lexer(input)
            .spanned()
            .collect::<Vec<(Result<Token, _>, Span)>>();
        assert_eq!(tokens, expected);
    }

    #[rstest]
    #[case(r###"r#"foo"#"###, vec![(Ok(Token::QuotedString(r###"r#"foo"#"###)), 0..8)])]
    #[case(r#####"r###"foo"###"#####, vec![(Ok(Token::QuotedString(r#####"r###"foo"###"#####)), 0..12)])]
    #[case(r###"r#"foo""###, vec![(Err(()), 0..7)])]
    fn test_quoted_raw_string(
        #[case] input: &str,
        #[case] expected: Vec<(Result<Token, ()>, Span)>,
    ) {
        let tokens = Token::lexer(input)
            .spanned()
            .collect::<Vec<(Result<Token, _>, Span)>>();
        assert_eq!(tokens, expected);
    }

    #[rstest]
    #[case("foo", vec![(Ok(Token::Ident("foo")), 0..3)])]
    #[case("foo_bar", vec![(Ok(Token::Ident("foo_bar")), 0..7)])]
    #[case("_foo_bar", vec![(Ok(Token::Ident("_foo_bar")), 0..8)])]
    #[case("1foo", vec![(Ok(Token::NotAnIdent("1foo")), 0..4)])]
    #[case("fooé", vec![(Ok(Token::NotAnIdent("fooé")), 0..5)])]
    #[case("fooébar", vec![(Ok(Token::NotAnIdent("fooébar")), 0..8)])]
    fn test_not_an_ident(#[case] input: &str, #[case] expected: Vec<(Result<Token, ()>, Span)>) {
        let tokens = Token::lexer(input)
            .spanned()
            .collect::<Vec<(Result<Token, _>, Span)>>();
        assert_eq!(tokens, expected);
    }
}
