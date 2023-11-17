use logos::Logos;
use peekmore::{PeekMore, PeekMoreIterator};

#[derive(PartialEq, Eq, Clone, Debug, Logos)]
#[logos(skip r"[ \t\n\r\f]+")]
pub enum Token<'a> {
    #[token("as")]
    As(&'a str),
    #[token(")")]
    CloseBracket(&'a str),
    #[token("}")]
    CloseCurly(&'a str),
    #[token("::")]
    Colon2(&'a str),
    #[token(",")]
    Comma(&'a str),
    #[regex(r"[A-Z_a-z][A-Z_a-z0-9]*")]
    Ident(&'a str),
    #[token("(")]
    OpenBracket(&'a str),
    #[token("{")]
    OpenCurly(&'a str),
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
    UnrecognizedToken(&'a str),
}

impl<'a> Token<'a> {
    pub fn as_str(&self) -> &'a str {
        match self {
            Self::As(span) => span,
            Self::CloseBracket(span) => span,
            Self::CloseCurly(span) => span,
            Self::Colon2(span) => span,
            Self::Comma(span) => span,
            Self::Ident(span) => span,
            Self::OpenBracket(span) => span,
            Self::OpenCurly(span) => span,
            Self::SemiColon(span) => span,
            Self::Star(span) => span,
            Self::Use(span) => span,
            // Other
            Self::NotAnIdent(span) => span,
            Self::UnrecognizedToken(span) => span,
        }
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
        let start = start.as_ptr() as usize;
        let end = end.as_ptr() as usize + end.len();
        let input = self.input.as_ptr() as usize;
        if start >= input && end <= input + self.input.len() {
            &self.input[(start - input)..(end - input)]
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
    #[case("foo", vec![(Ok(Token::Ident("foo")), 0..3)])]
    #[case("foo_bar", vec![(Ok(Token::Ident("foo_bar")), 0..7)])]
    #[case("_foo_bar", vec![(Ok(Token::Ident("_foo_bar")), 0..8)])]
    #[case("1foo", vec![(Ok(Token::NotAnIdent("1foo")), 0..4)])]
    #[case("fooé", vec![(Ok(Token::NotAnIdent("fooé")), 0..5)])]
    #[case("fooébar", vec![(Ok(Token::NotAnIdent("fooébar")), 0..8)])]
    fn test_unrecognized_token(
        #[case] input: &str,
        #[case] expected: Vec<(Result<Token, ()>, Span)>,
    ) {
        let tokens = Token::lexer(input)
            .spanned()
            .collect::<Vec<(Result<Token, _>, Span)>>();
        assert_eq!(tokens, expected);
    }
}
