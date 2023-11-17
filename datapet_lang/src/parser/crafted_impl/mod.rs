use crate::ast::UseDeclaration;

use self::lexer::{Lexer, Token};

use super::SpannedError;

pub mod lexer;

#[allow(unused)]
macro_rules! identifier_lookahead {
    () => {
        Some((Token::Ident(_), _))
    };
}

macro_rules! simple_path_lookahead {
    () => {
        // TODO catch NotAnIdent as a strong error, here and in nom_impl
        [Some(Token::Colon2(_)), Some(Token::Ident(_))] | [Some(Token::Ident(_)), _]
    };
}

macro_rules! use_sub_tree_lookahead {
    () => {
        Some(Token::Star(_)) | Some(Token::OpenCurly(_))
    };
}

macro_rules! use_tree_lookahead {
    () => {
        simple_path_lookahead!() | [Some(Token::Colon2(_)), _] | [use_sub_tree_lookahead!(), _]
    };
}

#[allow(unused)]
macro_rules! use_declaration_lookahead {
    () => {
        Some(Token::Use(_))
    };
}

macro_rules! match_token {
    ($lexer:expr, $type:path, $err:expr) => {
        match $lexer.tokens().peek() {
            Some($type(_)) => {
                let token = $lexer.tokens().next().unwrap();
                Ok(token.as_str())
            }
            Some(token) => Err(SpannedError {
                kind: $err,
                span: token.as_str(),
            }),
            None => Err(SpannedError {
                kind: $err,
                span: &$lexer.input()[$lexer.input().len()..],
            }),
        }
    };
}

pub fn use_declaration<'a, I>(
    lexer: &mut Lexer<'a, I>,
) -> Result<UseDeclaration<'a>, SpannedError<&'a str>>
where
    I: Iterator<Item = Token<'a>>,
{
    match_token!(lexer, Token::Use, super::SpannedErrorKind::Token("use"))?;
    let use_tree = use_tree(lexer)?;
    match_token!(lexer, Token::SemiColon, super::SpannedErrorKind::Token(";"))?;
    Ok(UseDeclaration {
        use_tree: use_tree.into(),
    })
}

pub fn use_tree<'a, I>(lexer: &mut Lexer<'a, I>) -> Result<&'a str, SpannedError<&'a str>>
where
    I: Iterator<Item = Token<'a>>,
{
    let (start, end) = match lexer.tokens().peek_amount(2) {
        simple_path_lookahead!() => {
            let sp = simple_path(lexer)?;
            match lexer.tokens().peek() {
                Some(Token::Colon2(_)) => {
                    lexer.tokens().next().unwrap();
                    let ust = use_sub_tree(lexer)?;
                    (sp, ust)
                }
                Some(Token::As(_)) => {
                    lexer.tokens().next().unwrap();
                    let ident = identifier(lexer)?;
                    (sp, ident)
                }
                use_sub_tree_lookahead!() => {
                    let ust = use_sub_tree(lexer)?;
                    (sp, ust)
                }
                _ => (sp, sp),
            }
        }
        _ => match lexer.tokens().peek() {
            Some(Token::Colon2(_)) => {
                let colon2 = lexer.tokens().next().unwrap();
                let ust = use_sub_tree(lexer)?;
                (colon2.as_str(), ust)
            }
            _ => {
                let ust = use_sub_tree(lexer)?;
                (ust, ust)
            }
        },
    };
    Ok(lexer.input_slice(start, end))
}

pub fn use_sub_tree<'a, I>(lexer: &mut Lexer<'a, I>) -> Result<&'a str, SpannedError<&'a str>>
where
    I: Iterator<Item = Token<'a>>,
{
    let (start, end) = match lexer.tokens().peek() {
        Some(Token::Star(_)) => {
            let star = lexer.tokens().next().unwrap();
            (star.as_str(), star.as_str())
        }
        _ => {
            let open = match_token!(lexer, Token::OpenCurly, super::SpannedErrorKind::Token("{"))?;
            while let use_tree_lookahead!() = lexer.tokens().peek_amount(2) {
                use_tree(lexer)?;
                match lexer.tokens().peek() {
                    Some(Token::CloseCurly(_)) => break,
                    _ => {
                        match_token!(lexer, Token::Comma, super::SpannedErrorKind::Token(","))?;
                    }
                }
            }
            let close = match_token!(
                lexer,
                Token::CloseCurly,
                super::SpannedErrorKind::Token("}")
            )?;
            (open, close)
        }
    };
    Ok(lexer.input_slice(start, end))
}

pub fn opt_streams0<'a, I>(
    lexer: &mut Lexer<'a, I>,
) -> Result<Option<Vec<&'a str>>, SpannedError<&'a str>>
where
    I: Iterator<Item = Token<'a>>,
{
    if let Some(Token::OpenSquare(_)) = lexer.tokens().peek() {
        streams0(lexer).map(Some)
    } else {
        Ok(None)
    }
}

fn streams0<'a, I>(lexer: &mut Lexer<'a, I>) -> Result<Vec<&'a str>, SpannedError<&'a str>>
where
    I: Iterator<Item = Token<'a>>,
{
    let mut streams = Vec::new();
    match_token!(
        lexer,
        Token::OpenSquare,
        super::SpannedErrorKind::Token("[")
    )?;
    while let Some(Token::Ident(_)) = lexer.tokens().peek() {
        let ident = identifier(lexer)?;
        streams.push(ident);
        match lexer.tokens().peek() {
            Some(Token::CloseSquare(_)) => break,
            _ => {
                match_token!(lexer, Token::Comma, super::SpannedErrorKind::Token(","))?;
            }
        }
    }
    match_token!(
        lexer,
        Token::CloseSquare,
        super::SpannedErrorKind::Token("]")
    )?;
    Ok(streams)
}

pub fn simple_path<'a, I>(lexer: &mut Lexer<'a, I>) -> Result<&'a str, SpannedError<&'a str>>
where
    I: Iterator<Item = Token<'a>>,
{
    let start = if let Some(Token::Colon2(_)) = lexer.tokens().peek() {
        let colon2 = lexer.tokens().next().unwrap();
        Some(colon2.as_str())
    } else {
        None
    };
    let ident = identifier(lexer)?;
    let start = start.unwrap_or(ident);
    let mut end = ident;
    // TODO catch NotAnIdent as a strong error, here and in nom_impl
    while let [Some(Token::Colon2(_)), Some(Token::Ident(_))] = lexer.tokens().peek_amount(2) {
        lexer.tokens().next().unwrap();
        let ident = identifier(lexer)?;
        end = ident;
    }
    Ok(lexer.input_slice(start, end))
}

pub fn identifier<'a, I>(lexer: &mut Lexer<'a, I>) -> Result<&'a str, SpannedError<&'a str>>
where
    I: Iterator<Item = Token<'a>>,
{
    match_token!(lexer, Token::Ident, super::SpannedErrorKind::Identifier)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use crate::parser::{crafted_impl::lexer::lexer, SpannedErrorKind};

    use super::*;

    macro_rules! assert_span_at_distance {
        ($input:ident, $span:expr, $expected:expr, $expected_distance:expr) => {
            assert_span_at_distance!($input, $span, $expected, $expected_distance, "span")
        };
        ($input:ident, $span:expr, $expected:expr, $expected_distance:expr, $what:literal) => {
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
        let mut lexer = lexer(input);
        let ud = assert_matches!(use_declaration(&mut lexer), Ok(ud) => ud);
        assert_eq!(ud.use_tree, expected_use_tree);
    }

    #[rstest]
    #[case("use foo::{bar::,mar};", SpannedErrorKind::Token("{"), ",", "use foo::{bar::".as_bytes().len())]
    #[case("use foo as;", SpannedErrorKind::Identifier, ";", "use foo as".as_bytes().len())]
    #[case("use foo as\u{20};", SpannedErrorKind::Identifier, ";", "use foo as\u{20}".as_bytes().len())]
    #[case("use foo::{::,};", SpannedErrorKind::Token("{"), ",", "use foo::{::".as_bytes().len())]
    fn test_invalid_use_declaration(
        #[case] input: &str,
        #[case] expected_kind: SpannedErrorKind,
        #[case] expected_span: &str,
        #[case] expected_distance: usize,
    ) {
        let mut lexer = lexer(input);
        let (kind, span) = assert_matches!(
            use_declaration(&mut lexer),
            Err(SpannedError { kind, span }) => (kind, span)
        );
        assert_eq!(kind, expected_kind);
        assert_span_at_distance!(input, span, expected_span, expected_distance);
    }

    #[test]
    fn test_valid_identifier() {
        let input = "foo_123";
        let mut lexer = lexer(input);
        let ident = assert_matches!(identifier(&mut lexer), Ok(ident) => ident);
        assert_eq!(lexer.tokens().next(), None);
        assert_eq!(ident, "foo_123");
    }

    #[test]
    fn test_valid_identifier_underscore() {
        let input = "_";
        let mut lexer = lexer(input);
        let ident = assert_matches!(identifier(&mut lexer), Ok(ident) => ident);
        assert_eq!(lexer.tokens().next(), None);
        assert_eq!(ident, "_");
    }

    #[test]
    fn test_identifier_extra_alphabetic_char() {
        let input = "café";
        let mut lexer = lexer(input);
        let (kind, span) = assert_matches!(
            identifier(&mut lexer),
            Err(SpannedError { kind, span }) => (kind, span)
        );
        assert_eq!(kind, SpannedErrorKind::Identifier);
        assert_span_at_distance!(input, span, "café", 0);
    }

    #[test]
    fn test_identifier_extra_non_alphabetic_char() {
        let input = "caf(";
        let mut lexer = lexer(input);
        let ident = assert_matches!(identifier(&mut lexer), Ok(ident) => ident);
        let tail = assert_matches!(lexer.tokens().next(), Some(Token::OpenBracket(tail)) => tail);
        assert_span_at_distance!(input, tail, "(", 3, "tail");
        assert_eq!(ident, "caf");
    }
}
