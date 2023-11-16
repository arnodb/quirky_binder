use std::ops::Range;

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
        [Some((Token::Colon2(_), _)), Some((Token::Ident(_), _))] | [Some((Token::Ident(_), _)), _]
    };
}

macro_rules! use_sub_tree_lookahead {
    () => {
        Some((Token::Star(_), _)) | Some((Token::OpenCurly(_), _))
    };
}

macro_rules! use_tree_lookahead {
    () => {
        simple_path_lookahead!() | [Some((Token::Colon2(_), _)), _] | [use_sub_tree_lookahead!(), _]
    };
}

#[allow(unused)]
macro_rules! use_declaration_lookahead {
    () => {
        Some((Token::Use(_), _))
    };
}

macro_rules! match_token {
    ($lexer:expr, $type:path, $res:expr, $err:expr) => {
        match $lexer.tokens().peek() {
            Some(($type(_), _)) => {
                let (token, range) = $lexer.tokens().next().unwrap();
                Ok(($res(&token), range))
            }
            Some((token, _)) => Err(SpannedError {
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
) -> Result<(UseDeclaration<'a>, Range<usize>), SpannedError<&'a str>>
where
    I: Iterator<Item = (Token<'a>, Range<usize>)>,
{
    let start = match_token!(
        lexer,
        Token::Use,
        |_| (),
        super::SpannedErrorKind::Token("use")
    )?
    .1
    .start;
    let (use_tree, _) = use_tree(lexer)?;
    let end = match_token!(
        lexer,
        Token::SemiColon,
        |_| (),
        super::SpannedErrorKind::Token(";")
    )?
    .1
    .end;
    Ok((
        UseDeclaration {
            use_tree: use_tree.into(),
        },
        start..end,
    ))
}

pub fn use_tree<'a, I>(
    lexer: &mut Lexer<'a, I>,
) -> Result<(&'a str, Range<usize>), SpannedError<&'a str>>
where
    I: Iterator<Item = (Token<'a>, Range<usize>)>,
{
    let start = lexer.tokens().peek().map(|(_, range)| range.start);
    let end = match lexer.tokens().peek_amount(2) {
        simple_path_lookahead!() => {
            let (_, range) = simple_path(lexer)?;
            match lexer.tokens().peek() {
                Some((Token::Colon2(_), _)) => {
                    lexer.tokens().next();
                    let (_, range) = use_sub_tree(lexer)?;
                    range.end
                }
                Some((Token::As(_), _)) => {
                    lexer.tokens().next();
                    let (_, range) = identifier(lexer)?;
                    range.end
                }
                use_sub_tree_lookahead!() => {
                    let (_, range) = use_sub_tree(lexer)?;
                    range.end
                }
                _ => range.end,
            }
        }
        _ => match lexer.tokens().peek() {
            Some((Token::Colon2(_), _)) => {
                lexer.tokens().next();
                let (_, range) = use_sub_tree(lexer)?;
                range.end
            }
            _ => {
                let (_, range) = use_sub_tree(lexer)?;
                range.end
            }
        },
    };
    let start = start.unwrap();
    Ok((&lexer.input()[start..end], start..end))
}

pub fn use_sub_tree<'a, I>(
    lexer: &mut Lexer<'a, I>,
) -> Result<(&'a str, Range<usize>), SpannedError<&'a str>>
where
    I: Iterator<Item = (Token<'a>, Range<usize>)>,
{
    let start = lexer.tokens().peek().map(|(_, range)| range.start);
    let end = match lexer.tokens().peek() {
        Some((Token::Star(_), _)) => {
            let (_, range) = lexer.tokens().next().unwrap();
            range.end
        }
        _ => {
            match_token!(
                lexer,
                Token::OpenCurly,
                |_| (),
                super::SpannedErrorKind::Token("{")
            )?;
            while let use_tree_lookahead!() = lexer.tokens().peek_amount(2) {
                use_tree(lexer)?;
                match lexer.tokens().peek() {
                    Some((Token::CloseCurly(_), _)) => break,
                    _ => {
                        match_token!(
                            lexer,
                            Token::Comma,
                            |_| (),
                            super::SpannedErrorKind::Token(",")
                        )?;
                    }
                }
            }
            let (_, range) = match_token!(
                lexer,
                Token::CloseCurly,
                |_| (),
                super::SpannedErrorKind::Token("}")
            )?;
            range.end
        }
    };
    let start = start.unwrap();
    Ok((&lexer.input()[start..end], start..end))
}

pub fn simple_path<'a, I>(
    lexer: &mut Lexer<'a, I>,
) -> Result<(&'a str, Range<usize>), SpannedError<&'a str>>
where
    I: Iterator<Item = (Token<'a>, Range<usize>)>,
{
    let start = lexer.tokens().peek().map(|(_, range)| range.start);
    if let Some((Token::Colon2(_), _)) = lexer.tokens().peek() {
        lexer.tokens().next();
    }
    let (_, range) = identifier(lexer)?;
    let mut end = range.end;
    // TODO catch NotAnIdent as a strong error, here and in nom_impl
    while let [Some((Token::Colon2(_), _)), Some((Token::Ident(_), _))] =
        lexer.tokens().peek_amount(2)
    {
        lexer.tokens().next();
        let (_, range) = identifier(lexer)?;
        end = range.end;
    }
    let start = start.unwrap();
    Ok((&lexer.input()[start..end], start..end))
}

pub fn identifier<'a, I>(
    lexer: &mut Lexer<'a, I>,
) -> Result<(&'a str, Range<usize>), SpannedError<&'a str>>
where
    I: Iterator<Item = (Token<'a>, Range<usize>)>,
{
    match_token!(
        lexer,
        Token::Ident,
        Token::as_str,
        super::SpannedErrorKind::Identifier
    )
}

#[cfg(test)]
mod tests {
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

    #[test]
    fn test_valid_identifier() {
        let input = "foo_123";
        let mut lexer = lexer(input);
        let (ident, _) = assert_matches!(identifier(&mut lexer), Ok(ident) => ident);
        assert_eq!(lexer.tokens().next(), None);
        assert_eq!(ident, "foo_123");
    }

    #[test]
    fn test_valid_identifier_underscore() {
        let input = "_";
        let mut lexer = lexer(input);
        let (ident, _) = assert_matches!(identifier(&mut lexer), Ok(ident) => ident);
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
        let (ident, _) = assert_matches!(identifier(&mut lexer), Ok(ident) => ident);
        let tail =
            assert_matches!(lexer.tokens().next(), Some((Token::OpenBracket(tail), _)) => tail);
        assert_span_at_distance!(input, tail, "(", 3, "tail");
        assert_eq!(ident, "caf");
    }
}
