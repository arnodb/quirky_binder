use crate::ast::{
    ConnectedFilter, Filter, Graph, GraphDefinition, GraphDefinitionSignature, Module, StreamLine,
    StreamLineInput, StreamLineOutput, UseDeclaration,
};

use self::lexer::{Lexer, Token};

use super::{assemble_inputs, SpannedError, SpannedErrorKind};

pub mod lexer;

macro_rules! identifier_lookahead {
    () => {
        Some(Token::Ident(_)) | Some(Token::NotAnIdent(_))
    };
}

macro_rules! simple_path_lookahead {
    () => {
        [Some(Token::Colon2(_)), Some(Token::Ident(_))]
            | [Some(Token::Colon2(_)), Some(Token::NotAnIdent(_))]
            | [Some(Token::Ident(_)), _]
            | [Some(Token::NotAnIdent(_)), _]
    };
}

macro_rules! streams0_lookahead {
    () => {
        Some(Token::OpenSquare(_))
    };
}

macro_rules! stream_line_input_lookahead {
    () => {
        Some(Token::OpenAngle(_)) | Some(Token::Dash(_))
    };
}

macro_rules! graph_definition_signature_lookahead {
    () => {
        streams0_lookahead!() | identifier_lookahead!()
    };
}

macro_rules! graph_definition_lookahead {
    () => {
        Some(Token::Pub(_)) | graph_definition_signature_lookahead!()
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

pub fn module<'a, I>(lexer: &mut Lexer<'a, I>) -> Result<Module<'a>, SpannedError<&'a str>>
where
    I: Iterator<Item = Token<'a>>,
{
    let mut items = Vec::new();
    loop {
        match lexer.tokens().peek() {
            use_declaration_lookahead!() => {
                let item = use_declaration(lexer)?;
                items.push(item.into());
            }
            graph_definition_lookahead!() => {
                let item = graph_definition(lexer)?;
                items.push(item.into());
            }
            Some(_) => {
                let item = graph(lexer)?;
                items.push(item.into());
            }
            None => break,
        }
    }
    Ok(Module { items })
}

pub fn use_declaration<'a, I>(
    lexer: &mut Lexer<'a, I>,
) -> Result<UseDeclaration<'a>, SpannedError<&'a str>>
where
    I: Iterator<Item = Token<'a>>,
{
    match_token!(lexer, Token::Use, SpannedErrorKind::Token("use"))?;
    let use_tree = use_tree(lexer)?;
    match_token!(lexer, Token::SemiColon, SpannedErrorKind::Token(";"))?;
    Ok(UseDeclaration { use_tree })
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
            let open = match_token!(lexer, Token::OpenCurly, SpannedErrorKind::Token("{"))?;
            while let use_tree_lookahead!() = lexer.tokens().peek_amount(2) {
                use_tree(lexer)?;
                match lexer.tokens().peek() {
                    Some(Token::Comma(_)) => {
                        lexer.tokens().next().unwrap();
                    }
                    _ => break,
                }
            }
            let close = match_token!(lexer, Token::CloseCurly, SpannedErrorKind::Token("}"))?;
            (open, close)
        }
    };
    Ok(lexer.input_slice(start, end))
}

pub fn graph_definition<'a, I>(
    lexer: &mut Lexer<'a, I>,
) -> Result<GraphDefinition<'a>, SpannedError<&'a str>>
where
    I: Iterator<Item = Token<'a>>,
{
    let visibility = if let Some(Token::Pub(_)) = lexer.tokens().peek() {
        Some(lexer.tokens().next().unwrap().as_str())
    } else {
        None
    };
    let signature = graph_definition_signature(lexer)?;
    let stream_lines = stream_lines(lexer)?;
    Ok(GraphDefinition {
        signature,
        stream_lines,
        visible: visibility.is_some(),
    })
}

pub fn graph_definition_signature<'a, I>(
    lexer: &mut Lexer<'a, I>,
) -> Result<GraphDefinitionSignature<'a>, SpannedError<&'a str>>
where
    I: Iterator<Item = Token<'a>>,
{
    let inputs = opt_streams0(lexer)?;
    let name = identifier(lexer)?;
    let params = params(lexer)?;
    let outputs = opt_streams0(lexer)?;
    Ok(GraphDefinitionSignature {
        inputs,
        name,
        params,
        outputs,
    })
}

pub fn params<'a, I>(lexer: &mut Lexer<'a, I>) -> Result<Vec<&'a str>, SpannedError<&'a str>>
where
    I: Iterator<Item = Token<'a>>,
{
    match_token!(lexer, Token::OpenBracket, SpannedErrorKind::Token("("))?;
    let mut params = Vec::new();
    while let identifier_lookahead!() = lexer.tokens().peek() {
        let param = identifier(lexer)?;
        params.push(param);
        match lexer.tokens().peek() {
            Some(Token::Comma(_)) => {
                lexer.tokens().next().unwrap();
            }
            _ => break,
        }
    }
    match_token!(lexer, Token::CloseBracket, SpannedErrorKind::Token(")"))?;
    Ok(params)
}

pub fn graph<'a, I>(lexer: &mut Lexer<'a, I>) -> Result<Graph<'a>, SpannedError<&'a str>>
where
    I: Iterator<Item = Token<'a>>,
{
    Ok(Graph {
        stream_lines: stream_lines(lexer)?,
    })
}

pub fn stream_lines<'a, I>(
    lexer: &mut Lexer<'a, I>,
) -> Result<Vec<StreamLine<'a>>, SpannedError<&'a str>>
where
    I: Iterator<Item = Token<'a>>,
{
    match_token!(lexer, Token::OpenCurly, SpannedErrorKind::Token("{"))?;
    let mut stream_lines = Vec::new();
    loop {
        match lexer.tokens().peek() {
            Some(Token::CloseCurly(_)) => {
                break;
            }
            _ => {
                let stream_line = stream_line(lexer)?;
                stream_lines.push(stream_line);
            }
        }
    }
    match_token!(lexer, Token::CloseCurly, SpannedErrorKind::Token("}"))?;
    Ok(stream_lines)
}

pub fn stream_line<'a, I>(lexer: &mut Lexer<'a, I>) -> Result<StreamLine<'a>, SpannedError<&'a str>>
where
    I: Iterator<Item = Token<'a>>,
{
    match_token!(lexer, Token::OpenBracket, SpannedErrorKind::Token("("))?;
    let first_filter = {
        let inputs = if let stream_line_input_lookahead!() = lexer.tokens().peek() {
            stream_line_inputs(lexer)?
        } else {
            Vec::default()
        };
        let filter = filter(lexer)?;
        ConnectedFilter { inputs, filter }
    };
    let mut filters = vec![first_filter];
    let output = loop {
        match lexer.tokens().peek_amount(2) {
            [Some(Token::Dash(_)), Some(Token::CloseBracket(_))] => {
                let main_output = lexer.tokens().next().unwrap().as_str();
                break Some(StreamLineOutput::Main(main_output));
            }
            [Some(Token::Dash(_)), Some(Token::CloseAngle(_))] => {
                lexer.tokens().next().unwrap();
                lexer.tokens().next().unwrap();
                break Some(StreamLineOutput::Named(identifier(lexer)?));
            }
            [Some(Token::CloseBracket(_)), _] => {
                break None;
            }
            _ => {
                let filter = stream_line_filter(lexer)?;
                filters.push(filter);
            }
        }
    };
    match_token!(lexer, Token::CloseBracket, SpannedErrorKind::Token(")"))?;
    Ok(StreamLine { filters, output })
}

pub fn stream_line_inputs<'a, I>(
    lexer: &mut Lexer<'a, I>,
) -> Result<Vec<StreamLineInput<'a>>, SpannedError<&'a str>>
where
    I: Iterator<Item = Token<'a>>,
{
    let main_input = if let Some(Token::OpenAngle(_)) = lexer.tokens().peek() {
        lexer.tokens().next().unwrap();
        Some(identifier(lexer)?)
    } else {
        None
    };
    let (main_stream, extra_streams) = filter_input_streams(lexer)?;
    Ok(assemble_inputs(
        main_input.map_or_else(
            || StreamLineInput::Main(main_stream),
            StreamLineInput::Named,
        ),
        extra_streams,
    ))
}

pub fn stream_line_filter<'a, I>(
    lexer: &mut Lexer<'a, I>,
) -> Result<ConnectedFilter<'a>, SpannedError<&'a str>>
where
    I: Iterator<Item = Token<'a>>,
{
    let (main_stream, extra_streams) = filter_input_streams(lexer)?;
    let filter = filter(lexer)?;
    Ok(ConnectedFilter {
        inputs: assemble_inputs(StreamLineInput::Main(main_stream), extra_streams),
        filter,
    })
}

pub fn filter_input_streams<'a, I>(
    lexer: &mut Lexer<'a, I>,
) -> Result<(&'a str, Vec<&'a str>), SpannedError<&'a str>>
where
    I: Iterator<Item = Token<'a>>,
{
    let main = match_token!(lexer, Token::Dash, SpannedErrorKind::Token("-"))?;
    let streams = opt_streams1(lexer)?;
    Ok((main, streams.unwrap_or_default()))
}

pub fn filter<'a, I>(lexer: &mut Lexer<'a, I>) -> Result<Filter<'a>, SpannedError<&'a str>>
where
    I: Iterator<Item = Token<'a>>,
{
    let name = simple_path(lexer)?;
    let alias = if let Some(Token::Hash(_)) = lexer.tokens().peek() {
        lexer.tokens().next().unwrap();
        Some(identifier(lexer)?)
    } else {
        None
    };
    let params = filter_params(lexer)?;
    let extra_streams = opt_streams1(lexer)?;
    Ok(Filter {
        name,
        alias,
        params,
        extra_outputs: extra_streams.unwrap_or_default(),
    })
}

pub fn filter_params<'a, I>(lexer: &mut Lexer<'a, I>) -> Result<&'a str, SpannedError<&'a str>>
where
    I: Iterator<Item = Token<'a>>,
{
    let start = match_token!(lexer, Token::OpenBracket, SpannedErrorKind::Token("("))?;
    loop {
        match lexer.tokens().peek() {
            Some(Token::CloseBracket(_)) => {
                break;
            }
            Some(Token::QuotedChar(_))
            | Some(Token::QuotedString(_))
            | Some(Token::OpenBracket(_))
            | Some(Token::OpenCurly(_))
            | Some(Token::OpenSquare(_)) => {
                code(lexer)?;
            }
            Some(Token::As(_))
            | Some(Token::CloseAngle(_))
            | Some(Token::Colon2(_))
            | Some(Token::Comma(_))
            | Some(Token::Dash(_))
            | Some(Token::Ident(_))
            | Some(Token::Hash(_))
            | Some(Token::OpenAngle(_))
            | Some(Token::Pub(_))
            | Some(Token::SemiColon(_))
            | Some(Token::Star(_))
            | Some(Token::Use(_))
            | Some(Token::NotAnIdent(_))
            | Some(Token::Char(_)) => {
                lexer.tokens().next().unwrap();
            }
            Some(Token::CloseCurly(_)) | Some(Token::CloseSquare(_)) | None => {
                break;
            }
            Some(Token::UnrecognizedToken(span)) => {
                return match span.chars().next() {
                    Some('\'') => Err(SpannedError {
                        kind: SpannedErrorKind::UnterminatedChar,
                        span: &span[0..1],
                    }),
                    Some('"') => Err(SpannedError {
                        kind: SpannedErrorKind::UnterminatedString,
                        span: &span[0..1],
                    }),
                    _ => Err(SpannedError {
                        kind: SpannedErrorKind::UnrecognizedToken,
                        span,
                    }),
                };
            }
        }
    }
    let end = match_token!(lexer, Token::CloseBracket, SpannedErrorKind::Token(")"))?;
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
    match_token!(lexer, Token::OpenSquare, SpannedErrorKind::Token("["))?;
    while let identifier_lookahead!() = lexer.tokens().peek() {
        let ident = identifier(lexer)?;
        streams.push(ident);
        match lexer.tokens().peek() {
            Some(Token::Comma(_)) => {
                lexer.tokens().next().unwrap();
            }
            _ => break,
        }
    }
    match_token!(lexer, Token::CloseSquare, SpannedErrorKind::Token("]"))?;
    Ok(streams)
}

pub fn opt_streams1<'a, I>(
    lexer: &mut Lexer<'a, I>,
) -> Result<Option<Vec<&'a str>>, SpannedError<&'a str>>
where
    I: Iterator<Item = Token<'a>>,
{
    if let Some(Token::OpenSquare(_)) = lexer.tokens().peek() {
        streams1(lexer).map(Some)
    } else {
        Ok(None)
    }
}

pub fn streams1<'a, I>(lexer: &mut Lexer<'a, I>) -> Result<Vec<&'a str>, SpannedError<&'a str>>
where
    I: Iterator<Item = Token<'a>>,
{
    let mut streams = Vec::new();
    match_token!(lexer, Token::OpenSquare, SpannedErrorKind::Token("["))?;
    let ident = identifier(lexer)?;
    streams.push(ident);
    if let Some(Token::Comma(_)) = lexer.tokens().peek() {
        lexer.tokens().next().unwrap();
        while let identifier_lookahead!() = lexer.tokens().peek() {
            let ident = identifier(lexer)?;
            streams.push(ident);
            match lexer.tokens().peek() {
                Some(Token::Comma(_)) => {
                    lexer.tokens().next().unwrap();
                }
                _ => break,
            }
        }
    }
    match_token!(lexer, Token::CloseSquare, SpannedErrorKind::Token("]"))?;
    Ok(streams)
}

pub fn code<'a, I>(lexer: &mut Lexer<'a, I>) -> Result<&'a str, SpannedError<&'a str>>
where
    I: Iterator<Item = Token<'a>>,
{
    let mut brackets = Vec::new();
    let mut start_end = None;
    while let Some(token) = lexer.tokens().peek() {
        let push = match token {
            Token::OpenBracket(_) => true,
            Token::OpenCurly(_) => true,
            Token::OpenSquare(_) => true,
            Token::CloseBracket(_) => match brackets.last() {
                Some(Token::OpenBracket(_)) => {
                    brackets.pop();
                    false
                }
                Some(last) => {
                    return Err(SpannedError {
                        kind: SpannedErrorKind::UnbalancedCode,
                        span: last.as_str(),
                    });
                }
                None => break,
            },
            Token::CloseCurly(_) => match brackets.last() {
                Some(Token::OpenCurly(_)) => {
                    brackets.pop();
                    false
                }
                Some(last) => {
                    return Err(SpannedError {
                        kind: SpannedErrorKind::UnbalancedCode,
                        span: last.as_str(),
                    });
                }
                None => break,
            },
            Token::CloseSquare(_) => match brackets.last() {
                Some(Token::OpenSquare(_)) => {
                    brackets.pop();
                    false
                }
                Some(last) => {
                    return Err(SpannedError {
                        kind: SpannedErrorKind::UnbalancedCode,
                        span: last.as_str(),
                    });
                }
                None => break,
            },
            Token::UnrecognizedToken(span) => {
                return match span.chars().next() {
                    Some('\'') => Err(SpannedError {
                        kind: SpannedErrorKind::UnterminatedChar,
                        span: &span[0..1],
                    }),
                    Some('"') => Err(SpannedError {
                        kind: SpannedErrorKind::UnterminatedString,
                        span: &span[0..1],
                    }),
                    _ => Err(SpannedError {
                        kind: SpannedErrorKind::UnrecognizedToken,
                        span,
                    }),
                };
            }
            _ => false,
        };
        let eaten = lexer.tokens().next().unwrap();
        if let Some((_start, end)) = start_end.as_mut() {
            *end = eaten.as_str();
        } else {
            start_end = Some((eaten.as_str(), eaten.as_str()));
        }
        if push {
            brackets.push(eaten);
        }
    }
    if let Some(last) = brackets.last() {
        return Err(SpannedError {
            kind: SpannedErrorKind::UnbalancedCode,
            span: last.as_str(),
        });
    } else if let Some((start, end)) = start_end {
        return Ok(lexer.input_slice(start, end));
    } else {
        return Err(SpannedError {
            kind: SpannedErrorKind::Code,
            span: if let Some(next) = lexer.tokens().peek() {
                next.as_str()
            } else {
                &lexer.input()[lexer.input().len()..]
            },
        });
    }
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
    while let [Some(Token::Colon2(_)), Some(Token::Ident(_))]
    | [Some(Token::Colon2(_)), Some(Token::NotAnIdent(_))] = lexer.tokens().peek_amount(2)
    {
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
    match_token!(lexer, Token::Ident, SpannedErrorKind::Identifier)
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
    #[case("use", SpannedErrorKind::Token("{"), "", "use".as_bytes().len())]
    #[case("use foo::{bar::,mar};", SpannedErrorKind::Token("{"), ",", "use foo::{bar::".as_bytes().len())]
    #[case("use foo as;", SpannedErrorKind::Identifier, ";", "use foo as".as_bytes().len())]
    #[case("use foo as\u{20};", SpannedErrorKind::Identifier, ";", "use foo as\u{20}".as_bytes().len())]
    #[case("use foo::{::,};", SpannedErrorKind::Token("{"), ",", "use foo::{::".as_bytes().len())]
    #[case("use foo::as bar;", SpannedErrorKind::Token("{"), "as", "use foo::".as_bytes().len())]
    #[case("use ::{::{foo};", SpannedErrorKind::Token("}"), ";", "use ::{::{foo]".as_bytes().len())]
    #[case("use foo asbar;", SpannedErrorKind::Token(";"), "asbar", "use foo ".as_bytes().len())]
    #[case("use ::{foo bar};", SpannedErrorKind::Token("}"), "bar", "use ::{foo ".as_bytes().len())]
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

    #[rstest]
    #[case("foo{bar", SpannedErrorKind::UnbalancedCode, "{", "foo".as_bytes().len())]
    #[case("foo{(bar", SpannedErrorKind::UnbalancedCode, "(", "foo{".as_bytes().len())]
    #[case("(foo\"bar", SpannedErrorKind::UnterminatedString, "\"", "(foo".as_bytes().len())]
    #[case("foo'bar", SpannedErrorKind::UnterminatedChar, "'", "foo".as_bytes().len())]
    #[case("foo'bar\\", SpannedErrorKind::UnterminatedChar, "'", "foo".as_bytes().len())]
    fn test_invalid_code(
        #[case] input: &str,
        #[case] expected_kind: SpannedErrorKind,
        #[case] expected_span: &str,
        #[case] expected_distance: usize,
    ) {
        let mut lexer = lexer(input);
        let (kind, span) = assert_matches!(
            code(&mut lexer),
            Err(SpannedError { kind, span }) => (kind, span)
        );
        assert_eq!(kind, expected_kind);
        assert_span_at_distance!(input, span, expected_span, expected_distance);
    }

    #[rstest]
    #[case("[2foo]", SpannedErrorKind::Identifier, "2foo", "[".as_bytes().len())]
    #[case("[foo, 2bar]", SpannedErrorKind::Identifier, "2bar", "[foo, ".as_bytes().len())]
    #[case("[, foo]", SpannedErrorKind::Token("]"), ",", "[".as_bytes().len())]
    #[case("[foo bar]", SpannedErrorKind::Token("]"), "bar", "[foo ".as_bytes().len())]
    fn test_invalid_opt_streams0(
        #[case] input: &str,
        #[case] expected_kind: SpannedErrorKind,
        #[case] expected_span: &str,
        #[case] expected_distance: usize,
    ) {
        let mut lexer = lexer(input);
        let (kind, span) = assert_matches!(
            opt_streams0(&mut lexer),
            Err(SpannedError { kind, span }) => (kind, span)
        );
        assert_eq!(kind, expected_kind);
        assert_span_at_distance!(input, span, expected_span, expected_distance);
    }

    #[rstest]
    #[case("[2foo]", SpannedErrorKind::Identifier, "2foo", "[".as_bytes().len())]
    #[case("[foo, 2bar]", SpannedErrorKind::Identifier, "2bar", "[foo, ".as_bytes().len())]
    #[case("[, foo]", SpannedErrorKind::Identifier, ",", "[".as_bytes().len())]
    #[case("[foo bar]", SpannedErrorKind::Token("]"), "bar", "[foo ".as_bytes().len())]
    fn test_invalid_opt_streams1(
        #[case] input: &str,
        #[case] expected_kind: SpannedErrorKind,
        #[case] expected_span: &str,
        #[case] expected_distance: usize,
    ) {
        let mut lexer = lexer(input);
        let (kind, span) = assert_matches!(
            opt_streams1(&mut lexer),
            Err(SpannedError { kind, span }) => (kind, span)
        );
        assert_eq!(kind, expected_kind);
        assert_span_at_distance!(input, span, expected_span, expected_distance);
    }

    #[rstest]
    #[case("::2bar", SpannedErrorKind::Identifier, "2bar", "::".as_bytes().len())]
    #[case("foo::2bar", SpannedErrorKind::Identifier, "2bar", "foo::".as_bytes().len())]
    #[case(":: :: foo", SpannedErrorKind::Identifier, "::", ":: ".as_bytes().len())]
    fn test_invalid_simple_path(
        #[case] input: &str,
        #[case] expected_kind: SpannedErrorKind,
        #[case] expected_span: &str,
        #[case] expected_distance: usize,
    ) {
        let mut lexer = lexer(input);
        let (kind, span) = assert_matches!(
            simple_path(&mut lexer),
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
