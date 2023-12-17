#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

impl Span {
    pub fn zero() -> Self {
        Self { start: 0, end: 0 }
    }

    pub fn span_of_str(s: &str, part: &str) -> Self {
        let input_start = s.as_ptr() as usize;
        let input_end = input_start + s.len();
        let start = part.as_ptr() as usize;
        let end = start + part.len();
        if start >= input_start && end <= input_end {
            Self {
                start: start - input_start,
                end: end - input_start,
            }
        } else {
            panic!("slices out of range");
        }
    }
}

#[derive(PartialEq, Eq, Clone, Copy, Debug, new)]
pub struct Location {
    pub line: usize,
    pub col: usize,
}

impl Location {
    pub fn from_source_and_span(src: &str, span: Span) -> Self {
        src.as_bytes()[0..span.start]
            .iter()
            .fold(Location { line: 1, col: 1 }, |loc, &b| match b {
                b'\n' => loc.next_line(),
                b => {
                    if b <= 0x7f {
                        // First byte of a UTF-8 character
                        loc.next_col()
                    } else {
                        // Extra byte of a UTF-8 character
                        loc
                    }
                }
            })
    }

    fn next_line(mut self) -> Self {
        self.line += 1;
        self.col = 1;
        self
    }

    fn next_col(mut self) -> Self {
        self.col += 1;
        self
    }
}
