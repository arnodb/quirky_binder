use std::io::{BufRead, Stdin, StdinLock};

use fallible_iterator::FallibleIterator;

/// Reads a buffer and stream one item per line, `'0x0a' trimmed from the end.`
#[derive(new)]
pub struct ReadLines<I: BufRead> {
    input: I,
    #[new(default)]
    buffer: String,
}

impl<I: BufRead> FallibleIterator for ReadLines<I> {
    type Item = Box<str>;
    type Error = std::io::Error;

    fn next(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        self.buffer.clear();
        let read = self.input.read_line(&mut self.buffer)?;
        if read > 0 {
            let value = std::mem::take(&mut self.buffer);
            let value = value.trim_end_matches('\n');
            Ok(Some(value.into()))
        } else {
            Ok(None)
        }
    }
}

lazy_static! {
    static ref STDIN: Stdin = std::io::stdin();
}

#[derive(new)]
pub struct ReadStdinLines {
    #[new(value = "STDIN.lock()")]
    stdin_lock: StdinLock<'static>,
    #[new(default)]
    buffer: String,
}

impl FallibleIterator for ReadStdinLines {
    type Item = Box<str>;
    type Error = std::io::Error;

    fn next(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        self.buffer.clear();
        let read = self.stdin_lock.read_line(&mut self.buffer)?;
        if read > 0 {
            let value = std::mem::take(&mut self.buffer);
            let value = value.trim_end_matches('\n');
            Ok(Some(value.into()))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    #[test]
    fn should_stream_lines_from_string() {
        let input = "Hello\nWorld";
        let mut stream = ReadLines::new(input.as_bytes());
        assert_matches!(stream.next(), Ok(Some(line)) if &*line == "Hello");
        assert_matches!(stream.next(), Ok(Some(line)) if &*line == "World");
        // End of stream
        assert_matches!(stream.next(), Ok(None));
        assert_matches!(stream.next(), Ok(None));
    }
}
