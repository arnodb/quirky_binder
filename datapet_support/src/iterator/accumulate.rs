use fallible_iterator::FallibleIterator;
use std::collections::VecDeque;

/// Accumulates items in memory and stream them.
#[derive(new)]
pub struct Accumulate<I: FallibleIterator<Item = R, Error = E>, R, E> {
    input: I,
    #[new(default)]
    buffer: VecDeque<R>,
    #[new(value = "false")]
    finalizing: bool,
}

impl<I: FallibleIterator<Item = R, Error = E>, R, E> FallibleIterator for Accumulate<I, R, E> {
    type Item = R;
    type Error = E;

    fn next(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        if !self.finalizing {
            while let Some(rec) = self.input.next()? {
                self.buffer.push_back(rec);
            }
            self.finalizing = true;
        }
        Ok(self.buffer.pop_front())
    }
}

#[test]
fn should_accumulate_stream() {
    let mut stream = Accumulate::new(fallible_iterator::convert(
        ["ZZZ", "", "a", "z", "A"].into_iter().map(Ok::<_, ()>),
    ));
    assert_matches!(stream.next(), Ok(Some(v)) if v == "ZZZ");
    assert_matches!(stream.next(), Ok(Some(v)) if v == "");
    assert_matches!(stream.next(), Ok(Some(v)) if v == "a");
    assert_matches!(stream.next(), Ok(Some(v)) if v == "z");
    assert_matches!(stream.next(), Ok(Some(v)) if v == "A");
    // End of stream
    assert_matches!(stream.next(), Ok(None));
    assert_matches!(stream.next(), Ok(None));
}
