use std::marker::PhantomData;

use fallible_iterator::FallibleIterator;
use serde::{Deserialize, Serialize};

use crate::data::buffer::{Buffer, BufferReader};

/// Accumulates items in memory and stream them.
#[derive(new)]
pub struct Accumulate<
    'de,
    I: FallibleIterator<Item = R, Error = E>,
    R: Serialize + Deserialize<'de>,
    E,
> {
    input: I,
    #[new(value = "State::Buffering")]
    state: State,
    _de: PhantomData<&'de ()>,
}

impl<
        'de,
        I: FallibleIterator<Item = R, Error = E>,
        R: Serialize + Deserialize<'de>,
        E: From<bincode::Error>,
    > FallibleIterator for Accumulate<'de, I, R, E>
{
    type Item = R;
    type Error = E;

    fn next(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        if let State::Buffering = &self.state {
            let mut buffer = Buffer::new();
            let mut written = 0;
            while let Some(record) = self.input.next()? {
                buffer.push(record)?;
                written += 1;
            }
            self.state = State::Reading {
                buffer: buffer.end_writing()?,
                written,
                read: 0,
            }
        }
        match &mut self.state {
            State::Buffering => {
                unreachable!();
            }
            State::Reading {
                buffer,
                written,
                read,
            } => {
                if read < written {
                    let record: R = buffer.read()?;
                    (*read) += 1;
                    Ok(Some(record))
                } else {
                    let mut state = State::Done;
                    std::mem::swap(&mut self.state, &mut state);
                    state.fallible_drop()?;
                    Ok(None)
                }
            }
            State::Done => Ok(None),
        }
    }
}

enum State {
    Buffering,
    Reading {
        buffer: BufferReader,
        written: usize,
        read: usize,
    },
    Done,
}

impl State {
    fn fallible_drop(self) -> Result<(), bincode::Error> {
        match self {
            State::Reading {
                buffer,
                written,
                read,
            } => {
                assert_eq!(written, read);
                buffer.end_reading()
            }
            Self::Buffering | State::Done => Ok(()),
        }
    }
}

#[test]
fn should_accumulate_stream() {
    #[derive(Debug)]
    enum Error {
        Bincode(bincode::Error),
    }

    impl From<bincode::Error> for Error {
        fn from(err: bincode::Error) -> Self {
            Self::Bincode(err)
        }
    }

    let mut stream = Accumulate::new(fallible_iterator::convert(
        [
            "ZZZ".to_owned(),
            "".to_owned(),
            "a".to_owned(),
            "z".to_owned(),
            "A".to_owned(),
        ]
        .into_iter()
        .map(Ok::<_, Error>),
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
