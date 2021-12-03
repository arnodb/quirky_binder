pub mod streams {
    include!(concat!(env!("OUT_DIR"), "/chain_streams.rs"));
}

include!(concat!(env!("OUT_DIR"), "/chain.rs"));

pub mod tokenize {
    use fallible_iterator::FallibleIterator;
    use std::collections::VecDeque;

    type RecordIn = crate::chain::streams::read_input::read::Record0<
        { crate::chain::streams::read_input::read::MAX_SIZE },
    >;

    type RecordOut = crate::chain::streams::read_input::read::Record1<
        { crate::chain::streams::read_input::read::MAX_SIZE },
    >;
    type UnpackedRecordOut = crate::chain::streams::read_input::read::UnpackedRecord1;

    #[derive(new)]
    pub struct Tokenize<I: FallibleIterator<Item = RecordIn, Error = E>, E> {
        input: I,
        #[new(default)]
        buffer: VecDeque<Box<str>>,
    }

    impl<I: FallibleIterator<Item = RecordIn, Error = E>, E> FallibleIterator for Tokenize<I, E> {
        type Item = RecordOut;
        type Error = E;

        fn next(&mut self) -> Result<Option<Self::Item>, Self::Error> {
            self.buffer.pop_front().map_or_else(
                || loop {
                    let record_0 = if let Some(rec) = self.input.next()? {
                        rec
                    } else {
                        return Ok(None);
                    };
                    let mut words = record_0.words().split_whitespace();
                    if let Some(first) = words.next() {
                        for w in words {
                            self.buffer.push_back(w.into())
                        }
                        let first_char = first.chars().next().expect("first char");
                        return Ok(Some(RecordOut::new(UnpackedRecordOut {
                            word: first.into(),
                            first_char,
                        })));
                    }
                },
                |word| {
                    let first_char = word.chars().next().expect("first char");
                    Ok(Some(RecordOut::new(UnpackedRecordOut { word, first_char })))
                },
            )
        }
    }
}
