pub mod streams {
    include!(concat!(env!("OUT_DIR"), "/chain_streams.rs"));
}

include!(concat!(env!("OUT_DIR"), "/chain.rs"));

pub mod tokenize {
    use crate::chain::streams::read_input::datapet_main::Record0 as RecordIn;
    use crate::chain::streams::read_input::datapet_main::Record1 as RecordOut;
    use crate::chain::streams::read_input::datapet_main::UnpackedRecord1 as UnpackedRecordOut;
    use fallible_iterator::FallibleIterator;
    use std::collections::VecDeque;

    pub fn tokenize<I, E>(mut input: I) -> impl FallibleIterator<Item = RecordOut, Error = E>
    where
        I: FallibleIterator<Item = RecordIn, Error = E>,
    {
        let mut buffer = VecDeque::<Box<str>>::new();
        datapet_support::iterator::from_fn(move || {
            buffer.pop_front().map_or_else(
                || loop {
                    let record_0 = if let Some(rec) = input.next()? {
                        rec
                    } else {
                        return Ok(None);
                    };
                    let mut words = record_0.words().split_whitespace();
                    if let Some(first) = words.next() {
                        for w in words {
                            buffer.push_back(w.into())
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
        })
    }
}
