include!(concat!(env!("OUT_DIR"), "/chain.rs"));

pub mod tokenize {
    use std::collections::VecDeque;

    use fallible_iterator::FallibleIterator;

    use crate::chain::streams::quirky_binder_main_0::read_input::{
        Record0 as RecordIn, Record1 as RecordOut, UnpackedRecord1 as UnpackedRecordOut,
    };

    pub fn tokenize<I, E>(mut input: I) -> impl FallibleIterator<Item = RecordOut, Error = E>
    where
        I: FallibleIterator<Item = RecordIn, Error = E>,
    {
        let mut buffer = VecDeque::<Box<str>>::new();
        quirky_binder_support::iterator::from_fn(move || {
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
