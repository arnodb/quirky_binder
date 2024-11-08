use std::cmp::Ordering;

use binary_heap_plus::BinaryHeap;
use compare::Compare;
use fallible_iterator::FallibleIterator;
use serde::{Deserialize, Serialize};

use super::collections::CollectionsIteratorFnHelper;
use crate::data::buffer::{Buffer, BufferReader};

pub const DEFAULT_SLICE_SIZE: usize = 1 << 16;

/// Sorts items in memory and stream them.
pub struct Sort<Input: FallibleIterator<Item = Record, Error = Error>, Record, Error, CmpFn>
where
    CmpFn: Fn(&Record, &Record) -> Ordering,
{
    input: Input,
    cmp: CmpFn,
    slice_size: usize,
    state: State<Record, CmpFn>,
}

impl<Input: FallibleIterator<Item = Record, Error = Error>, Record, Error, CmpFn>
    Sort<Input, Record, Error, CmpFn>
where
    CmpFn: Fn(&Record, &Record) -> Ordering,
{
    pub fn new(input: Input, cmp: CmpFn) -> Self {
        Self::with_slice_size(input, cmp, DEFAULT_SLICE_SIZE)
    }

    pub fn with_slice_size(input: Input, cmp: CmpFn, slice_size: usize) -> Self {
        Self {
            input,
            cmp,
            slice_size,
            state: State::Buffering,
        }
    }
}

impl<'de, Input: FallibleIterator<Item = Record, Error = Error>, Record, Error, CmpFn>
    FallibleIterator for Sort<Input, Record, Error, CmpFn>
where
    CmpFn: Fn(&Record, &Record) -> Ordering + Clone,
    Record: Serialize + Deserialize<'de>,
    Error: From<bincode::Error>,
{
    type Item = Record;
    type Error = Error;

    fn next(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        if let State::Buffering = &self.state {
            let mut slice_buffers = Vec::new();
            let mut current_slice = Vec::new();
            while let Some(record) = self.input.next()? {
                if current_slice.len() >= self.slice_size {
                    current_slice.sort_by(|r1, r2| (self.cmp)(r1, r2));
                    let mut buffer = Buffer::new();
                    let len = current_slice.len();
                    for record in current_slice {
                        buffer.push(record)?;
                    }
                    current_slice = Vec::new();
                    slice_buffers.push(SliceBuffer {
                        buffer: buffer.end_writing()?,
                        written: len,
                        read: 0,
                    });
                }
                current_slice.push(record);
            }
            if !current_slice.is_empty() {
                // reverse is important
                current_slice.sort_by(|r1, r2| (self.cmp)(r1, r2).reverse());
            }
            let slice_buffers_len = slice_buffers.len();
            let heads = slice_buffers
                .iter_mut()
                .enumerate()
                .map(
                    |(
                        source_index,
                        SliceBuffer {
                            buffer,
                            written,
                            read,
                        },
                    )| {
                        assert_ne!(*written, 0);
                        assert_eq!(*read, 0);
                        let record: Record = buffer.read()?;
                        (*read) = 1;
                        Ok(Some(SourcedRecord {
                            record,
                            source_index,
                        }))
                    },
                )
                .filter_map(Result::transpose)
                .chain(
                    current_slice
                        .pop()
                        .map(|record| {
                            Ok(SourcedRecord {
                                record,
                                source_index: slice_buffers_len,
                            })
                        })
                        .into_iter(),
                )
                .collect::<Result<Vec<SourcedRecord<Record>>, _>>()?;
            self.state = State::Reading {
                slice_buffers,
                last_slice: current_slice,
                heads: BinaryHeap::from_vec_cmp(
                    heads,
                    MinSourcedRecordComparator {
                        cmp: self.cmp.clone(),
                    },
                ),
            }
        }
        match &mut self.state {
            State::Buffering => {
                unreachable!();
            }
            State::Reading {
                slice_buffers,
                last_slice,
                heads,
            } => {
                if let Some(SourcedRecord {
                    record,
                    source_index,
                }) = heads.pop()
                {
                    if source_index < slice_buffers.len() {
                        let SliceBuffer {
                            buffer,
                            written,
                            read,
                        } = &mut slice_buffers[source_index];
                        if read < written {
                            let new_record: Record = buffer.read()?;
                            (*read) += 1;
                            heads.push(SourcedRecord {
                                record: new_record,
                                source_index,
                            });
                        }
                    } else {
                        assert_eq!(source_index, slice_buffers.len());
                        if let Some(new_record) = last_slice.pop() {
                            heads.push(SourcedRecord {
                                record: new_record,
                                source_index,
                            });
                        }
                    }
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

enum State<Record, CmpFn>
where
    CmpFn: Fn(&Record, &Record) -> Ordering,
{
    Buffering,
    Reading {
        slice_buffers: Vec<SliceBuffer>,
        last_slice: Vec<Record>,
        heads: BinaryHeap<SourcedRecord<Record>, MinSourcedRecordComparator<CmpFn>>,
    },
    Done,
}

struct SliceBuffer {
    buffer: BufferReader,
    written: usize,
    read: usize,
}

impl<Record, CmpFn> State<Record, CmpFn>
where
    CmpFn: Fn(&Record, &Record) -> Ordering,
{
    fn fallible_drop(self) -> Result<(), bincode::Error> {
        match self {
            State::Reading {
                slice_buffers,
                last_slice,
                heads,
            } => {
                for SliceBuffer {
                    buffer,
                    written,
                    read,
                } in slice_buffers
                {
                    assert_eq!(written, read);
                    buffer.end_reading()?;
                }
                assert!(last_slice.is_empty());
                assert!(heads.is_empty());
                Ok(())
            }
            Self::Buffering | State::Done => Ok(()),
        }
    }
}

struct SourcedRecord<Record> {
    record: Record,
    source_index: usize,
}

struct MinSourcedRecordComparator<CmpFn> {
    cmp: CmpFn,
}

impl<Record, CmpFn> Compare<SourcedRecord<Record>> for MinSourcedRecordComparator<CmpFn>
where
    CmpFn: Fn(&Record, &Record) -> Ordering,
{
    fn compare(&self, a: &SourcedRecord<Record>, b: &SourcedRecord<Record>) -> Ordering {
        // reverse b-a is important
        (self.cmp)(&b.record, &a.record).then_with(|| b.source_index.cmp(&a.source_index))
    }
}

trait Heads<Record>: Send {
    fn push(&mut self, record: Record);

    fn pop(&mut self) -> Option<Record>;

    fn is_empty(&self) -> bool;
}

impl<Record: Send, C: Compare<Record> + Send> Heads<Record> for BinaryHeap<Record, C> {
    fn push(&mut self, record: Record) {
        BinaryHeap::push(self, record)
    }

    fn pop(&mut self) -> Option<Record> {
        BinaryHeap::pop(self)
    }

    fn is_empty(&self) -> bool {
        BinaryHeap::is_empty(self)
    }
}

#[derive(new)]
pub struct SubSort<
    Input: FallibleIterator<Item = Record, Error = Error>,
    Record,
    Error,
    CollectionsIteratorFn,
    Collection,
    SubRecord,
    CmpFn,
> where
    CollectionsIteratorFn: for<'r> CollectionsIteratorFnHelper<'r, Record, Collection>,
    Collection: AsMut<[SubRecord]>,
    CmpFn: Fn(&SubRecord, &SubRecord) -> Ordering,
{
    input: Input,
    collections_iterator_fn: CollectionsIteratorFn,
    cmp: CmpFn,
    _collection: std::marker::PhantomData<Collection>,
    _sub_record: std::marker::PhantomData<SubRecord>,
}

impl<
        Input: FallibleIterator<Item = Record, Error = Error>,
        Record,
        Error,
        CollectionsIteratorFn,
        Collection,
        SubRecord,
        CmpFn,
    > FallibleIterator
    for SubSort<Input, Record, Error, CollectionsIteratorFn, Collection, SubRecord, CmpFn>
where
    CollectionsIteratorFn: for<'r> CollectionsIteratorFnHelper<'r, Record, Collection>,
    Collection: AsMut<[SubRecord]>,
    CmpFn: Fn(&SubRecord, &SubRecord) -> Ordering,
{
    type Item = Record;
    type Error = Error;

    fn next(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        Ok(self.input.next()?.map(|mut rec| {
            for collection in self.collections_iterator_fn.call(&mut rec) {
                collection.as_mut().sort_by(|r1, r2| (self.cmp)(r1, r2));
            }
            rec
        }))
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(DEFAULT_SLICE_SIZE)]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(42)]
    fn should_sort_stream(#[case] slice_size: usize) {
        #[derive(Debug)]
        enum Error {
            Bincode(bincode::Error),
        }

        impl From<bincode::Error> for Error {
            fn from(err: bincode::Error) -> Self {
                Self::Bincode(err)
            }
        }

        let mut stream = Sort::with_slice_size(
            fallible_iterator::convert(
                [
                    "ZZZ".to_owned(),
                    "".to_owned(),
                    "a".to_owned(),
                    "z".to_owned(),
                    "A".to_owned(),
                ]
                .into_iter()
                .map(Ok::<_, Error>),
            ),
            |a, b| {
                a.to_lowercase()
                    .cmp(&b.to_lowercase())
                    .then_with(|| a.cmp(b))
            },
            slice_size,
        );
        assert_matches!(stream.next(), Ok(Some(v)) if v == "");
        assert_matches!(stream.next(), Ok(Some(v)) if v == "A");
        assert_matches!(stream.next(), Ok(Some(v)) if v == "a");
        assert_matches!(stream.next(), Ok(Some(v)) if v == "z");
        assert_matches!(stream.next(), Ok(Some(v)) if v == "ZZZ");
        // End of stream
        assert_matches!(stream.next(), Ok(None));
        assert_matches!(stream.next(), Ok(None));
    }

    #[rstest]
    #[case(DEFAULT_SLICE_SIZE)]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    #[case(5)]
    #[case(42)]
    fn should_sort_big_stream(#[case] slice_size: usize) {
        #[derive(Debug)]
        enum Error {
            Bincode(bincode::Error),
        }

        impl From<bincode::Error> for Error {
            fn from(err: bincode::Error) -> Self {
                Self::Bincode(err)
            }
        }

        use rand::Rng;
        use rand_chacha::rand_core::SeedableRng;

        let mut rng = rand_chacha::ChaCha8Rng::from_entropy();
        println!("Seed: {:02x?}", rng.get_seed());

        let input = (0..(1024 * 1024))
            .map(|_| {
                let num: i32 = rng.gen();
                num
            })
            .collect::<Vec<_>>();

        let mut stream = Sort::with_slice_size(
            fallible_iterator::convert(input.clone().into_iter().map(Ok::<_, Error>)),
            |a, b| a.cmp(b),
            slice_size,
        );

        let mut input = input;
        input.sort();
        for num in input {
            assert_matches!(stream.next(), Ok(Some(v)) if v == num);
        }
        // End of stream
        assert_matches!(stream.next(), Ok(None));
        assert_matches!(stream.next(), Ok(None));
    }
}
