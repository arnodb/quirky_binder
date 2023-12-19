use std::cmp::Ordering;

use fallible_iterator::FallibleIterator;

use super::collections::CollectionsIteratorFnHelper;

/// Sorts items in memory and stream them.
#[derive(new)]
pub struct Sort<Input: FallibleIterator<Item = Record, Error = Error>, Record, Error, CmpFn>
where
    CmpFn: Fn(&Record, &Record) -> Ordering,
{
    input: Input,
    cmp: CmpFn,
    #[new(default)]
    buffer: Vec<Record>,
    #[new(value = "false")]
    finalizing: bool,
}

impl<Input: FallibleIterator<Item = Record, Error = Error>, Record, Error, Cmp> FallibleIterator
    for Sort<Input, Record, Error, Cmp>
where
    Cmp: Fn(&Record, &Record) -> Ordering,
{
    type Item = Record;
    type Error = Error;

    fn next(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        if !self.finalizing {
            while let Some(rec) = self.input.next()? {
                self.buffer.push(rec);
            }
            self.buffer.sort_by(|r1, r2| (self.cmp)(r1, r2).reverse());
            self.finalizing = true;
        }
        Ok(self.buffer.pop())
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

#[test]
fn should_sort_stream() {
    let mut stream = Sort::new(
        fallible_iterator::convert(["ZZZ", "", "a", "z", "A"].into_iter().map(Ok::<_, ()>)),
        |a, b| {
            a.to_lowercase()
                .cmp(&b.to_lowercase())
                .then_with(|| a.cmp(b))
        },
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
