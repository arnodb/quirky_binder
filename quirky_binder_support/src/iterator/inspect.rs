use fallible_iterator::FallibleIterator;

struct InspectEnd<I, F>
where
    I: FallibleIterator,
    F: FnMut() -> Result<(), I::Error>,
{
    input: I,
    f: F,
}

impl<I, F> FallibleIterator for InspectEnd<I, F>
where
    I: FallibleIterator,
    F: FnMut() -> Result<(), I::Error>,
{
    type Item = I::Item;
    type Error = I::Error;

    fn next(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        let item = self.input.next();
        if matches!(item.as_ref(), Ok(None)) {
            (self.f)()?;
        }
        item
    }
}

pub trait InspectIterator: FallibleIterator {
    fn inspect_end<F>(self, f: F) -> impl FallibleIterator<Item = Self::Item, Error = Self::Error>
    where
        Self: Sized,
        F: FnMut() -> Result<(), Self::Error>,
    {
        InspectEnd { input: self, f }
    }
}

impl<I: FallibleIterator> InspectIterator for I {}
