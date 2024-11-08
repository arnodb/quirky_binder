use fallible_iterator::FallibleIterator;

pub mod accumulate;
pub mod collections;
pub mod dedup;
pub mod group;
pub mod io;
pub mod sort;
pub mod sync;

pub fn from_fn<T, E, F>(f: F) -> FromFn<F>
where
    F: FnMut() -> Result<Option<T>, E>,
{
    FromFn(f)
}

pub struct FromFn<F>(F);

impl<T, E, F> FallibleIterator for FromFn<F>
where
    F: FnMut() -> Result<Option<T>, E>,
{
    type Item = T;
    type Error = E;

    #[inline]
    fn next(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        (self.0)()
    }
}
