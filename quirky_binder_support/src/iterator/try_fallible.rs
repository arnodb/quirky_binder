use fallible_iterator::FallibleIterator;

pub trait TryFallibleIterator: FallibleIterator {
    fn try_next(&mut self) -> Result<Option<Option<Self::Item>>, Self::Error>;

    #[inline]
    fn try_inspect<F>(self, f: F) -> TryInspect<Self, F>
    where
        Self: Sized,
        F: FnMut(&Self::Item) -> Result<(), Self::Error>,
    {
        TryInspect { it: self, f }
    }

    #[inline]
    fn try_map_err<B, F>(self, f: F) -> TryMapErr<Self, F>
    where
        F: FnMut(Self::Error) -> B,
        Self: Sized,
    {
        TryMapErr { it: self, f }
    }
}

pub struct TryInspect<I, F> {
    it: I,
    f: F,
}

impl<I, F> FallibleIterator for TryInspect<I, F>
where
    I: TryFallibleIterator,
    F: FnMut(&I::Item) -> Result<(), I::Error>,
{
    type Item = I::Item;
    type Error = I::Error;

    fn next(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        match self.it.next()? {
            Some(i) => {
                (self.f)(&i)?;
                Ok(Some(i))
            }
            None => Ok(None),
        }
    }
}

impl<I, F> TryFallibleIterator for TryInspect<I, F>
where
    I: TryFallibleIterator,
    F: FnMut(&I::Item) -> Result<(), I::Error>,
{
    #[inline]
    fn try_next(&mut self) -> Result<Option<Option<I::Item>>, I::Error> {
        match self.it.try_next()? {
            Some(Some(i)) => {
                (self.f)(&i)?;
                Ok(Some(Some(i)))
            }
            Some(None) => Ok(Some(None)),
            None => Ok(None),
        }
    }
}

pub struct TryMapErr<I, F> {
    it: I,
    f: F,
}

impl<B, I, F> FallibleIterator for TryMapErr<I, F>
where
    I: FallibleIterator,
    F: FnMut(I::Error) -> B,
{
    type Item = I::Item;

    type Error = B;

    fn next(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        self.it.next().map_err(&mut self.f)
    }
}

impl<B, I, F> TryFallibleIterator for TryMapErr<I, F>
where
    I: TryFallibleIterator,
    F: FnMut(I::Error) -> B,
{
    fn try_next(&mut self) -> Result<Option<Option<Self::Item>>, Self::Error> {
        self.it.try_next().map_err(&mut self.f)
    }
}
