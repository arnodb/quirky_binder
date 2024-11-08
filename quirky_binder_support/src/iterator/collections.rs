pub trait CollectionsIteratorFnHelper<'r, Record, Collection>
where
    Record: 'r,
    Collection: 'r,
{
    type Iter: Iterator<Item = &'r mut Collection>;

    fn call(&self, val: &'r mut Record) -> Self::Iter;
}

impl<'r, Record, Collection, Iter, Func> CollectionsIteratorFnHelper<'r, Record, Collection>
    for Func
where
    Record: 'r,
    Collection: 'r,
    Iter: Iterator<Item = &'r mut Collection>,
    Func: Fn(&'r mut Record) -> Iter,
{
    type Iter = Iter;

    fn call(&self, val: &'r mut Record) -> Self::Iter {
        (self)(val)
    }
}
