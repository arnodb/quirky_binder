use std::sync::mpsc::{Receiver, RecvError};

use fallible_iterator::IntoFallibleIterator;

use crate::iterator::sync::mpsc::Receive;

#[derive(new)]
pub struct ThreadInput<R>(Receiver<Option<R>>);

impl<R> IntoFallibleIterator for ThreadInput<R> {
    type Item = R;

    type Error = RecvError;

    type IntoFallibleIter = Receive<R>;

    fn into_fallible_iter(self) -> Self::IntoFallibleIter {
        Receive::new(self.0)
    }
}
