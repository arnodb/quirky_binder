use std::sync::mpsc::Receiver;

use crate::iterator::sync::mpsc::Receive;

#[derive(new)]
pub struct ThreadInput<R>(Receiver<Option<R>>);

impl<R> ThreadInput<R> {
    pub fn into_try_fallible_iter(self) -> Receive<R> {
        Receive::new(self.0)
    }
}
