use std::sync::mpsc::{SendError, SyncSender};

#[derive(new)]
pub struct ThreadOutput<R>(SyncSender<Option<R>>);

impl<R> ThreadOutput<R> {
    pub fn send(&self, t: Option<R>) -> Result<(), SendError<Option<R>>> {
        self.0.send(t)
    }
}
