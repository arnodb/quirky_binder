use std::sync::mpsc::{SendError, SyncSender};

#[derive(new)]
pub struct ThreadOutput<R>(SyncSender<Option<R>>);

#[derive(new)]
pub struct InstrumentedThreadOutput<F, R>
where
    F: FnMut(),
{
    inspect: F,
    output: ThreadOutput<R>,
}

impl<F, R> InstrumentedThreadOutput<F, R>
where
    F: FnMut(),
{
    pub fn send(&mut self, t: Option<R>) -> Result<(), SendError<Option<R>>> {
        (self.inspect)();
        self.output.0.send(t)
    }
}
