use std::sync::mpsc::{SendError, SyncSender};

#[derive(new)]
pub struct ThreadOutput<R>(SyncSender<Option<R>>);

#[derive(new)]
pub struct InstrumentedThreadOutput<F, R, E>
where
    F: FnMut(&R) -> Result<(), E>,
    E: From<SendError<Option<R>>>,
{
    inspect: F,
    output: ThreadOutput<R>,
}

impl<F, R, E> InstrumentedThreadOutput<F, R, E>
where
    F: FnMut(&R) -> Result<(), E>,
    E: From<SendError<Option<R>>>,
{
    pub fn send(&mut self, r: Option<R>) -> Result<(), E> {
        if let Some(r) = &r {
            (self.inspect)(r)?;
        }
        self.output.0.send(r)?;
        Ok(())
    }
}
