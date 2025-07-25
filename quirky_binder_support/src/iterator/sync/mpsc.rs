use std::sync::mpsc::{Receiver, RecvError, TryRecvError};

use fallible_iterator::FallibleIterator;

use crate::iterator::try_fallible::TryFallibleIterator;

/// Receives records from a `Receiver`
#[derive(new)]
pub struct Receive<R> {
    rx: Receiver<Option<R>>,
    #[new(default)]
    end_of_input: bool,
}

impl<R> FallibleIterator for Receive<R> {
    type Item = R;
    type Error = RecvError;

    fn next(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        if !self.end_of_input {
            let res = self.rx.recv()?;
            if res.is_none() {
                self.end_of_input = true
            };
            Ok(res)
        } else {
            Ok(None)
        }
    }
}

impl<R> TryFallibleIterator for Receive<R> {
    fn try_next(&mut self) -> Result<Option<Option<Self::Item>>, Self::Error> {
        if !self.end_of_input {
            self.rx.try_recv().map_or_else(
                |err| match err {
                    TryRecvError::Empty => Ok(None),
                    TryRecvError::Disconnected => Err(RecvError),
                },
                |r| Ok(Some(r)),
            )
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    #[test]
    fn should_stream_received_records() {
        use std::sync::mpsc::channel;

        let (tx, rx) = channel();
        let mut stream = Receive::new(rx);
        for i in 0..42 {
            tx.send(Some(i)).unwrap();
        }
        tx.send(None).unwrap();
        for i in 0..42 {
            assert_matches!(stream.next(), Ok(Some(j)) if j == i);
        }
        // End of stream
        assert_matches!(stream.next(), Ok(None));
        assert_matches!(stream.next(), Ok(None));
    }

    #[test]
    fn should_stream_received_records_from_sync_channel() {
        use std::sync::mpsc::sync_channel;

        let (tx, rx) = sync_channel(100);
        let mut stream = Receive::new(rx);
        for i in 0..42 {
            tx.send(Some(i)).unwrap();
        }
        tx.send(None).unwrap();
        for i in 0..42 {
            assert_matches!(stream.next(), Ok(Some(j)) if j == i);
        }
        // End of stream
        assert_matches!(stream.next(), Ok(None));
        assert_matches!(stream.next(), Ok(None));
    }
}
