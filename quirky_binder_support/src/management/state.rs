use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use fallible_iterator::FallibleIterator;

use crate::iterator::from_fn;

#[derive(Debug, Default)]
pub struct ThreadState {
    streams: BTreeMap<String, StreamState>,
}

#[derive(Debug, Default)]
struct StreamState {
    written: usize,
    read: usize,
}

pub fn inline_stream_state_update<'a, I, E>(
    mut input: I,
    state: Arc<Mutex<ThreadState>>,
    key: &'a str,
    error: E,
) -> impl FallibleIterator<Item = I::Item, Error = I::Error> + 'a
where
    I: FallibleIterator + 'a,
    E: Fn(String) -> I::Error + 'a,
{
    let mut time_mark = SystemTime::now();
    let mut seen = 0;
    from_fn(move || {
        if let Some(record) = input.next()? {
            seen += 1;
            let new_time_mark = SystemTime::now();
            if new_time_mark
                .duration_since(time_mark)
                .map_err(|err| error(err.to_string()))?
                >= Duration::from_secs(2)
            {
                let mut state = state.lock().map_err(|err| error(err.to_string()))?;
                let stream = state
                    .streams
                    .entry(key.to_owned())
                    .or_insert_with(Default::default);
                stream.written = seen;
                stream.read = seen;
                time_mark = new_time_mark;
            }
            Ok(Some(record))
        } else {
            let mut state = state.lock().map_err(|err| error(err.to_string()))?;
            let stream = state
                .streams
                .entry(key.to_owned())
                .or_insert_with(Default::default);
            stream.written = seen;
            stream.read = seen;
            Ok(None)
        }
    })
}
