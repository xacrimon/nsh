use std::collections::LinkedList;
use std::sync::Arc;
use std::sync::{Condvar, Mutex};
use std::time::Duration;

struct State<T> {
    queue: LinkedList<T>,
    closed: bool,
}

struct Inner<T> {
    state: Mutex<State<T>>,
    condvar: Condvar,
}

macro_rules! iterator {
    ($t:ty) => {
        &mut dyn Iterator<Item=$t>
    };
}

macro_rules! predicate {
    ($t:ty) => {
        impl Fn(crate::channel::iterator!(&$t)) -> bool
    };
}

pub(crate) use iterator;
pub(crate) use predicate;

pub struct SendError;

pub enum RecvTimeoutError {
    Timeout,
    Disconnected,
}

#[derive(Clone)]
pub struct Channel<T> {
    inner: Arc<Inner<T>>,
}

impl<T> Channel<T> {
    pub fn new() -> Self {
        Channel {
            inner: Arc::new(Inner {
                state: Mutex::new(State {
                    queue: LinkedList::new(),
                    closed: false,
                }),
                condvar: Condvar::new(),
            }),
        }
    }

    pub fn send_when(&self, v: T, predicate: predicate!(T)) -> Result<(), SendError> {
        let mut state = self.inner.state.lock().unwrap();
        if state.closed {
            return Err(SendError);
        }

        state = self
            .inner
            .condvar
            .wait_while(state, |state| !predicate(&mut state.queue.iter()))
            .unwrap();

        state.queue.push_back(v);
        self.inner.condvar.notify_all();
        Ok(())
    }

    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        let mut state = self.inner.state.lock().unwrap();
        let mut status;

        loop {
            if state.closed {
                return Err(RecvTimeoutError::Disconnected);
            }

            if let Some(v) = state.queue.pop_front() {
                self.inner.condvar.notify_all();
                return Ok(v);
            }

            (state, status) = self.inner.condvar.wait_timeout(state, timeout).unwrap();
            if status.timed_out() {
                return Err(RecvTimeoutError::Timeout);
            }
        }
    }

    pub fn close(&self) {
        let mut state = self.inner.state.lock().unwrap();
        state.closed = true;
        self.inner.condvar.notify_all();
    }
}
