use crate::Result;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{spawn, JoinHandle};

type Job = Box<dyn FnOnce() + Send + 'static>;

/// Trait for constructing a new thread pool and spawning tasks for it
pub trait ThreadPool: Sized {
    /// Constructs new pool with a specified number of threads
    fn new(threads: u32) -> Result<Self>;

    /// Give the pool a task to complete
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}

/// Spawns new thread for every job
pub struct NaiveThreadPool;

impl ThreadPool for NaiveThreadPool {
    fn new(_threads: u32) -> Result<Self> {
        Ok(Self)
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        spawn(job);
    }
}

/// Sends tasks to a shared set of threads using a channel. Does not handle panics.
#[allow(dead_code)]
pub struct SharedQueueThreadPool {
    sender: Sender<Job>,
    threads: Vec<JoinHandle<()>>,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<Self> {
        let (tx, rx): (Sender<Job>, Receiver<Job>) = channel();
        let receiver = Arc::new(Mutex::new(rx));

        let threads = (0..threads)
            .map(|_| {
                let receiver = receiver.clone();
                spawn(move || loop {
                    let job = receiver
                        .lock()
                        .expect("mutex poisoned")
                        .recv()
                        .expect("sender dropped");
                    job();
                })
            })
            .collect();

        Ok(Self {
            threads,
            sender: tx,
        })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.sender
            .send(Box::new(job))
            .expect("all threads panicked");
    }
}

/// TODO
pub struct RayonThreadPool;

impl ThreadPool for RayonThreadPool {
    fn new(_threads: u32) -> Result<Self> {
        Ok(Self)
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {

    }
}
