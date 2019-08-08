use crate::Result;
use log::{error, info};
use rayon;
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
pub struct SharedQueueThreadPool {
    sender: Sender<Job>,
}

impl SharedQueueThreadPool {
    fn new_thread(receiver: Arc<Mutex<Receiver<Job>>>, idx: u32) -> JoinHandle<()> {
        spawn(move || {
            loop {
                // We only care about handling unwind panics, since abort panics end every thread
                // anyways
                if let Err(_) = std::panic::catch_unwind(|| {
                    let job = match receiver.lock().expect("mutex poisoned").recv() {
                        Ok(job) => job,
                        // Once sender has been dropped, worker threads should stop
                        Err(_) => return,
                    };

                    info!("Thread {} received job", idx);
                    job();
                    info!("Thread {} finished job", idx);
                }) {
                    error!("Thread {} panicked, continuing", idx);
                }
            }
        })
    }
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<Self> {
        let (tx, rx): (Sender<Job>, Receiver<Job>) = channel();
        let receiver = Arc::new(Mutex::new(rx));

        for idx in 0..threads {
            Self::new_thread(receiver.clone(), idx);
        }

        Ok(Self { sender: tx })
    }

    // Performs panic recovery by replacing dead threads before sending messages
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.sender
            .send(Box::new(job))
            .expect("all threads panicked");
    }
}

/// Wraps rayon's threadpool
pub struct RayonThreadPool(rayon::ThreadPool);

impl ThreadPool for RayonThreadPool {
    fn new(threads: u32) -> Result<Self> {
        Ok(RayonThreadPool(
            rayon::ThreadPoolBuilder::new()
                .num_threads(threads as usize)
                .build()?,
        ))
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.0.install(job);
    }
}
