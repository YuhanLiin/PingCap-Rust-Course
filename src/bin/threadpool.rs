use kvs::Result;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{spawn, JoinHandle};

type Job = Box<dyn FnOnce() + Send + 'static>;

#[allow(dead_code)]
struct ThreadPool {
    sender: Sender<Job>,
    threads: Vec<JoinHandle<()>>,
}
impl ThreadPool {
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

fn main() {
    let pool = ThreadPool::new(14).expect("can't create pool");

    for _ in 0..4 {
        pool.spawn(move || panic!());
    }

    loop {}
}
