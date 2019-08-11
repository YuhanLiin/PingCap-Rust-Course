use criterion::*;
use crossbeam::sync::WaitGroup;
use kvs::{
    client::ThreadedKvsClient,
    server::KvsServer,
    thread_pool::{RayonThreadPool, SharedQueueThreadPool, ThreadPool},
    KvStore, KvsEngine, Result,
};
use rand::{distributions::Alphanumeric, rngs::StdRng, Rng, SeedableRng};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::thread::{spawn, JoinHandle};
use tempfile::TempDir;

fn tcp_addr() -> SocketAddr {
    "127.0.0.1:4000".parse().unwrap()
}

fn gen_string(rng: &mut impl Rng) -> String {
    (0..1000).map(|_| rng.sample(Alphanumeric)).collect()
}

fn gen_data(seed: u64) -> Vec<(String, String)> {
    let mut rng: StdRng = SeedableRng::seed_from_u64(seed);

    let val = gen_string(&mut rng);
    (0..100)
        .map(|_| (val.clone(), gen_string(&mut rng)))
        .collect()
}

fn new_kvs(path: &Path) -> KvStore {
    KvStore::open(path).expect("can't open kvs")
}

// Holds the resources necessary to shutdown a running server when dropped
struct ServerHandle<E: KvsEngine + Sync, P: ThreadPool + Send + Sync + 'static> {
    thread: JoinHandle<Result<()>>,
    server: Arc<KvsServer<E, P>>,
}

impl<E: KvsEngine + Sync, P: ThreadPool + Send + Sync + 'static> ServerHandle<E, P> {
    fn run(server: Arc<KvsServer<E, P>>) -> Self {
        let server_clone = Arc::clone(&server);
        let bind_event = WaitGroup::new();
        let cloned_event = WaitGroup::clone(&bind_event);
        let thread = spawn(move || server_clone.run(&tcp_addr(), Some(cloned_event)));
        // Wait for server to finish binding so we don't get "connection refused"
        bind_event.wait();
        Self { server, thread }
    }
}

impl<E: KvsEngine + Sync, P: ThreadPool + Send + Sync + 'static> Drop for ServerHandle<E, P> {
    // Shuts down the server and joins the thread. This work is done outside the benchmark.
    fn drop(&mut self) {
        self.server.shutdown(&tcp_addr()).expect("shutdown failed");
        // Replace server thread with zombie thread so we can check if server ran successfully
        let thread = std::mem::replace(&mut self.thread, spawn(move || Ok(())));
        // If server failed, just panic
        thread
            .join()
            .expect("unexpected panic")
            .expect("server error");
    }
}

fn write_threaded_kvstore<P: ThreadPool + Send + Sync + 'static>(c: &mut Criterion, name: &str) {
    let data = gen_data(99999);
    let temp = TempDir::new().expect("can't open tempdir");
    let inputs = &[2, 4, 8];

    c.bench_function_over_inputs(
        name,
        move |b, &&threads| {
            let kvs = new_kvs(&temp.path());
            let kvs_clone = kvs.clone();
            let server = Arc::new(KvsServer::<_, P>::new(kvs, threads).expect("server problem"));
            let client = ThreadedKvsClient::<P>::new(tcp_addr(), threads).expect("client problem");

            // We only care about dropping this value
            #[allow(unused)]
            let handle = ServerHandle::run(Arc::clone(&server));

            b.iter_batched(
                || {
                    kvs_clone.clear().unwrap();
                    data.clone()
                },
                |data| client.set(data.into_iter()).expect("set failed"),
                BatchSize::SmallInput,
            )
        },
        inputs,
    );
}

fn write_threaded_kvstore_queue(c: &mut Criterion) {
    write_threaded_kvstore::<SharedQueueThreadPool>(c, "write to KVS server with queue threadpool");
}

fn write_threaded_kvstore_rayon(c: &mut Criterion) {
    write_threaded_kvstore::<RayonThreadPool>(c, "write to KVS server with Rayon threadpool");
}

criterion_group!(
    benches,
    write_threaded_kvstore_rayon,
    write_threaded_kvstore_queue
);
criterion_main!(benches);
