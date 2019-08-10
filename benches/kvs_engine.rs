use criterion::*;
use kvs::{KvStore, KvsEngine, SledKvsEngine};
use rand::{distributions::Alphanumeric, rngs::StdRng, Rng, SeedableRng};
use std::path::Path;
use tempfile::TempDir;

static WRITE_SEED: u64 = 12345;
static READ_SEED: u64 = 67890;

fn gen_string(rng: &mut impl Rng) -> String {
    let len = rng.gen_range(1, 1000);
    (0..len).map(|_| rng.sample(Alphanumeric)).collect()
}

fn gen_write_data() -> Vec<(String, String)> {
    let mut rng: StdRng = SeedableRng::seed_from_u64(WRITE_SEED);

    (0..100)
        .map(|_| {
            let key = gen_string(&mut rng);
            let val = gen_string(&mut rng);
            (key, val)
        })
        .collect()
}

fn write_loop(store: &impl KvsEngine, data: Vec<(String, String)>) {
    for (key, val) in data.into_iter() {
        store.set(key, val).expect("write failed");
    }
}

fn gen_read_data() -> Vec<String> {
    let mut rng: StdRng = SeedableRng::seed_from_u64(READ_SEED);

    (0..100).map(|_| gen_string(&mut rng)).collect()
}

fn read_loop(store: &impl KvsEngine, data: Vec<String>) {
    for key in data.into_iter() {
        store.get(key).expect("read failed");
    }
}

fn new_kvs(path: &Path) -> KvStore {
    KvStore::open(path).expect("can't open kvs")
}

fn new_sled(path: &Path) -> SledKvsEngine {
    SledKvsEngine::open(path).expect("can't open sled")
}

fn write_bench_kvs(c: &mut Criterion) {
    let data = gen_write_data();
    let temp = TempDir::new().expect("can't open tempdir");

    c.bench_function("write kvs", move |b| {
        let kvs = new_kvs(&temp.path());
        b.iter_batched(
            || {
                kvs.clear().unwrap();
                data.clone()
            },
            |data| write_loop(&kvs, data),
            BatchSize::SmallInput,
        )
    });
}

fn write_bench_sled(c: &mut Criterion) {
    let data = gen_write_data();
    let temp = TempDir::new().expect("can't open tempdir");

    c.bench_function("write sled", move |b| {
        let sled = new_sled(&temp.path());
        b.iter_batched(
            || {
                sled.clear().unwrap();
                data.clone()
            },
            |data| write_loop(&sled, data),
            BatchSize::SmallInput,
        )
    });
}

fn read_bench_kvs(c: &mut Criterion) {
    let data = gen_read_data();
    let temp = TempDir::new().expect("can't open tempdir");

    c.bench_function("read kvs", move |b| {
        let kvs = new_kvs(&temp.path());
        b.iter_batched(
            || {
                kvs.clear().unwrap();
                // Write in the keys before reading them
                let write_data = data.iter().cloned().map(|s| (s.clone(), s)).collect();
                write_loop(&kvs, write_data);
                data.clone()
            },
            |data| read_loop(&kvs, data),
            BatchSize::SmallInput,
        )
    });
}

fn read_bench_sled(c: &mut Criterion) {
    let data = gen_read_data();
    let temp = TempDir::new().expect("can't open tempdir");

    c.bench_function("read sled", move |b| {
        let sled = new_sled(&temp.path());
        b.iter_batched(
            || {
                sled.clear().unwrap();
                // Write in the keys before reading them
                let write_data = data.iter().cloned().map(|s| (s.clone(), s)).collect();
                write_loop(&sled, write_data);
                data.clone()
            },
            |data| read_loop(&sled, data),
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(
    benches,
    write_bench_kvs,
    write_bench_sled,
    read_bench_kvs,
    read_bench_sled
);
criterion_main!(benches);
