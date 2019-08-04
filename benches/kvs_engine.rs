use criterion::*;
use kvs::{KvStore, KvsEngine, SledKvsEngine};
use rand::{distributions::Alphanumeric, rngs::StdRng, Rng, SeedableRng};
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

fn write_loop(store: &mut impl KvsEngine, data: Vec<(String, String)>) {
    for (key, val) in data.into_iter() {
        store.set(key, val).expect("write failed");
    }
}

fn gen_read_data() -> Vec<String> {
    let mut rng: StdRng = SeedableRng::seed_from_u64(READ_SEED);

    (0..100).map(|_| gen_string(&mut rng)).collect()
}

fn read_loop(store: &mut impl KvsEngine, data: Vec<String>) {
    for key in data.into_iter() {
        store.get(key).expect("read failed");
    }
}

fn new_kvs() -> KvStore {
    KvStore::open(TempDir::new().expect("can't open tempdir").path()).expect("can't open kvs")
}

fn new_sled() -> SledKvsEngine {
    SledKvsEngine::open(TempDir::new().expect("can't open tempdir").path()).expect("can't open kvs")
}

fn write_bench_kvs(c: &mut Criterion) {
    let data = gen_write_data();

    c.bench_function("write kvs", move |b| {
        b.iter_batched(
            || (new_kvs(), data.clone()),
            |(mut kvs, data)| write_loop(&mut kvs, data),
            BatchSize::SmallInput,
        )
    });
}

fn write_bench_sled(c: &mut Criterion) {
    let data = gen_write_data();

    c.bench_function("write sled", move |b| {
        b.iter_batched(
            || (new_sled(), data.clone()),
            |(mut sled, data)| write_loop(&mut sled, data),
            BatchSize::SmallInput,
        )
    });
}

fn read_bench_kvs(c: &mut Criterion) {
    let data = gen_read_data();

    c.bench_function("read kvs", move |b| {
        b.iter_batched(
            || {
                let mut kvs = new_kvs();
                // Write in the keys before reading them
                let write_data = data.iter().cloned().map(|s| (s.clone(), s)).collect();
                write_loop(&mut kvs, write_data);
                (kvs, data.clone())
            },
            |(mut kvs, data)| read_loop(&mut kvs, data),
            BatchSize::SmallInput,
        )
    });
}

fn read_bench_sled(c: &mut Criterion) {
    let data = gen_read_data();

    c.bench_function("read sled", move |b| {
        b.iter_batched(
            || {
                let mut sled = new_sled();
                // Write in the keys before reading them
                let write_data = data.iter().cloned().map(|s| (s.clone(), s)).collect();
                write_loop(&mut sled, write_data);
                (sled, data.clone())
            },
            |(mut sled, data)| read_loop(&mut sled, data),
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
