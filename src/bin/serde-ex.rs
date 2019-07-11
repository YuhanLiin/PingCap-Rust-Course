use bson::Document;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{Read, Write};

#[derive(Debug, Default)]
struct Buffer {
    buf: Vec<u8>,
    rd_idx: usize,
}

impl Read for Buffer {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let sz = std::cmp::min(self.buf.len() - self.rd_idx, buf.len());
        if sz == 0 {
            Ok(0)
        } else {
            buf[0..sz].clone_from_slice(&self.buf[self.rd_idx..self.rd_idx + sz]);
            self.rd_idx += sz;
            Ok(sz)
        }
    }
}

impl Write for Buffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buf.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.buf.flush()
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum Direction {
    Up,
    Down,
    Left,
    Right,
}

#[derive(Debug, Serialize, Deserialize)]
struct Move {
    direction: Direction,
    #[serde(with = "bson::compat::u2f")]
    distance: u32,
}

fn main() {
    let mv = Move {
        direction: Direction::Up,
        distance: 4,
    };

    let serialized = ron::ser::to_string(&mv).unwrap();
    println!("serialized = {}", serialized);

    let deserialized: Move = ron::de::from_str(&serialized).unwrap();
    println!("deserialized = {:?}", deserialized);

    let serialized = serde_json::to_vec(&mv).unwrap();
    println!("serialized = {:?}", serialized);

    let deserialized: Move = serde_json::from_slice(&serialized).unwrap();
    println!("deserialized = {:?}", deserialized);

    let moves = (0..100)
        .map(|n| {
            (
                n.to_string(),
                bson::to_bson(&Move {
                    direction: Direction::Left,
                    distance: n as u32,
                })
                .unwrap(),
            )
        })
        .collect::<Document>();

    {
        let mut file = File::create("move.bson").unwrap();
        bson::encode_document(&mut file, moves.iter()).expect("Bad encode");
    }
    {
        let mut file = File::open("move.bson").unwrap();
        let deserialized = bson::decode_document(&mut file).expect("Bad decode");
        println!("deserialized = {}", deserialized);
    }

    let mut buf = Buffer::default();
    bson::encode_document(&mut buf, moves.iter()).expect("Bad encode");
    let deserialized = bson::decode_document(&mut buf).expect("Bad decode");
    println!("deserialized = {}", deserialized);
}
