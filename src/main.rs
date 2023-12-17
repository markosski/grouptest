#![allow(unused)]

mod other;

use std::default;
use std::fs::{File};
use std::io::{Result, Write, BufRead, LineWriter, BufReader, Lines};
use rand::prelude::*;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher, BuildHasherDefault};
use std::collections::HashMap;
use std::thread::{self, JoinHandle, available_parallelism};
use std::sync::{Arc};
use std::time::{Instant};
use ahash::AHashMap;

struct Data {
    ids: Vec<u32>,
    keys: Vec<String>,
    age: Vec<u8>,
    values: Vec<u32>
}

fn main() {
    let file = "data100m.txt";
    // create_file(file, 100_000_000);
    let num_partitions = available_parallelism().unwrap().get() as u8;
    println!("Will use {} cores", num_partitions);
    let partitions = Arc::new(read_data(file, num_partitions));

    let mut handles: Vec<JoinHandle<()>> = vec![];

    let start = Instant::now();
    for i in 0..num_partitions {
        let parts = Arc::clone(&partitions);

        let handle = thread::spawn(move || {
            println!("Started thead number {}", i);
            let grouped = group_count(&parts[i as usize]);
            for (k, v) in grouped.iter() {
                println!("k: {}, v: {}", k, v)            
            }
            println!("Finished thead number {}", i);
        });
        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.join();
    }
    let end = start.elapsed();
    println!("all done in {} millis", end.as_millis());
}

// perform group operation on selected field
fn group_count(data: &Data) -> HashMap<u8, u32, nohash_hasher::BuildNoHashHasher<u8>> {
    println!("Grouping data of size: {}", data.ids.len());
    let mut grouped = HashMap::with_hasher(nohash_hasher::BuildNoHashHasher::default());

    for i in 0..data.age.len() {
        let keyref = &data.age[i];

        if let Some(v) = grouped.get_mut(keyref) {
            *v += 1;
        } else {
            grouped.insert(data.age[i], 1);
        }
    }
    grouped
}

// hash function to assign key to specific 
fn hash(value: &u8, partitions: u8) -> u8 {
    let mut s = DefaultHasher::new();
    value.hash(&mut s);
    let hash = s.finish();
    let hash_partitioned = hash % partitions as u64;
    hash_partitioned as u8
}

fn read_lines(file_name: &str) -> Result<Lines<BufReader<File>>> {
    let file = File::open(file_name)?;
    Ok(BufReader::new(file).lines())
}

// return type: number of items in the vector represents partitions
// hash function mentioned in this thread: https://stackoverflow.com/questions/70551997/faster-hashmap-for-sequential-keys
fn read_data(file_name: &str, num_partitions: u8) -> Vec<Data> {
    let mut partitions = Vec::new();
    for _ in 0..num_partitions {
        partitions.push(
            Data { 
                ids: vec![],
                keys: vec![], 
                age: vec![],
                values: vec![],
            }
        ) 
    }

    if let Ok(lines) = read_lines(file_name) {
        let mut id: u32;
        let mut key: String;
        let mut age: u8;
        let mut value: u32;
        let mut line_counter: u32 = 0;
        let mut line_counter_init: u32 = 1000;
        for (_, line) in lines.enumerate() {
            let row = line.unwrap();
            let parts = row.split(',').collect::<Vec<&str>>();
            id = String::from(parts[0]).parse().unwrap();
            key = String::from(parts[1]);
            age = String::from(parts[2]).parse().unwrap();
            value = parts[3].parse().unwrap();
            let selected_partition = hash(&age, num_partitions) as usize;

            partitions[selected_partition].ids.push(id);
            partitions[selected_partition].keys.push(key);
            partitions[selected_partition].age.push(age);
            partitions[selected_partition].values.push(value);

            line_counter += 1;
            if line_counter % line_counter_init == 0 {
                line_counter_init = (line_counter_init as f32 * 1.25).round() as u32;
                println!("read {} lines so far", line_counter);
            }
        }

        partitions
    } else {
        panic!("something did not work");
    }
}

fn create_file(file_name: &str, line_num: i32) {
    let word_size = 3;

    let file = File::create(file_name).unwrap();
    let mut file = LineWriter::new(file);


    for n in 1..=line_num {
        let mut value = String::from("");
        let rand_value = thread_rng().gen_range(0..999);
        let rand_age = thread_rng().gen_range(18..99);

        for _ in 0..word_size {
            let char_index = thread_rng().gen_range(0..9) as usize;
            let chars = Vec::from(['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']);
            value.push(chars[char_index]);
        }

        let mut line = format!("{},{},{},{}", n, value, rand_age, rand_value);
        if n != line_num {
            line.push('\n');
        }
        file.write_all(line.as_bytes()).unwrap();
    }
}
