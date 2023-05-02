use cthulhu::tentable::*;
use mimalloc::MiMalloc;
use std::env;
use std::time::Instant;
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;
fn main() {
    let start = Instant::now();
    let args = env::args().collect::<Vec<String>>();
    let file_path = args[1].to_owned();

    let table = read_csv(&file_path, Some(2)).unwrap();

    let end = start.elapsed();
    println!("Time elapsed reading file is: {:?}", end);
    drop(table);
    let mut input = String::new();
    std::io::stdin().read_line(&mut input).unwrap();
}
