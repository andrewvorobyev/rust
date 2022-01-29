use csv;
use std::env;
use std::io;
use std::path::PathBuf;
use transactor::process;

fn parse_args(args: &[String]) -> PathBuf {
    if args.len() != 2 {
        panic!("Invalid arguments. Usage: cargo run -- <transactions file path>")
    }

    PathBuf::from(&args[1])
}

fn main() {
    let args: Vec<_> = env::args().collect();
    let fpath = parse_args(&args);
    let mut reader = csv::Reader::from_path(&fpath).expect("Failed to read input file");
    let mut writer = csv::Writer::from_writer(io::stdout());
    process(&mut reader, &mut writer);
}
