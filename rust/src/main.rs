use std::{fmt::Write, io};

use tokio::io::AsyncReadExt;
use sha2::{digest::generic_array::GenericArray, Digest};

struct FileToHash {
    len : u64,
    name : std::path::PathBuf
}

fn send_io_error(api : &'static str, err : io::Error, channel : &crossbeam::channel::Sender<String>) {
	let mut buf = String::new();
	write!(&mut buf, "{} {}", api, err.to_string());
	channel.send(buf).expect("send_io_error");
}

fn enumerate(directory_to_hash : String, channel : crossbeam::channel::Sender<FileToHash>, errors : crossbeam::channel::Sender<String>) {
    for dir_entry in walkdir::WalkDir::new(directory_to_hash) {
        match dir_entry {
            Err(e) => send_io_error("walkdir", e.into_io_error().expect("walkdir/into_io_error"), &errors),
            Ok(entry) => {
                match entry.metadata() {
                    Err(e) => eprintln!("{e}"),
                    Ok(meta) => {
                        if meta.is_file() {
                            channel.send( 
                                FileToHash { 
                                    len: meta.len(), 
                                    name: entry.path().to_owned() })
                            .expect("channel/enum/send")
                        }
                    }
                }
            }
        }
    }
}

async fn hash_file(buf0 : &mut Vec<u8>, buf1 : &mut Vec<u8>, fp : &mut tokio::fs::File, hasher : &mut sha2::Sha256) -> std::io::Result<()> {
    
    let mut read_in_flight = fp.read(buf1.as_mut_slice() );

    loop {
        let number_read = read_in_flight.await?;
        std::mem::swap(buf0, buf1);
        if number_read == 0 {
            return Ok(())
        }
        else {
            read_in_flight = fp.read(buf1.as_mut_slice());
            hasher.update(&buf0[0..number_read]);
        }
    }
}

async fn hash_files(
	files : crossbeam::channel::Receiver<FileToHash>, 
	hashes : crossbeam::channel::Sender<String>,
	errors : crossbeam::channel::Sender<String>,
	bufsize : usize) {

    let mut buf0 = vec![0u8; bufsize];
    let mut buf1 = vec![0u8; bufsize];
    //let mut bufs = [buf0, buf1];

    let mut hasher = sha2::Sha256::new();
    let mut hash_output = GenericArray::default();

    for file in files {
        match tokio::fs::File::options().read(true).open(&file.name).await {
            Err(e) =>  send_io_error("open", e, &errors),
            Ok(mut fp) => {
                match hash_file(&mut buf0, &mut buf1, &mut fp, &mut hasher).await {
                    Err(read_err) => send_io_error("read", read_err, &errors),
                    Ok(()) => {
                        hasher.finalize_into_reset(&mut hash_output);
						let mut hash_line = String::new();
						write!(&mut hash_line, "{hash_output:X}\t{}\t{}", file.len, file.name.display());
						hashes.send(hash_line).expect("hash_files/hashes/send");
                    }
                }
            }
        }
    }
}

async fn main_hash() {

    let directory_to_hash = std::env::args().nth(1).unwrap_or_else(|| ".".to_string());
	let bufsize = 4096;
	let workers = std::thread::available_parallelism().expect("available_parallelism").get();

    let (enum_send, enum_recv) = crossbeam::channel::bounded(256);
	let (hash_send, hash_recv) = crossbeam::channel::bounded(4096);
	let (err_send, err_recv) = crossbeam::channel::bounded(4096);


    let mut hasher_tasks = tokio::task::JoinSet::new();
    for _ in 0..workers {
        hasher_tasks.spawn(hash_files(enum_recv.clone(), hash_send.clone(), err_send.clone(), bufsize ) );
    }

	let enum_erros = err_send.clone();
    let enum_handle = std::thread::spawn(|| {
        enumerate(directory_to_hash, enum_send, enum_erros);
    });

	// wait for enumerate and hashers to finish
    enum_handle.join().unwrap();
    while let Some(item) = hasher_tasks.join_next().await {
        let () = item.unwrap();
    }
	drop(hash_send);
	drop(err_send);



}

fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(main_hash())
}
