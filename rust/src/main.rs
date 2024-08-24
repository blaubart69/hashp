use std::{io::Write, path::{Path, PathBuf}, sync::{Arc, Mutex}, time::Instant};

use tokio::io::{AsyncReadExt};
use sha2::{Digest};

struct FileToHash {
    len : u64,
    name : std::path::PathBuf
}

/*
fn send_io_error(api : &'static str, err : io::Error, err_writer : &mut tokio::io::BufWriter<tokio::fs::File>) {
    use std::fmt::Write;
	let mut buf = String::new();
    use std::fmt::Write;
	write!(&mut buf, "{} {}", api, err.to_string());
    err_writer.write(buf.as_bytes());
}
    */

fn enumerate(directory_to_hash : String, channel : crossbeam_channel::Sender<FileToHash>) {
    for dir_entry in walkdir::WalkDir::new(directory_to_hash) {
        match dir_entry {
            Err(e) => eprintln!("{e}"),
            Ok(entry) => {
                match entry.metadata() {
                    Err(e) => eprintln!("{e}"),
                    Ok(meta) => {
                        if meta.is_file() {
                            let _ = channel.send( 
                                FileToHash { 
                                    len: meta.len(), 
                                    name: entry.into_path() })
                            .expect("channel/enum/send");
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
	files : crossbeam_channel::Receiver<FileToHash>, 
	//hashes : Arc<Mutex<std::io::BufWriter<std::fs::File>>>,
    hashes : Arc<Mutex<impl Write>>,
	bufsize : usize,
    rootdir : Arc<PathBuf>
) {

    let mut buf0 = vec![0u8; bufsize];
    let mut buf1 = vec![0u8; bufsize];
    //let mut bufs = [buf0, buf1];

    let mut hasher = sha2::Sha256::new();
    let mut hash_output = sha2::digest::generic_array::GenericArray::default();
    let mut hash_line = String::new();

    for file in files {
        match tokio::fs::File::options().read(true).open(&file.name).await {
            Err(e) =>  eprintln!("{e}"),
            Ok(mut fp) => {
                match hash_file(&mut buf0, &mut buf1, &mut fp, &mut hasher).await {
                    Err(read_err) => eprintln!("{read_err}"),
                    Ok(()) => {
                        hasher.finalize_into_reset(&mut hash_output);
                        
                        use std::fmt::Write;
                        hash_line.clear();

                        write!(&mut hash_line, "{hash_output:X}\t{}\t{}\n", file.len, file.name.display());
                        //hashes.send(hash_line).await.expect("hash_files/hashes/send");
                        hashes.lock().unwrap().write(hash_line.as_bytes());
                    }
                }
            }
        }
    }
}


async fn main_hash(workers : usize) {

    let directoryname_to_hash = std::env::args().nth(1).unwrap_or_else(|| ".".to_string());

    let root_dir = Arc::new(Path::canonicalize(Path::new(&directoryname_to_hash)).unwrap());

    let hashes_filename = "./hashes.txt";
    let errors_filename = "./errors.txt";
	let bufsize = 64 * 1024;
    let hash_writer = std::io::BufWriter::with_capacity(64*1024, std::fs::File::create(hashes_filename).expect("could not create hash result file"));
    let err_writer = std::io::BufWriter::new(std::fs::File::create(errors_filename).expect("could not create file for errors"));
    

    let mux_hash_writer = Arc::new( Mutex::new(hash_writer));

    let (enum_send, enum_recv) = crossbeam_channel::bounded(256);

    let start = Instant::now();

    println!("starting {} hash workers for directory {}", workers, root_dir.display());
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..workers {
        tasks.spawn(hash_files(enum_recv.clone(), mux_hash_writer.clone(), bufsize, root_dir.clone() ) );
    }
    let enum_thread = std::thread::spawn(|| enumerate(directoryname_to_hash, enum_send));

	// wait for enumerate and hashers to finish
    enum_thread.join().unwrap();
    while let Some(item) = tasks.join_next().await {
        let () = item.unwrap();
    }
    let duration = start.elapsed();

    println!("done in {:?}",duration);
}


fn main() {

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    
    rt.block_on(main_hash(rt.metrics().num_workers()))
}
