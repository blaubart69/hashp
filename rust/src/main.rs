use tokio::io::AsyncReadExt;
use sha2::{digest::generic_array::GenericArray, Digest};

struct FileToHash {
    len : u64,
    name : std::path::PathBuf
}

fn enumerate(directory_to_hash : String, channel : crossbeam::channel::Sender<FileToHash>) {
    for dir_entry in walkdir::WalkDir::new(directory_to_hash) {
        match dir_entry {
            Err(e) => eprintln!("{e}"),
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

//async fn hash_file(bufs : &mut [Vec<u8>; 2], fp : &mut tokio::fs::File, hasher : &mut sha2::Sha256) -> std::io::Result<()> {
async fn hash_file(buf0 : &mut Vec<u8>, buf1 : &mut Vec<u8>, fp : &mut tokio::fs::File, hasher : &mut sha2::Sha256) -> std::io::Result<()> {
    
    //let (b0, b1) = bufs.split_at_mut(1);
    //let buf0 = &mut b0[0];
    //let buf1 = &mut b1[0];

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

async fn hash_files(channel : crossbeam::channel::Receiver<FileToHash>, bufsize : usize) {

    let mut buf0 = vec![0u8; bufsize];
    let mut buf1 = vec![0u8; bufsize];
    //let mut bufs = [buf0, buf1];

    let mut hasher = sha2::Sha256::new();
    let mut hash_output = GenericArray::default();

    for file in channel {

        match tokio::fs::File::options().read(true).open(&file.name).await {
            Err(e) => eprintln!("OPEN {e}"),
            Ok(mut fp) => {
                match hash_file(&mut buf0, &mut buf1, &mut fp, &mut hasher).await {
                    Err(read_err) => eprintln!("READ {read_err}"),
                    Ok(()) => {
                        hasher.finalize_into_reset(&mut hash_output);
                        println!("{hash_output:X}\t{}\t{}", file.len, file.name.display())
                    }
                }
            }
        }
    }
}

async fn main_hash(bufsize : usize) {

    let directory_to_hash = std::env::args().nth(1).unwrap_or_else(|| ".".to_string());

    let (enum_send_channel, enum_recv_channel) = crossbeam::channel::bounded(256);

    let mut set = tokio::task::JoinSet::new();
    for _ in 0..16 {
        set.spawn(hash_files(enum_recv_channel.clone(), bufsize ) );
    }

    let enum_handle = std::thread::spawn(|| {
        enumerate(directory_to_hash, enum_send_channel);
    });

    enum_handle.join().unwrap();
    while let Some(item) = set.join_next().await {
        let () = item.unwrap();
    }
}

fn main() {
    tokio::runtime::Builder::new_multi_thread()
        //.worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(main_hash(4096))
}
