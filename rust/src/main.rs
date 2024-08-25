use std::{io::Write, ops::DerefMut, path::PathBuf, sync::{Arc, Mutex}, time::Instant};

use tokio::io::{AsyncReadExt};
use sha2::{Digest};

struct FileToHash {
    len : u64,
    name : std::path::PathBuf
}

fn write_error(api : &str, err : std::io::Error, errors : &mut impl Write) {
	errors.write_fmt(format_args!("{api} {err}\n")).expect("could not write to errors file");
}

struct DirWalker<W: ?Sized + Write> {
	channel : crossbeam_channel::Sender<FileToHash>,
	error_writer : Arc<Mutex<W>>
}

impl<W: Write> DirWalker<W> {
	pub fn new(channel : crossbeam_channel::Sender<FileToHash>, error_writer : Arc<Mutex<W>>) -> Self
	{
		DirWalker {
			channel,
			error_writer
		}
	}
	pub fn enumerate(&mut self, dir : &PathBuf) {

		match std::fs::read_dir(&dir)  {
			Err(e) => write_error("readdir(open)", e, self.error_writer.lock().unwrap().deref_mut() ),
			Ok(dir_iterator) => {
				for dir_entry in dir_iterator {
					match dir_entry {
						Err(e) => write_error("readdir(next)", e, self.error_writer.lock().unwrap().deref_mut() ),
						Ok(entry) => {
							match entry.metadata() {
								Err(e) => write_error("metadata", e, self.error_writer.lock().unwrap().deref_mut() ),
								Ok(meta) => {
									if meta.is_dir() {
										self.enumerate(&entry.path())	//
									}
									else {
										let _ = self.channel.send( 
											FileToHash { 
												len: meta.len(), 
												name: entry.path() })
										.expect("channel/enum/send");
									}
								}
							}
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
	errors : Arc<Mutex<impl Write>>,
	bufsize : usize,
    root_dir : Arc<PathBuf>
) {

    let mut buf0 = vec![0u8; bufsize];
    let mut buf1 = vec![0u8; bufsize];
    //let mut bufs = [buf0, buf1];

    let mut hasher = sha2::Sha256::new();
    let mut hash_output = sha2::digest::generic_array::GenericArray::default();
    let mut tmp_string_buf = String::new();

    for file in files {
		use std::fmt::Write;
        match tokio::fs::File::options().read(true).open(&file.name).await {
            Err(e) => {
				write_error("open", e, errors.lock().unwrap().deref_mut() );
			},
            Ok(mut fp) => {
                match hash_file(&mut buf0, &mut buf1, &mut fp, &mut hasher).await {
                    Err(read_err) => {
						write_error("read", read_err, errors.lock().unwrap().deref_mut() );
					},
                    Ok(()) => {
                        hasher.finalize_into_reset(&mut hash_output);
                        
                        
                        tmp_string_buf.clear();

						match file.name.strip_prefix(root_dir.as_path()) {
							Err(e) => {
								panic!("filename.strip_prefix {e}. this should not happen. root_dir: {}, filename: {}", root_dir.display(), file.name.display())
							},
							Ok(p) => {
								write!(&mut tmp_string_buf, "{hash_output:X}\t{}\t{}\n", file.len, p.display()).expect("error writing hash line to tmp buffer");
								//hashes.send(hash_line).await.expect("hash_files/hashes/send");
								hashes.lock().unwrap().write(tmp_string_buf.as_bytes()).expect("error writing hash to file");
							}
						}
                    }
                }
            }
        }
    }
}

async fn main_hash(workers : usize) {

	let directoryname_to_hash = std::env::args().nth(1).unwrap_or_else(|| ".".to_string());
	
    let hashes_filename = "./hashes.txt";
    let errors_filename = "./errors.txt";
	let bufsize = 64 * 1024;
    let hash_writer = std::io::BufWriter::with_capacity(64*1024, std::fs::File::create(hashes_filename).expect("could not create hash result file"));
    let error_writer = std::io::BufWriter::new(std::fs::File::create(errors_filename).expect("could not create file for errors"));
    

    let mux_hash_writer = Arc::new( Mutex::new(hash_writer));
	let mux_error_writer = Arc::new( Mutex::new(error_writer));

    let (enum_send, enum_recv) = crossbeam_channel::bounded(256);

    let start = Instant::now();

	let root_dir = Arc::new(PathBuf::from(&directoryname_to_hash));
	println!("starting {} hash workers for directory {}", workers, root_dir.display());
	let mut tasks = tokio::task::JoinSet::new();
	for _ in 0..workers {
		tasks.spawn(hash_files(enum_recv.clone(), mux_hash_writer.clone(), mux_error_writer.clone(), bufsize, root_dir.clone() ) );
	}

	let enum_dir = PathBuf::from(directoryname_to_hash);
    let enum_thread = std::thread::spawn(
		move || {
			let mut dir_walker = DirWalker::new(enum_send, mux_error_writer);
			dir_walker.enumerate(&enum_dir);
		});

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
