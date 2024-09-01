use std::{io::Write, ops::DerefMut, path::PathBuf, sync::{atomic, Arc, Mutex}, time::{Duration, Instant}};
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use tokio::io::{AsyncReadExt};
use sha2::{Digest};

struct Stats {
	files_read : AtomicU64,
	bytes_read : AtomicU64,
	errors     : AtomicU64,
}

impl Stats {
	pub fn new() -> Self {
		Stats {
			files_read : AtomicU64::new(0),
			bytes_read : AtomicU64::new(0),
			errors : AtomicU64::new(0)
		}
	}
}

struct FileToHash {
    len : u64,
    name : std::path::PathBuf
}

/*
fn write_error(api : &str, err : std::io::Error, path : &Path, errors : &mut impl Write) {
	errors.write_fmt(format_args!("{api} {err} {path}\n")).expect("could not write to errors file");
}*/

fn write_error2(api : &str, err : std::io::Error, path : &Path, errors : &Arc<Mutex<impl Write>>) {
	errors.lock().unwrap().deref_mut()
		.write_fmt(format_args!("{api} {err} {}\n", path.display()))
		.expect("could not write to errors file");
}

struct DirWalker<W: ?Sized + Write> {
	channel : crossbeam_channel::Sender<FileToHash>,
	error_writer : Arc<Mutex<W>>,
	stats : Arc<Stats>
}

impl<W: Write> DirWalker<W> {
	pub fn new(channel : crossbeam_channel::Sender<FileToHash>, error_writer : Arc<Mutex<W>>, stats : Arc<Stats>) -> Self
	{
		DirWalker {
			channel,
			error_writer,
			stats
		}
	}
	pub fn enumerate(&mut self, dir : &PathBuf) {

		match std::fs::read_dir(&dir)  {
			Err(e) => {
				self.stats.errors.fetch_add(1, atomic::Ordering::Relaxed);
				write_error2("readdir(open)", e, dir.as_path(), &self.error_writer );
			},
			Ok(dir_iterator) => {
				for dir_entry in dir_iterator {
					match dir_entry {
						Err(e) => {
							self.stats.errors.fetch_add(1, atomic::Ordering::Relaxed);
							write_error2("readdir(next)", e, dir.as_path(), &self.error_writer );
						},
						Ok(entry) => {
							match entry.metadata() {
								Err(e) => {
									self.stats.errors.fetch_add(1, atomic::Ordering::Relaxed);
									write_error2("metadata", e, entry.path().as_path(), &self.error_writer );
								},
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

struct ByteCount {
	bytes : u64
}

impl std::fmt::Display for ByteCount {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let unit = 1024;
		if self.bytes < unit {
			//return fmt.Sprintf("%d B", b)
			write!(f, "{} B", self.bytes)
		}
		else {
			let (mut div, mut exp) = (unit, 0);

			let mut n = self.bytes / unit;
			while n >= unit {
				div *= unit;
				exp += 1;
				n /= unit
			}

			write!(f, "{:.2} {}iB", 
				(self.bytes as f32) / (div as f32),
				 "KMGTPE".chars().nth(exp).unwrap() )
		}
	}
}
/*
fn byte_count_iec(b u64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %ciB",
		float64(b)/float64(div), "KMGTPE"[exp])
}*/

async fn hash_file(buf0 : &mut Vec<u8>, buf1 : &mut Vec<u8>, fp : &mut tokio::fs::File, hasher : &mut sha2::Sha256, bytes_read : &AtomicU64) -> std::io::Result<()> {
    
    let mut read_in_flight = fp.read(buf1.as_mut_slice() );

    loop {
        let number_read = read_in_flight.await?;
        std::mem::swap(buf0, buf1);
        if number_read == 0 {
            return Ok(())
        }
        else {
			bytes_read.fetch_add(number_read.try_into().unwrap(), atomic::Ordering::Relaxed);
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
    root_dir : Arc<PathBuf>,
	stats : Arc<Stats>
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
				write_error2("open", e, file.name.as_path(), &errors );
				stats.errors.fetch_add(1, atomic::Ordering::Relaxed);
			},
            Ok(mut fp) => {
                match hash_file(&mut buf0, &mut buf1, &mut fp, &mut hasher, &stats.bytes_read).await {
                    Err(read_err) => {
						write_error2("read", read_err, file.name.as_path(), &errors );
						stats.errors.fetch_add(1, atomic::Ordering::Relaxed);
					},
                    Ok(()) => {
                        hasher.finalize_into_reset(&mut hash_output);
                        
                        tmp_string_buf.clear();

						match file.name.strip_prefix(root_dir.as_path()) {
							Err(e) => {
								panic!("filename.strip_prefix {e}. this should not happen. root_dir: {}, filename: {}", root_dir.display(), file.name.display())
							},
							Ok(p) => {
								write!(&mut tmp_string_buf, "{hash_output:x}\t{}\t{}\n", file.len, p.display()).expect("error writing hash line to tmp buffer");
								//hashes.send(hash_line).await.expect("hash_files/hashes/send");
								hashes.lock().unwrap().write(tmp_string_buf.as_bytes()).expect("error writing hash to file");
								stats.files_read.fetch_add(1, atomic::Ordering::Relaxed);
							}
						}
                    }
                }
            }
        }
    }
}

fn load_get_diff_set_last(val : &AtomicU64, last : &mut u64) -> (u64, u64) {
	let v = val.load(atomic::Ordering::Relaxed);
	let diff = v - *last;
	*last = v;
	(v,diff)
}

async fn print_stats(stats : Arc<Stats>) {
	let mut bytes_string = String::new();
	let mut last_bytes : u64 = 0;
	let mut last_files : u64 = 0;

	let pause_secs = 2;

	loop {
		tokio::time::sleep( Duration::from_secs(pause_secs) ).await;

		let (files, files_diff) = load_get_diff_set_last(&stats.files_read, &mut last_files);
		let (bytes, bytes_diff) = load_get_diff_set_last(&stats.bytes_read, &mut last_bytes);
		bytes_string.clear();
		use std::fmt::Write;
		write!(&mut bytes_string, "{}", ByteCount { bytes }).unwrap();
		println!("files: {:>12} {:>12} | files/s: {:>6} {:>4} MB/s | err: {}",
			files,
			bytes_string,
			files_diff/pause_secs,
			bytes_diff/pause_secs/1024/1024,
			stats.errors.load(atomic::Ordering::Relaxed));
	}
}

async fn main_hash(workers : usize, directoryname_to_hash : PathBuf, bufsize_read_hash : usize) {

	//let directoryname = Arc::new(directoryname_to_hash);
	
    let hashes_filename = "./hashes.txt";
    let errors_filename = "./errors.txt";
	let bufsize_hashwriter = 64 * 1024;
    let hash_writer = std::io::BufWriter::with_capacity(bufsize_hashwriter, std::fs::File::create(hashes_filename).expect("could not create hash result file"));
    let error_writer = std::io::BufWriter::new(std::fs::File::create(errors_filename).expect("could not create file for errors"));
    
    let mux_hash_writer = Arc::new( Mutex::new(hash_writer));
	let mux_error_writer = Arc::new( Mutex::new(error_writer));

    let (enum_send, enum_recv) = crossbeam_channel::bounded(256);

	let stats = Arc::new(Stats::new());

    let start = Instant::now();

	let root_dir = Arc::new(directoryname_to_hash.clone());
	println!("starting {} workers for directory {} with a read/hash buffer of {} bytes", workers, root_dir.display(), bufsize_read_hash);
	let mut tasks = tokio::task::JoinSet::new();
	for _ in 0..workers {
		tasks.spawn(hash_files(enum_recv.clone(), mux_hash_writer.clone(), mux_error_writer.clone(), bufsize_read_hash, root_dir.clone(), stats.clone() ) );
	}

	let _stats_task =tokio::spawn( print_stats(stats.clone()));

	let enum_dir = root_dir.clone();
	let enum_stats = stats.clone();
	let enum_thread = std::thread::spawn(
		move || {
			let mut dir_walker = DirWalker::new(enum_send, mux_error_writer, enum_stats);
			dir_walker.enumerate(&enum_dir);
		});
	// wait for enumerate and hashers to finish
    enum_thread.join().unwrap();
    while let Some(item) = tasks.join_next().await {
        let () = item.unwrap();
    }
    let duration = start.elapsed();

    println!("files\t{}\ndata\t{}\nduration\t{:?}",
			 stats.files_read.load(Relaxed),
			 ByteCount { bytes: stats.bytes_read.load(Relaxed) },
			 duration);
}

const HELP: &str = "\
Usage: hashp.exe [OPTIONS] [DIRECTORY]

Arguments:
  [DIRECTORY]  [default: .]

Options:
  -b <BUFSIZE>  bufsize in kilobytes (read file) [default: 64]
  -h            Print help

workers can be configured by setting the TOKIO_WORKER_THREADS environment variable
";

#[derive(Debug)]
struct AppArgs {
    directory : std::path::PathBuf,
    /// bufsize in kilobytes (read file)
    bufsize : usize
}

fn parse_args() -> Result<AppArgs, pico_args::Error> {
	let mut pargs = pico_args::Arguments::from_env();

	// Help has a higher priority and should be handled separately.
	if pargs.contains(["-h", "--help"]) {
		print!("{}", HELP);
		std::process::exit(0);
	}

	let args = AppArgs {
		bufsize: pargs.opt_value_from_str("-b")?.unwrap_or(64),
		directory: pargs.opt_free_from_str()?.unwrap_or(".".into())
	};

	// It's up to the caller what to do with the remaining arguments.
	let remaining = pargs.finish();
	if !remaining.is_empty() {
		eprintln!("Warning: unused arguments left: {:?}.", remaining);
	}

	Ok(args)
}

fn main() {

	let args = match parse_args() {
		Ok(v) => v,
		Err(e) => {
			eprintln!("Error: {}.", e);
			std::process::exit(1);
		}
	};

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    
    rt.block_on(main_hash(rt.metrics().num_workers(), args.directory, args.bufsize * 1024))
}
