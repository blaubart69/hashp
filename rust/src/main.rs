
struct FileToHash {
    len : u64,
    name : std::path::PathBuf
}

fn enumerate(channel : crossbeam::channel::Sender<FileToHash>) {
    for dir_entry in walkdir::WalkDir::new(".") {
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

async fn hash_files(channel : crossbeam::channel::Receiver<FileToHash>) {
    for file in channel {
        let name = file.name.display();
        println!("hash: {name}");
    }
}

async fn main_hash() {

    let (enum_send_channel, enum_recv_channel) = crossbeam::channel::bounded(256);

    let mut set = tokio::task::JoinSet::new();
    for i in 0..16 {
        set.spawn(hash_files(enum_recv_channel.clone()));
    }

    let enum_handle = std::thread::spawn(|| {
        enumerate(enum_send_channel);
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
        .block_on(main_hash())
}
