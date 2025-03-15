use anyhow::{bail, ensure, Context, Result};
use clap::Parser;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    collections::{
        btree_map::Entry::{Occupied, Vacant},
        BTreeMap, HashSet, VecDeque,
    },
    env::set_current_dir,
    fs::{create_dir_all, read_dir, rename, File},
    io::{self, Read, Seek, SeekFrom, Write},
    mem,
    net::ToSocketAddrs,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed},
        Mutex,
    },
    thread,
    time::{Duration, Instant},
};

/// Maintain a local copy of all of crates.io.
#[derive(Parser)]
struct Args {
    /// The directory to put everything in.
    ///
    /// This directory will be created if it doesn't exist yet.
    ///
    /// Several subdirectories will be created inside of this folder.
    dir: PathBuf,

    /// Number of parallel connections for downloading crates.
    #[clap(short, long, default_value_t = 200)]
    connections: usize,
}

fn main() -> Result<()> {
    let args = Args::parse();

    create_dir_all(&args.dir)?;
    set_current_dir(&args.dir)?;

    println!("Updating index...");
    Index::update()?;

    println!("Loading index...");
    let index = Index::read()?;

    println!(
        "Loaded metadata of {} crates with {} versions",
        index.crates.len(),
        index.crates.values().map(|c| c.len()).sum::<usize>(),
    );

    download_crates(&index, &args)?;

    Ok(())
}

fn download_crates(index: &Index, args: &Args) -> Result<()> {
    let n_total = index.crates.values().map(|c| c.len()).sum::<usize>();

    let mut x403_file = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open("403")?;
    let mut x403_log = String::new();
    x403_file.read_to_string(&mut x403_log)?;
    let x403_set: HashSet<&str> = x403_log.lines().collect();

    let mut queue = VecDeque::with_capacity(n_total);
    let mut n_todo = 0;
    for (name, versions) in &index.crates {
        create_dir_all(format!("crates/{name}"))?;
        for (version, data) in versions {
            let file = format!("crates/{name}/{name}-{version}.crate");
            if !x403_set.contains(file.as_str()) && !Path::new(&file).exists() {
                n_todo += 1;
                queue.push_back((name, version, &data.cksum));
            }
        }
    }

    if n_todo == 0 {
        println!("Cache already contains all {} crate files", n_total);
        return Ok(());
    }

    println!("Cache already contains {} crate files", n_total - n_todo);

    let n_threads = args.connections.min(n_todo);
    println!("Downloading the remaining {n_todo} using {n_threads} parallel connections...\n");

    let host = "static.crates.io";
    let sock_addrs = format!("{host}:443").to_socket_addrs()?.collect::<Vec<_>>();

    let client = reqwest::blocking::Client::builder()
        .user_agent("cratesync")
        .resolve_to_addrs(host, &sock_addrs)
        .build()?;

    let queue = Mutex::new(queue);
    let errors = Mutex::new(Vec::new());
    let n_done = AtomicUsize::new(0);
    let bytes = AtomicU64::new(0);
    let start = Instant::now();

    thread::scope(|s| -> Result<()> {
        for _ in 0..n_threads {
            s.spawn(|| loop {
                let item = queue.lock().unwrap().pop_front();
                let Some((name, version, cksum)) = item else {
                    break;
                };
                let url = format!("https://{host}/crates/{name}/{name}-{version}.crate");
                let file = format!("crates/{name}/{name}-{version}.crate");
                let partial_file = format!("{file}.partial");
                if let Err(e) = || -> Result<()> {
                    let response = client.get(&url).send()?;
                    if response.status() == reqwest::StatusCode::FORBIDDEN {
                        (&x403_file).write_all(format!("{file}\n").as_bytes())?;
                        return Ok(());
                    }
                    let mut response = response.error_for_status()?;
                    let mut f = File::options()
                        .read(true)
                        .write(true)
                        .create(true)
                        .truncate(true)
                        .open(&partial_file)?;
                    let b = response.copy_to(&mut f)?;
                    bytes.fetch_add(b, Relaxed);
                    f.seek(SeekFrom::Start(0))?;
                    let mut hasher = Sha256::new();
                    io::copy(&mut f, &mut hasher)?;
                    let hash = base16ct::lower::encode_string(&hasher.finalize());
                    ensure!(
                        &hash == cksum,
                        "invalid checksum on {file:?}: should be {cksum}, but is {hash}"
                    );
                    drop(f);
                    rename(partial_file, file)?;
                    Ok(())
                }() {
                    errors.lock().unwrap().push(e);
                }
                n_done.fetch_add(1, Relaxed);
            });
        }
        loop {
            let errors = mem::take(&mut *errors.lock().unwrap());
            if !errors.is_empty() {
                for e in errors {
                    println!("error: {e:#}");
                }
                println!();
            }
            let n_done = n_done.load(Relaxed);
            let secs = start.elapsed().as_secs().max(1);
            println!(
                "\x1b[ADownloading... {percent:3}% ({n_done}/{n_todo} - {crate_speed} crate/s - {kb_speed} KiB/s)\x1b[J",
                percent = n_done * 100 / n_todo,
                crate_speed = n_done as u64 / secs,
                kb_speed = bytes.load(Relaxed) / secs / 1024,
            );
            if n_done == n_todo {
                break;
            }
            thread::sleep(Duration::from_secs(1));
        }
        Ok(())
    })?;

    Ok(())
}

#[derive(Default)]
struct Index {
    /// name -> version -> CrateData
    crates: BTreeMap<String, BTreeMap<String, CrateData>>,
}

#[derive(Debug, Deserialize)]
struct CrateData {
    cksum: String,
    #[allow(unused)]
    yanked: bool,
}

#[derive(Debug, Deserialize)]
struct Metadata {
    name: String,
    vers: String,
    #[serde(flatten)]
    data: CrateData,
}

impl Index {
    fn add_dir(&mut self, dir: impl AsRef<Path>) -> Result<()> {
        for e in read_dir(dir.as_ref())? {
            let e = e?;
            let name = e.file_name();
            let name = name.to_str().context("invalid utf-8 file name in index")?;
            if e.file_type()?.is_dir() {
                self.add_dir(e.path())?;
            } else {
                let content = std::fs::read_to_string(e.path())
                    .with_context(|| format!("unable to read {:?}", e.path()))?;
                let mut entry = BTreeMap::default();
                let mut crate_name = None;
                for line in content.lines() {
                    let metadata = serde_json::from_str::<Metadata>(line)
                        .with_context(|| format!("unable to parse {:?}", e.path()))?;
                    assert!(
                        name.eq_ignore_ascii_case(&metadata.name),
                        "{:?} contains unexpected crate name {:?}",
                        e.path(),
                        metadata.name,
                    );
                    crate_name = Some(metadata.name);
                    entry.insert(metadata.vers, metadata.data);
                }
                if let Some(crate_name) = crate_name {
                    match self.crates.entry(crate_name) {
                        Vacant(e) => {
                            e.insert(entry);
                        }
                        Occupied(e) => bail!("duplicate entry for `{}`", e.key()),
                    }
                }
            }
        }
        Ok(())
    }

    fn update() -> Result<()> {
        if !Path::new("crates.io-index").exists() {
            git(["clone", "https://github.com/rust-lang/crates.io-index"])?;
        }
        git(["-C", "crates.io-index", "fetch"])?;
        git(["-C", "crates.io-index", "reset", "--hard", "origin/master"])?;
        Ok(())
    }

    fn read() -> Result<Self> {
        let mut index = Index {
            crates: BTreeMap::new(),
        };

        for e in read_dir("crates.io-index")? {
            let e = e?;
            if e.file_name().as_encoded_bytes().starts_with(b".") {
                // Ignore hidden directories like .git and .github.
            } else if e.file_type()?.is_dir() {
                index.add_dir(e.path())?;
            }
        }

        Ok(index)
    }
}

fn git<const N: usize>(args: [&str; N]) -> Result<()> {
    let status = Command::new("git").args(args).spawn()?.wait()?;
    ensure!(status.success(), "git command failed");
    Ok(())
}
