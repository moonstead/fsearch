//! The engine against unusable saved indexes, stops while one loads, a lock
//! another process holds, and corrupt content tables. Each crawl opens only
//! a fixture tree and the folders on the way to it (whatever else appears
//! meanwhile), one engine at a time (skip lists are process-wide), and
//! every wait has a deadline.

use fsearch::{Engine, Options, Query};
use std::io::BufRead;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());
const DEADLINE: Duration = Duration::from_secs(20);

struct Fixture {
    root: PathBuf,
    tree: PathBuf,
    home: String,
    dir: PathBuf,
}

impl Fixture {
    fn new(name: &str, files: usize) -> Fixture {
        let root = PathBuf::from(format!("/private/tmp/fsearch-engine-test-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let tree = root.join("tree");
        for i in 0..files {
            let p = tree.join(format!("d{}/sub{}/file-{i}.txt", i % 50, i % 7));
            std::fs::create_dir_all(p.parent().unwrap()).unwrap();
            std::fs::write(&p, format!("line {i}\n")).unwrap();
        }
        std::fs::create_dir_all(&tree).unwrap();
        std::fs::write(tree.join("alpha-report.txt"), b"alpha\n").unwrap();
        let tree = tree.canonicalize().unwrap();
        Fixture { home: tree.to_string_lossy().into(), dir: root.join("index"), root, tree }
    }

    /// Everything skipped but the tree and the way to it: a folder that
    /// appears elsewhere later is never crawled.
    fn options(&self, content: bool) -> Options {
        Options {
            dir: self.dir.clone(),
            home: self.home.clone(),
            skip: Some(vec![PathBuf::from("/")]),
            skip_names: vec![".*".into()],
            keep: vec![self.tree.clone()],
            content,
        }
    }

    fn index(&self) -> PathBuf {
        self.dir.join("index.bin")
    }

    /// Crawl once and save, then stop.
    fn build(&self, content: bool) {
        let e = Engine::start(self.options(content)).unwrap();
        assert!(wait(DEADLINE, || e.status().ready), "first crawl");
        if content {
            assert!(wait(Duration::from_secs(60), || e.status().content_docs > 0 && e.status().content_pending == 0), "first content build");
        }
        stop(e, &self.dir);
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn wait(deadline: Duration, mut ok: impl FnMut() -> bool) -> bool {
    let t = Instant::now();
    while t.elapsed() < deadline {
        if ok() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    false
}

/// Stop and wait until the engine's thread has let go of the index lock.
fn stop(e: Engine, dir: &Path) {
    e.stop();
    drop(e);
    let lock = std::fs::File::open(dir.join("daemon.lock")).unwrap();
    assert!(wait(DEADLINE, || lock.try_lock().is_ok()), "lock still held after stop");
    lock.unlock().unwrap();
}

/// A search for the file `name` on another thread: how many hits are that
/// file, or the error; None if it did not answer within the deadline (it
/// would block a caller forever).
fn search(e: &Engine, home: &str, name: &str, deadline: Duration) -> Option<Result<usize, String>> {
    let (tx, rx) = std::sync::mpsc::channel();
    let (e, q, end) = (e.clone(), Query::parse(&format!("{name} limit:50"), home).unwrap(), format!("/{name}"));
    std::thread::spawn(move || {
        let _ = tx.send(e.search(&q).map(|v| v.iter().filter(|f| f.path.to_string_lossy().ends_with(&end)).count()));
    });
    rx.recv_timeout(deadline).ok()
}

fn field(b: &[u8], k: usize) -> u64 {
    u64::from_le_bytes(b[8 + k * 8..16 + k * 8].try_into().unwrap())
}

fn set_field(b: &mut [u8], k: usize, v: u64) {
    b[8 + k * 8..16 + k * 8].copy_from_slice(&v.to_le_bytes());
}

/// Byte offset of name-index section `s` (in `index.rs` order), worked out
/// here rather than by the code under test.
fn section(b: &[u8], s: usize) -> usize {
    let (n, d, u, nl) = (field(b, 0) as usize, field(b, 1) as usize, field(b, 2) as usize, field(b, 3) as usize);
    let lens = [
        u.div_ceil(64) * fsearch::index::NBITMAPS * 8,
        (u + 1) * 4,
        nl,
        n * 4,
        n,
        n * 4,
        n * 4,
        n * 4,
        d * 4,
        d * 4,
        d * 4,
        d * 4,
        d,
        d * 4,
        (u + 1) * 4,
        n * 4,
        u,
        u * 4,
        u,
        256 * 16,
        n.div_ceil(fsearch::index::ZONE) * 16,
    ];
    lens[..s].iter().fold(4096, |at, l| (at + l + 63) & !63)
}

const NAME_OFF: usize = 1;
const PARENT: usize = 5;
const DIR_PARENT: usize = 13;

fn set32(b: &mut [u8], s: usize, k: usize, v: u32) {
    let at = section(b, s) + 4 * k;
    b[at..at + 4].copy_from_slice(&v.to_le_bytes());
}

/// Start on a saved index that `edit` spoiled: the engine must answer
/// searches at once (not block on the load), rebuild, find the fixture's
/// files, and leave a saved index that loads.
fn rebuilds(name: &str, edit: fn(&mut Vec<u8>)) {
    let _one = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    let f = Fixture::new(name, 2000);
    f.build(false);
    let mut b = std::fs::read(f.index()).unwrap();
    edit(&mut b);
    rebuilds_from(&f, name, b);
}

/// Start on the saved index `b` (written here) and check the engine
/// rebuilds it (see `rebuilds`).
fn rebuilds_from(f: &Fixture, name: &str, b: Vec<u8>) {
    std::fs::write(f.index(), &b).unwrap();
    let e = Engine::start(f.options(false)).unwrap();
    assert!(search(&e, &f.home, "alpha-report.txt", DEADLINE).is_some(), "{name}: a search blocked");
    assert!(wait(DEADLINE, || e.status().ready), "{name}: not ready");
    assert!(wait(DEADLINE, || std::fs::read(f.index()).is_ok_and(|now| now != b)), "{name}: the spoiled index was kept, not rebuilt");
    assert_eq!(search(&e, &f.home, "alpha-report.txt", DEADLINE), Some(Ok(1)), "{name}: wrong results");
    assert!(fsearch::index::Index::load(&f.index()).is_some(), "{name}: the saved index still does not load");
    stop(e, &f.dir);
}

macro_rules! rebuild_tests {
    ($($name:ident: $edit:expr;)*) => {$(
        #[test]
        fn $name() {
            rebuilds(stringify!($name), $edit);
        }
    )*};
}

/// Bits set past the last name in every name bitmap: a search would turn
/// one into a name id past the name table.
#[test]
fn rebuilds_bits_past_the_last_name() {
    let _one = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    let f = Fixture::new("bits_past_names", 2000);
    // A name count that leaves bits past it in the last word: add names
    // until it does (the folders on the way contribute names too).
    let mut b = Vec::new();
    for extra in 0..8 {
        f.build(false);
        b = std::fs::read(f.index()).unwrap();
        if !field(&b, 2).is_multiple_of(64) {
            break;
        }
        std::fs::write(f.tree.join(format!("extra-name-{extra}.txt")), b"").unwrap();
    }
    let u = field(&b, 2) as usize;
    assert!(!u.is_multiple_of(64), "no bits past the last name to set");
    let words = u.div_ceil(64);
    for bitmap in 0..fsearch::index::NBITMAPS {
        let at = 4096 + 8 * (bitmap * words + words - 1);
        let w = u64::from_le_bytes(b[at..at + 8].try_into().unwrap()) | 1 << (u % 64);
        b[at..at + 8].copy_from_slice(&w.to_le_bytes());
    }
    rebuilds_from(&f, "bits_past_names", b);
}

rebuild_tests! {
    rebuilds_an_old_format: |b| b[..8].copy_from_slice(b"FSIDX007");
    rebuilds_a_truncated_index: |b| b.truncate(b.len() / 2);
    rebuilds_an_empty_index: |b| b.clear();
    rebuilds_a_zeroed_body: |b| b[4096..].iter_mut().for_each(|x| *x = 0);
    rebuilds_a_random_body: |b| {
        let mut s = 0x9e37_79b9_7f4a_7c15u64;
        for x in b[4096..].iter_mut() {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            *x = s as u8;
        }
    };
    rebuilds_zero_folders: |b| set_field(b, 1, 0);
    rebuilds_a_names_length_that_wraps: |b| set_field(b, 3, u64::MAX - 4095);
    rebuilds_a_name_count_that_wraps: |b| set_field(b, 2, 1 << 63);
    rebuilds_bad_name_offsets: |b| {
        let end = field(b, 3) as u32;
        set32(b, NAME_OFF, 1, end + 1)
    };
    rebuilds_a_folder_cycle: |b| set32(b, DIR_PARENT, 1, 1);
    rebuilds_an_entry_in_no_folder: |b| {
        let d = field(b, 1) as u32;
        set32(b, PARENT, 1, d)
    };
    rebuilds_an_event_id_from_the_future: |b| set_field(b, 4, u64::MAX);
}

/// `stop` while a saved index loads: searches answer `Err` at once and the
/// engine never becomes ready, valid index or not.
fn stop_while_loading(name: &str, edit: fn(&mut Vec<u8>)) {
    let _one = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    let f = Fixture::new(name, 20000);
    f.build(false);
    let mut b = std::fs::read(f.index()).unwrap();
    edit(&mut b);
    std::fs::write(f.index(), &b).unwrap();
    let e = Engine::start(f.options(false)).unwrap();
    e.stop();
    assert!(matches!(search(&e, &f.home, "alpha-report.txt", DEADLINE), Some(Err(_))), "{name}: search after stop did not fail at once");
    std::thread::sleep(Duration::from_millis(500));
    assert!(!e.status().ready, "{name}: ready after stop");
    assert!(matches!(search(&e, &f.home, "alpha-report.txt", DEADLINE), Some(Err(_))), "{name}: search answered after stop");
    stop(e, &f.dir);
}

#[test]
fn stop_while_a_valid_index_loads() {
    stop_while_loading("stop_valid", |_| {});
}

#[test]
fn stop_while_a_corrupt_index_loads() {
    stop_while_loading("stop_corrupt", |b| set_field(b, 1, 0));
}

/// Another process holds the lock, and the saved index is one this version
/// cannot read (as on the first start after an update, with the old
/// version still running). Once that process goes, the engine takes over
/// and builds.
#[test]
fn takes_over_from_an_owner_that_leaves_no_usable_index() {
    let _one = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    let f = Fixture::new("takeover", 2000);
    f.build(false);
    let mut b = std::fs::read(f.index()).unwrap();
    b[..8].copy_from_slice(b"FSIDX007");
    std::fs::write(f.index(), &b).unwrap();
    // The other process: this test binary again, running `lock_holder`.
    // It holds the lock until its stdin closes: no timing involved.
    let mut holder = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["lock_holder", "--exact", "--ignored", "--nocapture", "--test-threads=1"])
        .env("FSEARCH_TEST_HOLD", f.dir.join("daemon.lock"))
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let mut out = std::io::BufReader::new(holder.stdout.take().unwrap()).lines().map_while(Result::ok);
    assert!(out.any(|l| l.trim_end().ends_with("holding the lock")), "the holder did not take the lock");
    let e = Engine::start(f.options(false)).unwrap();
    std::thread::sleep(Duration::from_millis(300));
    let st = e.status();
    assert!(!st.owner && !st.ready, "owner {} ready {} while another process holds the lock", st.owner, st.ready);
    drop(holder.stdin.take());
    // Read the rest, so the holder's last lines have somewhere to go.
    out.for_each(drop);
    assert!(holder.wait().unwrap().success());
    let t = Instant::now();
    let took_over = wait(DEADLINE, || e.status().ready && e.status().owner);
    let st = e.status();
    assert!(took_over, "after {:.1?}: owner {} ready {} entries {}", t.elapsed(), st.owner, st.ready, st.entries);
    assert_eq!(search(&e, &f.home, "alpha-report.txt", DEADLINE), Some(Ok(1)));
    assert!(fsearch::index::Index::load(&f.index()).is_some());
    stop(e, &f.dir);
}

/// Run by `takes_over_...` in a second process: hold the index lock until
/// stdin closes.
#[test]
#[ignore = "a helper process for another test"]
fn lock_holder() {
    let Some(path) = std::env::var_os("FSEARCH_TEST_HOLD") else { return };
    let f = std::fs::File::create(path).unwrap();
    f.try_lock().unwrap();
    println!("holding the lock");
    std::io::copy(&mut std::io::stdin(), &mut std::io::sink()).unwrap();
    drop(f);
}

/// A content segment whose hash order names a content past its table is
/// not loaded; the engine indexes the contents again and finds new text.
#[test]
fn rejects_out_of_range_content_ids() {
    let _one = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    let f = Fixture::new("content_ids", 200);
    f.build(true);
    let cdir = f.dir.join("content");
    let seg = std::fs::read_dir(&cdir).unwrap().flatten().map(|e| e.path()).find(|p| p.extension().is_some_and(|x| x == "fsc")).expect("a segment");
    let mut b = std::fs::read(&seg).unwrap();
    assert_eq!(&b[..8], b"FSCSEG10");
    let [ndocs, ntri, plen, paths_len, nwords, nown, nlong]: [usize; 7] = std::array::from_fn(|k| field(&b, k) as usize);
    assert!(nown > 0);
    let (d, c) = (ndocs * 4, nown * 4);
    #[rustfmt::skip]
    let lens = [ntri * 4, (ntri + 1) * 4, plen, d + 4, paths_len, ndocs * 8, d, d, ndocs, d, d, d, d, d, c, c, c * 4, c, c + 4, nwords * 8, c + 4, nlong * 8];
    let by_hash = lens[..17].iter().fold(4096, |at, l| (at + l + 63) & !63);
    b[by_hash..by_hash + 4].copy_from_slice(&(u32::MAX - 15).to_le_bytes());
    std::fs::write(&seg, &b).unwrap();
    assert_eq!(fsearch::content::Content::open_shared(cdir.clone()).docs(), 0, "the corrupt segment was loaded");
    let e = Engine::start(f.options(true)).unwrap();
    std::fs::write(Path::new(&f.home).join("kilo-note.txt"), b"kiloword\n").unwrap();
    assert!(wait(DEADLINE, || e.status().ready), "not ready");
    let found = || {
        let q = Query::parse("grep:kiloword", &f.home).unwrap();
        let g = fsearch::Grep::new(q.grep.as_deref().unwrap(), q.grep_mode).unwrap();
        e.grep(&q, &g).is_ok_and(|(r, _)| r.files.len() == 1)
    };
    assert!(wait(Duration::from_secs(60), found), "new text not found");
    stop(e, &f.dir);
}
