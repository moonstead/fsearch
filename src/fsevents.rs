//! Whole-disk FSEvents stream, directory granularity, replayable by event id.

use std::ffi::{CStr, c_void};
use std::sync::mpsc::Sender;

pub const MUST_SCAN_SUBDIRS: u32 = 0x1;
pub const USER_DROPPED: u32 = 0x2;
pub const KERNEL_DROPPED: u32 = 0x4;
pub const HISTORY_DONE: u32 = 0x10;
const CREATE_FLAG_NO_DEFER: u32 = 0x2;

pub struct Event {
    pub path: Vec<u8>,
    pub flags: u32,
    pub id: u64,
    /// Which `watch` delivered it (its `stream` number).
    pub stream: u32,
}

/// A stream's callback context.
struct Info {
    tx: Sender<Vec<Event>>,
    stream: u32,
}

#[repr(C)]
struct Context {
    version: isize,
    info: *mut c_void,
    retain: *const c_void,
    release: *const c_void,
    copy_description: *const c_void,
}

type Callback = extern "C" fn(*mut c_void, *mut c_void, usize, *mut c_void, *const u32, *const u64);

/// CoreServices (and the CoreFoundation it brings) are loaded on first use,
/// not linked: CoreFoundation's initializer costs every process that links
/// it ~1 ms at launch, and only the daemon watches the disk.
struct Api {
    create: unsafe extern "C" fn(*const c_void, Callback, *const Context, *const c_void, u64, f64, u32) -> *mut c_void,
    set_queue: unsafe extern "C" fn(*mut c_void, *mut c_void),
    start: unsafe extern "C" fn(*mut c_void) -> u8,
    stop: unsafe extern "C" fn(*mut c_void),
    invalidate: unsafe extern "C" fn(*mut c_void),
    release: unsafe extern "C" fn(*mut c_void),
    current_id: unsafe extern "C" fn() -> u64,
    string: unsafe extern "C" fn(*const c_void, *const i8, u32) -> *const c_void,
    array: unsafe extern "C" fn(*const c_void, *const *const c_void, isize, *const c_void) -> *const c_void,
    array_callbacks: *const c_void,
    cf_release: unsafe extern "C" fn(*const c_void),
}

// Function pointers and a constant's address: fine to share.
unsafe impl Send for Api {}
unsafe impl Sync for Api {}

fn api() -> &'static Api {
    static API: std::sync::OnceLock<Api> = std::sync::OnceLock::new();
    API.get_or_init(|| unsafe {
        let open = |path: &CStr| {
            let h = libc::dlopen(path.as_ptr(), libc::RTLD_LAZY);
            assert!(!h.is_null(), "cannot load {path:?}");
            h
        };
        let (cs, cf) = (
            open(c"/System/Library/Frameworks/CoreServices.framework/CoreServices"),
            open(c"/System/Library/Frameworks/CoreFoundation.framework/CoreFoundation"),
        );
        Api {
            create: sym(cs, c"FSEventStreamCreate"),
            set_queue: sym(cs, c"FSEventStreamSetDispatchQueue"),
            start: sym(cs, c"FSEventStreamStart"),
            stop: sym(cs, c"FSEventStreamStop"),
            invalidate: sym(cs, c"FSEventStreamInvalidate"),
            release: sym(cs, c"FSEventStreamRelease"),
            current_id: sym(cs, c"FSEventsGetCurrentEventId"),
            string: sym(cf, c"CFStringCreateWithCString"),
            array: sym(cf, c"CFArrayCreate"),
            array_callbacks: sym(cf, c"kCFTypeArrayCallBacks"),
            cf_release: sym(cf, c"CFRelease"),
        }
    })
}

/// A symbol of a loaded image, as `T` (a function pointer, or a data
/// symbol's address).
unsafe fn sym<T>(h: *mut c_void, name: &CStr) -> T {
    let p = unsafe { libc::dlsym(h, name.as_ptr()) };
    assert!(!p.is_null(), "missing {name:?}");
    unsafe { std::mem::transmute_copy(&p) }
}

/// The newest event id on the system.
pub fn current_id() -> u64 {
    unsafe { (api().current_id)() }
}

unsafe extern "C" {
    fn dispatch_queue_create(label: *const i8, attr: *const c_void) -> *mut c_void;
    fn dispatch_queue_attr_make_with_qos_class(attr: *const c_void, qos: u32, relative_priority: i32) -> *const c_void;
    fn dispatch_release(object: *mut c_void);
}

/// The context's release callback: FSEvents calls it once, when it frees
/// the stream, so no callback can still be using the `Info`.
extern "C" fn release_info(info: *const c_void) {
    let info = unsafe { Box::from_raw(info as *mut Info) };
    #[cfg(test)]
    RELEASED.lock().unwrap().push(info.stream);
    drop(info);
}

/// The stream numbers of the contexts `release_info` freed, and of every
/// context dropped, however (tests count their own streams' entries).
#[cfg(test)]
static RELEASED: std::sync::Mutex<Vec<u32>> = std::sync::Mutex::new(Vec::new());
#[cfg(test)]
static INFO_FREED: std::sync::Mutex<Vec<u32>> = std::sync::Mutex::new(Vec::new());

#[cfg(test)]
impl Drop for Info {
    fn drop(&mut self) {
        INFO_FREED.lock().unwrap().push(self.stream);
    }
}

extern "C" fn on_events(_s: *mut c_void, info: *mut c_void, n: usize, paths: *mut c_void, flags: *const u32, ids: *const u64) {
    let info = unsafe { &*(info as *const Info) };
    let paths = paths as *const *const i8;
    let batch = (0..n)
        .map(|i| unsafe {
            Event { path: CStr::from_ptr(*paths.add(i)).to_bytes().to_vec(), flags: *flags.add(i), id: *ids.add(i), stream: info.stream }
        })
        .collect();
    let _ = info.tx.send(batch);
}

/// A running stream; dropping it stops it.
pub struct Stream {
    stream: *mut c_void,
    /// Its serial callback queue.
    queue: *mut c_void,
}

// The stream is only started and stopped, never shared mid-call.
unsafe impl Send for Stream {}
unsafe impl Sync for Stream {}

impl Drop for Stream {
    fn drop(&mut self) {
        unsafe {
            let a = api();
            (a.stop)(self.stream);
            (a.invalidate)(self.stream);
            // The context goes when FSEvents frees the stream (`release_info`).
            (a.release)(self.stream);
            // Work still queued keeps the queue alive until it has run.
            dispatch_release(self.queue);
        }
    }
}

/// Watch `/` from `since` (an event id). Batches of directory-level events,
/// stamped with `stream`, arrive on `tx` until the returned stream is dropped.
pub fn watch(since: u64, latency: f64, tx: Sender<Vec<Event>>, stream: u32) -> Stream {
    let a = api();
    unsafe {
        let root = (a.string)(std::ptr::null(), c"/".as_ptr(), 0x0800_0100);
        let arr = (a.array)(std::ptr::null(), &root, 1, a.array_callbacks);
        // Freed by FSEvents through `release_info`.
        let info = Box::into_raw(Box::new(Info { tx, stream }));
        let release: extern "C" fn(*const c_void) = release_info;
        let ctx = Context {
            version: 0,
            info: info as *mut c_void,
            retain: std::ptr::null(),
            release: release as *const c_void,
            copy_description: std::ptr::null(),
        };
        // Not IgnoreSelf: linked into an app, the app's own renames and moves
        // are exactly what its search must see.
        let s = (a.create)(std::ptr::null(), on_events, &ctx, arr, since, latency, CREATE_FLAG_NO_DEFER);
        // The stream keeps its own copy of the paths.
        (a.cf_release)(arr);
        (a.cf_release)(root);
        // The callback only copies paths out, but it must not queue behind
        // busy threads (a first build runs on every core at user-initiated
        // QoS): fseventsd drops what a client falls behind on, and the drop
        // costs a relist of every folder changed since the last sync.
        let qos = libc::qos_class_t::QOS_CLASS_USER_INITIATED as u32;
        let q = dispatch_queue_create(c"fsearch.fsevents".as_ptr(), dispatch_queue_attr_make_with_qos_class(std::ptr::null(), qos, 0));
        (a.set_queue)(s, q);
        (a.start)(s);
        Stream { stream: s, queue: q }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Range;
    use std::time::{Duration, Instant};

    /// For the streams numbered in `r`: how many contexts the release
    /// callback freed and how many were freed at all, once all `r.len()` of
    /// each are in (or 20 s passed), and a little longer to see any freed
    /// twice. Other tests' engines start streams too, numbered below these.
    fn freed(r: Range<u32>) -> (usize, usize) {
        let count = |v: &std::sync::Mutex<Vec<u32>>| v.lock().unwrap().iter().filter(|s| r.contains(s)).count();
        let now = || (count(&RELEASED), count(&INFO_FREED));
        let n = r.len();
        let t = Instant::now();
        while now() != (n, n) && t.elapsed() < Duration::from_secs(20) {
            std::thread::sleep(Duration::from_millis(10));
        }
        std::thread::sleep(Duration::from_millis(300));
        now()
    }

    /// Each dropped stream's context is freed once, by FSEvents through the
    /// context's release callback.
    #[test]
    fn dropped_streams_free_their_context_through_the_release_callback() {
        let streams = 1_000_000..1_000_050;
        let (tx, _rx) = std::sync::mpsc::channel();
        for k in streams.clone() {
            drop(watch(current_id(), 0.1, tx.clone(), k));
        }
        assert_eq!(freed(streams), (50, 50), "(by the release callback, freed)");
    }

    /// Streams dropped while their callbacks are busy replaying history:
    /// each context is still freed exactly once, by the release callback.
    #[test]
    fn streams_dropped_mid_replay_free_their_context_once() {
        let streams = 2_000_000..2_000_030;
        let (tx, rx) = std::sync::mpsc::channel();
        let since = current_id().saturating_sub(20_000);
        let mut mid_replay = 0;
        for k in streams.clone() {
            let s = watch(since, 0.0, tx.clone(), k);
            // Drop it as soon as its first batch is in: more are on the way.
            mid_replay += rx.recv_timeout(Duration::from_secs(2)).is_ok() as usize;
            drop(s);
            rx.try_iter().for_each(drop);
        }
        assert_eq!(freed(streams), (30, 30), "(by the release callback, freed)");
        assert!(mid_replay >= 25, "only {mid_replay} of 30 streams were dropped mid-replay");
    }
}
