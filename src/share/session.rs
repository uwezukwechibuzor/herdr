use std::sync::{Mutex, OnceLock};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::broadcast;

static SHARE_TX: OnceLock<broadcast::Sender<Vec<u8>>> = OnceLock::new();

// Last complete frame — lets guests see the current screen on connect.
static LAST_FRAME: OnceLock<Mutex<Vec<u8>>> = OnceLock::new();

// Set when a guest connects to trigger a full repaint on the next render tick.
static REPAINT_REQUESTED: AtomicBool = AtomicBool::new(false);

pub fn init() -> &'static broadcast::Sender<Vec<u8>> {
    LAST_FRAME.get_or_init(|| Mutex::new(Vec::new()));
    SHARE_TX.get_or_init(|| {
        let (tx, _) = broadcast::channel(2048);
        tx
    })
}

pub fn subscribe() -> Option<broadcast::Receiver<Vec<u8>>> {
    SHARE_TX.get().map(|tx| tx.subscribe())
}

pub fn is_active() -> bool {
    SHARE_TX.get().is_some()
}

pub fn has_guests() -> bool {
    SHARE_TX.get().is_some_and(|tx| tx.receiver_count() > 0)
}

pub fn request_full_repaint() {
    if SHARE_TX.get().is_some() {
        REPAINT_REQUESTED.store(true, Ordering::Release);
    }
}

pub fn take_repaint_request() -> bool {
    REPAINT_REQUESTED.swap(false, Ordering::AcqRel)
}

pub fn last_frame() -> Option<Vec<u8>> {
    let frame = LAST_FRAME.get()?.lock().ok()?;
    if frame.is_empty() { None } else { Some(frame.clone()) }
}

pub fn broadcast_frame(bytes: &[u8]) {
    let Some(tx) = SHARE_TX.get() else { return };
    if let Some(last) = LAST_FRAME.get() {
        if let Ok(mut last) = last.lock() {
            last.clear();
            last.extend_from_slice(bytes);
        }
    }
    if tx.receiver_count() > 0 {
        let _ = tx.send(bytes.to_vec());
    }
}
