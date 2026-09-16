use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, Mutex};
use tracing::{debug, info, warn};

pub const DEFAULT_PORT: u16 = 4002;

pub(crate) struct Session {
    pub(crate) tx: broadcast::Sender<Vec<u8>>,
    pub(crate) record_path: PathBuf,
    // Snapshotted under the lock at guest join time to avoid gaps between
    // history replay and live stream.
    pub(crate) bytes_recorded: u64,
}

pub(crate) type Sessions = Arc<Mutex<HashMap<String, Session>>>;

pub async fn run_relay(port: u16) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = format!("0.0.0.0:{port}");
    let listener = TcpListener::bind(&addr).await?;
    let sessions: Sessions = Arc::new(Mutex::new(HashMap::new()));

    println!("herdr relay listening on {addr}");
    println!("set HERDR_RELAY_ADDR={addr} on clients to use this relay");

    loop {
        let (stream, peer) = listener.accept().await?;
        debug!("connection from {peer}");
        let sessions = sessions.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, sessions).await {
                debug!("relay connection error: {e}");
            }
        });
    }
}

async fn handle_connection(
    stream: TcpStream,
    sessions: Sessions,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    let cmd = line.trim().to_string();

    if cmd == "SHARE" {
        let code = generate_code();
        let (tx, _) = broadcast::channel::<Vec<u8>>(1024);
        let record_path = std::env::temp_dir().join(format!("herdr-session-{code}.rec"));
        let mut record_file = tokio::fs::OpenOptions::new()
            .create(true).write(true).truncate(true)
            .open(&record_path).await?;

        sessions.lock().await.insert(code.clone(), Session {
            tx: tx.clone(),
            record_path: record_path.clone(),
            bytes_recorded: 0,
        });

        write_half.write_all(format!("CODE {code}\n").as_bytes()).await?;
        info!("session started: {code}");

        let mut buf = vec![0u8; 8192];
        loop {
            let n = reader.read(&mut buf).await?;
            if n == 0 { break; }
            let chunk = buf[..n].to_vec();

            match record_file.write_all(&chunk).await {
                Ok(_) => {
                    let mut locked = sessions.lock().await;
                    if let Some(sess) = locked.get_mut(&code) {
                        sess.bytes_recorded += chunk.len() as u64;
                        let _ = sess.tx.send(chunk);
                    }
                }
                Err(e) => {
                    warn!("session {code}: recording write failed ({e})");
                    let locked = sessions.lock().await;
                    if let Some(sess) = locked.get(&code) {
                        let _ = sess.tx.send(chunk);
                    }
                }
            }
        }

        sessions.lock().await.remove(&code);
        let _ = tokio::fs::remove_file(&record_path).await;
        info!("session ended: {code}");
    } else if let Some(code) = cmd.strip_prefix("JOIN ") {
        let code = code.trim();
        let mut locked = sessions.lock().await;
        let Some(sess) = locked.get_mut(code) else {
            let _ = write_half.write_all(b"NOTFOUND\n").await;
            return Ok(());
        };

        let mut rx = sess.tx.subscribe();
        let record_path = sess.record_path.clone();
        let replay_until = sess.bytes_recorded;
        drop(locked);

        write_half.write_all(b"OK\n").await?;
        info!("guest joined: {code}");

        if replay_until > 0 {
            match File::open(&record_path).await {
                Ok(mut file) => {
                    let mut remaining = replay_until;
                    let mut buf = vec![0u8; 65_536];
                    while remaining > 0 {
                        let to_read = (remaining as usize).min(buf.len());
                        let n = file.read(&mut buf[..to_read]).await?;
                        if n == 0 { break; }
                        write_half.write_all(&buf[..n]).await?;
                        remaining -= n as u64;
                    }
                }
                Err(e) => warn!("session {code}: could not open recording ({e})"),
            }
        }

        loop {
            match rx.recv().await {
                Ok(data) => { if write_half.write_all(&data).await.is_err() { break; } }
                Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
        info!("guest left: {code}");
    } else {
        let _ = write_half.write_all(b"ERROR unknown command\n").await;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

#[cfg(test)]
pub async fn start_relay_for_test() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let sessions: Sessions = Arc::new(Mutex::new(HashMap::new()));
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let sessions = sessions.clone();
            tokio::spawn(async move { let _ = handle_connection(stream, sessions).await; });
        }
    });
    addr
}

#[cfg(test)]
pub async fn _handle_connection_for_test(
    stream: tokio::net::TcpStream,
    sessions: Sessions,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    handle_connection(stream, sessions).await
}

fn generate_code() -> String {
    use sha2::{Digest, Sha256};
    use std::time::{SystemTime, UNIX_EPOCH};

    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();

    let mut extra = [0u8; 8];
    #[cfg(unix)]
    {
        use std::io::Read as _;
        if let Ok(mut f) = std::fs::File::open("/dev/urandom") {
            let _ = f.read_exact(&mut extra);
        }
    }

    let mut hasher = Sha256::new();
    hasher.update(ts.to_le_bytes());
    hasher.update(std::process::id().to_le_bytes());
    hasher.update(extra);
    let hash = hasher.finalize();

    const CHARS: &[u8] = b"abcdefghjkmnpqrstuvwxyz23456789";
    hash[..6].iter().map(|&b| CHARS[(b as usize) % CHARS.len()] as char).collect()
}
