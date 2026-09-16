use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tracing::{error, info};
use crate::share::session::{init, last_frame, request_full_repaint, subscribe};

pub const DEFAULT_RELAY: &str = "relay.herdr.dev:4002";

const BUF_SIZE: usize = 8_192;

/// Resolve the relay address: flag wins, then env var, then default.
pub fn resolve_relay(flag: Option<&str>) -> String {
    if let Some(addr) = flag {
        return addr.to_string();
    }
    std::env::var("HERDR_RELAY_ADDR").unwrap_or_else(|_| DEFAULT_RELAY.to_string())
}

// ---------------------------------------------------------------------------
// Background share mode  (`herdr --share`)
// ---------------------------------------------------------------------------

pub fn start_background_share(relay: Option<String>) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    init();

    let relay = resolve_relay(relay.as_deref());
    let (addr_tx, addr_rx) = std::sync::mpsc::channel::<Result<String, String>>();

    std::thread::spawn(move || {
        let rt = match tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                let _ = addr_tx.send(Err(e.to_string()));
                return;
            }
        };
        rt.block_on(async move {
            if let Err(e) = run_background_share(addr_tx, &relay).await {
                error!("background share error: {e}");
            }
        });
    });

    let join_cmd = addr_rx
        .recv_timeout(Duration::from_secs(15))
        .map_err(|_| "timed out connecting to relay")?
        .map_err(|e| Box::<dyn std::error::Error + Send + Sync>::from(e))?;

    save_share_addr(&join_cmd);
    Ok(join_cmd)
}

async fn run_background_share(
    addr_tx: std::sync::mpsc::Sender<Result<String, String>>,
    relay: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let stream = TcpStream::connect(relay).await
        .map_err(|e| format!("cannot reach relay {relay}: {e}"))?;

    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    write_half.write_all(b"SHARE\n").await?;

    let mut line = String::new();
    reader.read_line(&mut line).await?;
    let code = line
        .trim()
        .strip_prefix("CODE ")
        .ok_or("relay did not send a CODE")?
        .to_string();

    let _ = addr_tx.send(Ok(format!("herdr join {code}")));

    let mut rx = subscribe().ok_or("session not initialised")?;

    // Send the current screen as the first recorded frame so guests joining
    // before the next render tick don't see a blank terminal.
    if let Some(frame) = last_frame() {
        write_half.write_all(&frame).await?;
    }

    loop {
        match rx.recv().await {
            Ok(bytes) => {
                if write_half.write_all(&bytes).await.is_err() {
                    break;
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                request_full_repaint();
                #[cfg(unix)]
                unsafe { libc::raise(libc::SIGWINCH); }
            }
            Err(_) => break,
        }
    }

    clear_share_addr();
    Ok(())
}

// ---------------------------------------------------------------------------
// Host entry point  (`herdr share`)
// ---------------------------------------------------------------------------

pub async fn run_host(relay: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("  Connecting to relay ({relay})...");

    let stream = TcpStream::connect(&relay).await
        .map_err(|e| format!("cannot reach relay at {relay}: {e}\nhint: run 'herdr relay' locally and set HERDR_RELAY_ADDR=localhost:4002"))?;

    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    write_half.write_all(b"SHARE\n").await?;

    let mut line = String::new();
    reader.read_line(&mut line).await?;
    let code = line
        .trim()
        .strip_prefix("CODE ")
        .ok_or("unexpected relay response")?
        .to_string();

    let join_cmd = format!("herdr join {code}");
    save_share_addr(&join_cmd);
    copy_to_clipboard(&join_cmd);

    println!("\n  Session ready. Give this to your collaborator:\n");
    println!("  {join_cmd}\n");
    println!("  (copied to clipboard)\n");
    println!("  Starting herdr in 4 seconds...\n");
    for i in (1..=4u64).rev() {
        tokio::time::sleep(Duration::from_secs(1)).await;
        print!("  {i}...\r");
        use std::io::Write as _;
        std::io::stdout().flush().ok();
    }
    println!();

    let (bcast_tx, mut bcast_rx) = tokio::sync::broadcast::channel::<Vec<u8>>(512);

    tokio::spawn(async move {
        loop {
            match bcast_rx.recv().await {
                Ok(bytes) => {
                    if write_half.write_all(&bytes).await.is_err() {
                        break;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Err(_) => break,
            }
        }
    });

    let result = run_host_pty(bcast_tx).await;
    clear_share_addr();
    result
}

// ---------------------------------------------------------------------------
// Guest entry point  (`herdr join <code>`)
// ---------------------------------------------------------------------------

enum ScrollCmd {
    Up,
    Down,
    Live,
    Quit,
}

pub async fn run_guest(code: &str, relay: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("  Connecting to relay ({relay})...");

    let stream = TcpStream::connect(&relay).await
        .map_err(|e| format!("cannot reach relay at {relay}: {e}"))?;

    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    write_half.write_all(format!("JOIN {code}\n").as_bytes()).await?;

    let mut line = String::new();
    reader.read_line(&mut line).await?;
    match line.trim() {
        "OK" => {}
        "NOTFOUND" => return Err(format!("session '{code}' not found — is the host still sharing?").into()),
        other => return Err(format!("unexpected relay response: {other}").into()),
    }

    println!("  Connected!  PageUp/PageDown to scroll, 'q' to quit.\r");

    crossterm::terminal::enable_raw_mode()?;
    let _raw = RawModeGuard;
    crossterm::execute!(std::io::stdout(), crossterm::event::EnableMouseCapture)?;
    let _mouse = MouseCaptureGuard;

    let (key_tx, mut key_rx) = tokio::sync::mpsc::channel::<ScrollCmd>(32);
    tokio::task::spawn_blocking(move || {
        use crossterm::event::{Event, KeyCode, KeyModifiers, MouseEventKind};
        loop {
            let cmd = match crossterm::event::read() {
                Ok(Event::Key(e)) => match (e.code, e.modifiers) {
                    (KeyCode::PageUp, _) | (KeyCode::Up, KeyModifiers::ALT) => ScrollCmd::Up,
                    (KeyCode::PageDown, _) | (KeyCode::Down, KeyModifiers::ALT) => ScrollCmd::Down,
                    (KeyCode::Esc, _) => ScrollCmd::Live,
                    (KeyCode::Char('q'), KeyModifiers::NONE) => ScrollCmd::Quit,
                    _ => continue,
                },
                Ok(Event::Mouse(e)) => match e.kind {
                    MouseEventKind::ScrollUp => ScrollCmd::Up,
                    MouseEventKind::ScrollDown => ScrollCmd::Down,
                    _ => continue,
                },
                Err(_) => break,
                _ => continue,
            };
            let quit = matches!(cmd, ScrollCmd::Quit);
            if key_tx.blocking_send(cmd).is_err() || quit {
                break;
            }
        }
    });

    run_guest_display(&mut reader, &mut key_rx).await?;

    info!("guest session ended");
    Ok(())
}

// ---------------------------------------------------------------------------
// PTY host loop
// ---------------------------------------------------------------------------

struct RawModeGuard;
impl Drop for RawModeGuard {
    fn drop(&mut self) {
        let _ = crossterm::terminal::disable_raw_mode();
    }
}

struct MouseCaptureGuard;
impl Drop for MouseCaptureGuard {
    fn drop(&mut self) {
        let _ = crossterm::execute!(std::io::stdout(), crossterm::event::DisableMouseCapture);
    }
}

async fn run_host_pty(
    bcast_tx: tokio::sync::broadcast::Sender<Vec<u8>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use portable_pty::{CommandBuilder, PtySize, native_pty_system};
    use std::io::{Read as _, Write as _};

    let (cols, rows) = crossterm::terminal::size().unwrap_or((80, 24));
    let pty_system = native_pty_system();
    let pair = pty_system.openpty(PtySize { rows, cols, pixel_width: 0, pixel_height: 0 })?;

    let herdr_bin = std::env::current_exe()
        .unwrap_or_else(|_| std::path::PathBuf::from("herdr"));
    let mut cmd = CommandBuilder::new(&herdr_bin);
    cmd.env("TERM", "xterm-256color");
    cmd.env_remove("HERDR_SOCKET_PATH");
    cmd.env_remove("HERDR_CLIENT_SOCKET_PATH");
    let _child = pair.slave.spawn_command(cmd)?;
    drop(pair.slave);

    let mut pty_out = pair.master.try_clone_reader()?;
    let mut pty_in = pair.master.take_writer()?;

    crossterm::terminal::enable_raw_mode()?;
    let _raw = RawModeGuard;

    tokio::task::spawn_blocking(move || {
        let mut stdin = std::io::stdin();
        let mut buf = vec![0u8; 256];
        loop {
            match stdin.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => { if pty_in.write_all(&buf[..n]).is_err() { break; } }
            }
        }
    });

    let (pty_tx, mut pty_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(64);
    tokio::task::spawn_blocking(move || {
        let mut buf = vec![0u8; BUF_SIZE];
        loop {
            match pty_out.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => { if pty_tx.blocking_send(buf[..n].to_vec()).is_err() { break; } }
            }
        }
    });

    let mut stdout = std::io::stdout();
    while let Some(bytes) = pty_rx.recv().await {
        stdout.write_all(&bytes)?;
        stdout.flush()?;
        let _ = bcast_tx.send(bytes);
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Guest display loop
// ---------------------------------------------------------------------------

async fn run_guest_display(
    reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
    key_rx: &mut tokio::sync::mpsc::Receiver<ScrollCmd>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use std::io::Write as _;

    const HISTORY_CAP: usize = 100_000;
    let mut history: std::collections::VecDeque<Vec<u8>> =
        std::collections::VecDeque::with_capacity(HISTORY_CAP + 1);
    let mut scroll_offset: usize = 0;
    let mut stdout = std::io::stdout();
    let mut buf = vec![0u8; BUF_SIZE];

    loop {
        tokio::select! {
            result = reader.read(&mut buf) => {
                let n = match result {
                    Ok(0) | Err(_) => break,
                    Ok(n) => n,
                };
                let chunk = buf[..n].to_vec();
                if history.len() >= HISTORY_CAP {
                    history.pop_front();
                    scroll_offset = scroll_offset.saturating_sub(1);
                }
                history.push_back(chunk.clone());
                if scroll_offset == 0 {
                    let _ = stdout.write_all(&chunk);
                    let _ = stdout.flush();
                }
            }
            cmd = key_rx.recv() => {
                let Some(cmd) = cmd else { break; };
                match cmd {
                    ScrollCmd::Quit => break,
                    ScrollCmd::Live => {
                        scroll_offset = 0;
                        if let Some(f) = history.back() {
                            let _ = stdout.write_all(f);
                            let _ = stdout.flush();
                        }
                    }
                    ScrollCmd::Up => {
                        let max = history.len().saturating_sub(1);
                        scroll_offset = (scroll_offset + 1).min(max);
                        let idx = history.len().saturating_sub(1 + scroll_offset);
                        if let Some(f) = history.get(idx) {
                            let _ = stdout.write_all(f);
                            let _ = stdout.flush();
                        }
                    }
                    ScrollCmd::Down => {
                        scroll_offset = scroll_offset.saturating_sub(1);
                        let idx = history.len().saturating_sub(1 + scroll_offset);
                        if let Some(f) = history.get(idx) {
                            let _ = stdout.write_all(f);
                            let _ = stdout.flush();
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

pub fn share_addr_file() -> std::path::PathBuf {
    std::env::temp_dir().join("herdr-share.addr")
}

pub fn save_share_addr(join_cmd: &str) {
    let pid = std::process::id();
    let mut entries = read_active_entries();
    entries.retain(|(p, _)| *p != pid);
    entries.push((pid, join_cmd.to_string()));
    write_entries(&entries);
}

pub fn clear_share_addr() {
    let pid = std::process::id();
    let entries: Vec<_> = read_active_entries()
        .into_iter()
        .filter(|(p, _)| *p != pid)
        .collect();
    if entries.is_empty() {
        let _ = std::fs::remove_file(share_addr_file());
    } else {
        write_entries(&entries);
    }
}

pub fn load_share_addrs() -> Vec<String> {
    read_active_entries().into_iter().map(|(_, cmd)| cmd).collect()
}

pub fn load_share_pids() -> Vec<u32> {
    read_active_entries().into_iter().map(|(pid, _)| pid).collect()
}

fn read_active_entries() -> Vec<(u32, String)> {
    let content = match std::fs::read_to_string(share_addr_file()) {
        Ok(c) => c,
        Err(_) => return Vec::new(),
    };
    content
        .lines()
        .filter_map(|line| {
            let (pid_str, cmd) = line.trim().split_once(' ')?;
            let pid: u32 = pid_str.parse().ok()?;
            if !process_is_alive(pid) { return None; }
            Some((pid, cmd.to_string()))
        })
        .collect()
}

fn write_entries(entries: &[(u32, String)]) {
    let content = entries
        .iter()
        .map(|(pid, cmd)| format!("{pid} {cmd}"))
        .collect::<Vec<_>>()
        .join("\n");
    let _ = std::fs::write(share_addr_file(), content);
}

fn process_is_alive(pid: u32) -> bool {
    #[cfg(unix)]
    { unsafe { libc::kill(pid as libc::pid_t, 0) == 0 } }
    #[cfg(windows)]
    {
        let handle = unsafe {
            windows_sys::Win32::System::Threading::OpenProcess(
                windows_sys::Win32::System::Threading::PROCESS_QUERY_LIMITED_INFORMATION,
                0, pid,
            )
        };
        if handle == 0 { return false; }
        let mut code: u32 = 0;
        let alive = unsafe {
            windows_sys::Win32::System::Threading::GetExitCodeProcess(handle, &mut code) != 0
                && code == 259
        };
        unsafe { windows_sys::Win32::Foundation::CloseHandle(handle) };
        alive
    }
    #[cfg(not(any(unix, windows)))]
    { true }
}

pub fn copy_to_clipboard(text: &str) {
    #[cfg(target_os = "macos")]
    {
        use std::io::Write as _;
        if let Ok(mut child) = std::process::Command::new("pbcopy")
            .stdin(std::process::Stdio::piped())
            .spawn()
        {
            if let Some(stdin) = child.stdin.as_mut() {
                let _ = stdin.write_all(text.as_bytes());
            }
            let _ = child.wait();
        }
    }
    #[cfg(target_os = "linux")]
    {
        use std::io::Write as _;
        for prog in &["wl-copy", "xclip -selection clipboard"] {
            let parts: Vec<&str> = prog.split_whitespace().collect();
            if let Ok(mut child) = std::process::Command::new(parts[0])
                .args(&parts[1..])
                .stdin(std::process::Stdio::piped())
                .spawn()
            {
                if let Some(stdin) = child.stdin.as_mut() {
                    let _ = stdin.write_all(text.as_bytes());
                }
                if child.wait().map(|s| s.success()).unwrap_or(false) { return; }
            }
        }
    }
    #[cfg(target_os = "windows")]
    {
        use std::io::Write as _;
        if let Ok(mut child) = std::process::Command::new("clip")
            .stdin(std::process::Stdio::piped())
            .spawn()
        {
            if let Some(stdin) = child.stdin.as_mut() {
                let _ = stdin.write_all(text.as_bytes());
            }
            let _ = child.wait();
        }
    }
}
