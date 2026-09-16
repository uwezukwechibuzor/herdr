use crate::platform::begin_cli_output;
use crate::share::network::{
    copy_to_clipboard, load_share_addrs, load_share_pids, resolve_relay, run_guest, run_host,
    share_addr_file,
};
use crate::share::relay::{run_relay, DEFAULT_PORT};

/// Dispatch `herdr share [--relay <addr>]`.
pub(super) fn run_share_command(args: &[String]) -> std::io::Result<i32> {
    let (relay_flag, rest) = extract_relay_flag(args);

    match rest.first().map(String::as_str) {
        Some("help" | "--help" | "-h") => {
            print_share_help();
            return Ok(0);
        }
        Some("addr") => {
            begin_cli_output();
            let addrs = load_share_addrs();
            if addrs.is_empty() {
                eprintln!("no active share sessions");
                eprintln!("hint: start one with  herdr --share  or  herdr share");
                return Ok(1);
            }
            for addr in &addrs {
                println!("{addr}");
            }
            if let Some(last) = addrs.last() {
                copy_to_clipboard(last);
                if addrs.len() == 1 {
                    println!("(copied to clipboard)");
                } else {
                    println!("({} active sessions — last copied to clipboard)", addrs.len());
                }
            }
            return Ok(0);
        }
        Some("stop") => {
            begin_cli_output();
            return run_share_stop();
        }
        Some(arg) if arg.starts_with('-') => {
            eprintln!("unknown option: {arg}");
            eprintln!("usage: herdr share [--relay <addr>]");
            return Ok(2);
        }
        _ => {}
    }

    let relay = resolve_relay(relay_flag.as_deref());

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    rt.block_on(run_host(&relay))
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

    Ok(0)
}

/// Dispatch `herdr join [--relay <addr>] <code>`.
pub(super) fn run_join_command(args: &[String]) -> std::io::Result<i32> {
    let (relay_flag, rest) = extract_relay_flag(args);

    let Some(code) = rest.first() else {
        eprintln!("usage: herdr join [--relay <addr>] <code>");
        return Ok(2);
    };

    if matches!(code.as_str(), "help" | "--help" | "-h") {
        print_join_help();
        return Ok(0);
    }

    let code = code.clone();
    let relay = resolve_relay(relay_flag.as_deref());

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    rt.block_on(run_guest(&code, &relay))
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

    Ok(0)
}

/// Dispatch `herdr relay [port]`.
///
/// Starts the herdr relay server so hosts and guests on different networks
/// can find each other.  Anyone can run their own relay; point clients at it
/// with the `HERDR_RELAY_ADDR=host:port` environment variable.
pub(super) fn run_relay_command(args: &[String]) -> std::io::Result<i32> {
    if matches!(args.first().map(String::as_str), Some("help" | "--help" | "-h")) {
        print_relay_help();
        return Ok(0);
    }

    let port: u16 = match args.first() {
        Some(p) => p.parse().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "port must be a number")
        })?,
        None => DEFAULT_PORT,
    };

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    rt.block_on(run_relay(port))
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

    Ok(0)
}

/// Stop all active share sessions by signalling their processes.
fn run_share_stop() -> std::io::Result<i32> {
    let pids = load_share_pids();
    if pids.is_empty() {
        eprintln!("no active share sessions found");
        return Ok(1);
    }

    let mut stopped = 0usize;

    for pid in &pids {
        let pid = *pid;

        #[cfg(unix)]
        {
            let ret = unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
            if ret == 0 {
                println!("share session (pid {pid}) stopped");
                stopped += 1;
            } else {
                eprintln!("could not signal process {pid} (already exited?)");
            }
        }

        #[cfg(windows)]
        {
            let handle = unsafe {
                windows_sys::Win32::System::Threading::OpenProcess(
                    windows_sys::Win32::System::Threading::PROCESS_TERMINATE,
                    0,
                    pid,
                )
            };
            if handle != 0 {
                unsafe { windows_sys::Win32::System::Threading::TerminateProcess(handle, 1) };
                unsafe { windows_sys::Win32::Foundation::CloseHandle(handle) };
                println!("share session (pid {pid}) stopped");
                stopped += 1;
            } else {
                eprintln!("could not open process {pid} (already exited?)");
            }
        }

        #[cfg(not(any(unix, windows)))]
        {
            eprintln!("herdr share stop is not supported on this platform");
            return Ok(1);
        }
    }

    // Clean up stale file entries.
    let _ = std::fs::remove_file(share_addr_file());

    if stopped == 0 { Ok(1) } else { Ok(0) }
}

/// Extract `--relay <addr>` from args, returning `(relay, remaining_args)`.
fn extract_relay_flag(args: &[String]) -> (Option<String>, Vec<String>) {
    let mut relay: Option<String> = None;
    let mut rest = Vec::new();
    let mut i = 0;
    while i < args.len() {
        if args[i] == "--relay" {
            if let Some(val) = args.get(i + 1) {
                relay = Some(val.clone());
                i += 2;
                continue;
            }
        }
        rest.push(args[i].clone());
        i += 1;
    }
    (relay, rest)
}

fn print_share_help() {
    begin_cli_output();
    println!("herdr share — start a collaborative terminal session");
    println!();
    println!("Usage: herdr share");
    println!("       herdr share addr");
    println!("       herdr share stop");
    println!();
    println!("Subcommands:");
    println!("  addr   Print the active join code and copy it to clipboard");
    println!("  stop   Terminate the active sharing session");
    println!();
    println!("Connects to the herdr relay and prints a short code your collaborator");
    println!("can use with 'herdr join <code>' from any network, anywhere.");
    println!();
    println!("Override the relay:  HERDR_RELAY_ADDR=host:port herdr share");
    println!("Run your own relay:  herdr relay");
}

fn print_join_help() {
    begin_cli_output();
    println!("herdr join — join a collaborative terminal session");
    println!();
    println!("Usage: herdr join <code>");
    println!();
    println!("Arguments:");
    println!("  <code>   The short code printed by 'herdr share'");
    println!();
    println!("Connects through the relay and shows a live read-only view");
    println!("of the host's herdr session.  Works across any network.");
    println!();
    println!("Override the relay:  HERDR_RELAY_ADDR=host:port herdr join <code>");
}

fn print_relay_help() {
    begin_cli_output();
    println!("herdr relay — run a session relay server");
    println!();
    println!("Usage: herdr relay [port]");
    println!();
    println!("Arguments:");
    println!("  [port]   Port to listen on (default: 4002)");
    println!();
    println!("Forwards session bytes between hosts and guests.");
    println!("Anyone can run their own relay — no herdr account needed.");
    println!();
    println!("Point clients at your relay:");
    println!("  HERDR_RELAY_ADDR=<your-host>:4002 herdr share");
    println!("  HERDR_RELAY_ADDR=<your-host>:4002 herdr join <code>");
}
