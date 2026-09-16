use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use crate::share::relay::start_relay_for_test;

/// Relay assigns a unique code per session.
#[tokio::test]
async fn relay_assigns_unique_codes() {
    // Run two separate relays and get two codes — they should differ.
    // (The relay generates codes with OS entropy so collisions are negligible.)
    // Here we just verify the protocol: SHARE → CODE <code>.
    let relay_addr = start_relay_for_test().await;

    let mut s1 = TcpStream::connect(&relay_addr).await.unwrap();
    s1.write_all(b"SHARE\n").await.unwrap();
    let mut r1 = BufReader::new(&mut s1);
    let mut line1 = String::new();
    r1.read_line(&mut line1).await.unwrap();
    let code1 = line1.trim().strip_prefix("CODE ").unwrap().to_string();

    let mut s2 = TcpStream::connect(&relay_addr).await.unwrap();
    s2.write_all(b"SHARE\n").await.unwrap();
    let mut r2 = BufReader::new(&mut s2);
    let mut line2 = String::new();
    r2.read_line(&mut line2).await.unwrap();
    let code2 = line2.trim().strip_prefix("CODE ").unwrap().to_string();

    assert!(!code1.is_empty(), "code1 should not be empty");
    assert!(!code2.is_empty(), "code2 should not be empty");
    assert_ne!(code1, code2, "codes should differ");
}

/// Guest gets NOTFOUND for an unknown code.
#[tokio::test]
async fn relay_notfound_for_unknown_code() {
    let relay_addr = start_relay_for_test().await;

    let mut stream = TcpStream::connect(&relay_addr).await.unwrap();
    stream.write_all(b"JOIN xxxxxx\n").await.unwrap();
    let mut reader = BufReader::new(&mut stream);
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    assert_eq!(line.trim(), "NOTFOUND");
}

/// Data flows from host through relay to guest.
#[tokio::test]
async fn data_flows_host_through_relay_to_guest() {
    let relay_addr = start_relay_for_test().await;

    // Host: open SHARE session, get code.
    let mut host = TcpStream::connect(&relay_addr).await.unwrap();
    host.write_all(b"SHARE\n").await.unwrap();
    let (host_r, mut host_w) = host.into_split();
    let mut host_reader = BufReader::new(host_r);
    let mut line = String::new();
    host_reader.read_line(&mut line).await.unwrap();
    let code = line.trim().strip_prefix("CODE ").unwrap().to_string();

    // Guest: join with that code.
    let mut guest = TcpStream::connect(&relay_addr).await.unwrap();
    guest
        .write_all(format!("JOIN {code}\n").as_bytes())
        .await
        .unwrap();
    let (guest_r, _guest_w) = guest.into_split();
    let mut guest_reader = BufReader::new(guest_r);
    let mut ok_line = String::new();
    guest_reader.read_line(&mut ok_line).await.unwrap();
    assert_eq!(ok_line.trim(), "OK");

    // Host sends payload.
    let payload = b"hello from host";
    host_w.write_all(payload).await.unwrap();

    // Guest receives it.
    let mut received = vec![0u8; payload.len()];
    tokio::time::timeout(
        Duration::from_secs(5),
        guest_reader.read_exact(&mut received),
    )
    .await
    .expect("receive timed out")
    .expect("read error");

    assert_eq!(received, payload);
}
