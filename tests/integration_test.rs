use std::time::Duration;

use bytes::Buf;
use mcrouteur::connection::{Connection, ServerError};
use tokio::{
  io::{AsyncBufReadExt, AsyncWriteExt, BufStream},
  net::TcpStream,
  process::Command,
};

#[tokio::test]
async fn test_binary_protocol() {
  let mut conn = Connection::connect("tcp://localhost:11211".parse().unwrap())
    .await
    .unwrap();
  test_binary_commands(&mut conn).await;
  conn.close().await.unwrap();
}

#[tokio::test]
async fn test_text_protocol() {
  let mut stream = TcpStream::connect("[::]:11211").await.map(BufStream::new).unwrap();
  test_text_commands(&mut stream).await;
  stream.shutdown().await.unwrap();
}

#[tokio::test]
// #[ignore]
async fn test_mcrouteur() {
  let _command = Command::new(env!("CARGO_BIN_EXE_mcrouteur"))
    .arg("-b tcp://localhost:11213")
    .arg("-u primary=tcp://localhost:11211")
    .arg("-u secondary=tcp://localhost:11212")
    .kill_on_drop(true)
    .spawn()
    .unwrap();

  tokio::time::sleep(Duration::from_millis(1000)).await;

  let mut conn = Connection::connect("tcp://localhost:11213".parse().unwrap())
    .await
    .unwrap();
  test_binary_commands(&mut conn).await;
  conn.close().await.unwrap();

  let mut conn = TcpStream::connect("[::]:11213").await.map(BufStream::new).unwrap();
  test_text_commands(&mut conn).await;
  conn.shutdown().await.unwrap();
}

async fn test_binary_commands(conn: &mut Connection) {
  conn.noop().await.unwrap();
  conn.flush().await.unwrap();

  assert_eq!("1.6.19", conn.version().await.unwrap().as_str());

  conn.set("foo", b"bar", 0, None).await.unwrap();
  assert_eq!(b"bar", conn.get("foo").await.unwrap().chunk());

  conn.set("bar", b"baz", 0, None).await.unwrap();
  assert_eq!(b"baz", conn.get("bar").await.unwrap().chunk());

  conn.set("toto", b"tata", 0, None).await.unwrap();
  assert_eq!(b"tata", conn.get("toto").await.unwrap().chunk());

  conn.set("empty", b"", 0, None).await.unwrap();
  assert_eq!(b"", conn.get("empty").await.unwrap().chunk());

  assert_eq!(
    Some(ServerError::KeyNotFound),
    conn.get("missing").await.unwrap_err().as_server_error()
  );

  assert_eq!(
    Some(ServerError::KeyExists),
    conn.add("foo", b"baz", 0, None).await.unwrap_err().as_server_error()
  );

  conn.add("zoo", "pet", 0, None).await.unwrap();
  assert_eq!(b"pet", conn.get("zoo").await.unwrap().chunk());

  assert_eq!(
    Some(ServerError::KeyNotFound),
    conn
      .replace("missing", b"baz", 0, None)
      .await
      .unwrap_err()
      .as_server_error()
  );

  conn.replace("foo", b"baz", 0, None).await.unwrap();
  assert_eq!(b"baz", conn.get("foo").await.unwrap().chunk());

  conn.delete("toto").await.unwrap();
  assert_eq!(
    Some(ServerError::KeyNotFound),
    conn.get("toto").await.unwrap_err().as_server_error()
  );

  conn.touch("foo", 1).await.unwrap();
  assert_eq!(b"baz", conn.gat("bar", 1).await.unwrap().chunk());

  assert!(!conn.stats().await.unwrap().is_empty());
}

async fn test_text_commands(s: &mut BufStream<TcpStream>) {
  s.write_all(b"flush_all\r\n").await.unwrap();
  s.flush().await.unwrap();
  let mut buffer = String::new();
  s.read_line(&mut buffer).await.unwrap();
  assert_eq!("OK\r\n", buffer);

  s.write_all(b"version\r\n").await.unwrap();
  s.flush().await.unwrap();
  let mut buffer = String::new();
  s.read_line(&mut buffer).await.unwrap();
  assert_eq!("VERSION 1.6.19\r\n", buffer);

  s.write_all(b"set foo 0 0 3\r\nbar\r\n").await.unwrap();
  s.flush().await.unwrap();
  let mut buffer = String::new();
  s.read_line(&mut buffer).await.unwrap();
  assert_eq!("STORED\r\n", buffer);

  s.write_all(b"get foo\r\n").await.unwrap();
  s.flush().await.unwrap();
  let mut buffer = String::new();
  s.read_line(&mut buffer).await.unwrap();
  s.read_line(&mut buffer).await.unwrap();
  s.read_line(&mut buffer).await.unwrap();
  assert_eq!("VALUE foo 0 3\r\nbar\r\nEND\r\n", buffer);

  s.write_all(b"set toto 0 0 4\r\ntata\r\n").await.unwrap();
  s.flush().await.unwrap();
  let mut buffer = String::new();
  s.read_line(&mut buffer).await.unwrap();
  assert_eq!("STORED\r\n", buffer);

  s.write_all(b"get toto\r\n").await.unwrap();
  s.flush().await.unwrap();
  let mut buffer = String::new();
  s.read_line(&mut buffer).await.unwrap();
  s.read_line(&mut buffer).await.unwrap();
  s.read_line(&mut buffer).await.unwrap();
  assert_eq!("VALUE toto 0 4\r\ntata\r\nEND\r\n", buffer);

  s.write_all(b"set bar 0 0 3\r\nbaz\r\n").await.unwrap();
  s.flush().await.unwrap();
  let mut buffer = String::new();
  s.read_line(&mut buffer).await.unwrap();
  assert_eq!("STORED\r\n", buffer);

  s.write_all(b"get bar\r\n").await.unwrap();
  s.flush().await.unwrap();
  let mut buffer = String::new();
  s.read_line(&mut buffer).await.unwrap();
  s.read_line(&mut buffer).await.unwrap();
  s.read_line(&mut buffer).await.unwrap();
  assert_eq!("VALUE bar 0 3\r\nbaz\r\nEND\r\n", buffer);

  s.write_all(b"set empty 0 0 0\r\n\r\n").await.unwrap();
  s.flush().await.unwrap();
  let mut buffer = String::new();
  s.read_line(&mut buffer).await.unwrap();
  assert_eq!("STORED\r\n", buffer);

  s.write_all(b"get empty\r\n").await.unwrap();
  s.flush().await.unwrap();
  let mut buffer = String::new();
  s.read_line(&mut buffer).await.unwrap();
  s.read_line(&mut buffer).await.unwrap();
  s.read_line(&mut buffer).await.unwrap();
  assert_eq!("VALUE empty 0 0\r\n\r\nEND\r\n", buffer);

  s.write_all(b"get foo bar\r\n").await.unwrap();
  s.flush().await.unwrap();
  let mut buffer = String::new();
  s.read_line(&mut buffer).await.unwrap();
  s.read_line(&mut buffer).await.unwrap();
  s.read_line(&mut buffer).await.unwrap();
  s.read_line(&mut buffer).await.unwrap();
  s.read_line(&mut buffer).await.unwrap();
  assert_eq!("VALUE foo 0 3\r\nbar\r\nVALUE bar 0 3\r\nbaz\r\nEND\r\n", buffer);

  s.write_all(b"get missing\r\n").await.unwrap();
  s.flush().await.unwrap();
  let mut buffer = String::new();
  s.read_line(&mut buffer).await.unwrap();
  assert_eq!("END\r\n", buffer);

  s.write_all(b"add foo 0 0 3\r\nbaz\r\n").await.unwrap();
  s.flush().await.unwrap();
  let mut buffer = String::new();
  s.read_line(&mut buffer).await.unwrap();
  assert_eq!("NOT_STORED\r\n", buffer);

  s.write_all(b"add zoo 0 0 3\r\npet\r\n").await.unwrap();
  s.flush().await.unwrap();
  let mut buffer = String::new();
  s.read_line(&mut buffer).await.unwrap();
  assert_eq!("STORED\r\n", buffer);

  s.write_all(b"get zoo\r\n").await.unwrap();
  s.flush().await.unwrap();
  let mut buffer = String::new();
  s.read_line(&mut buffer).await.unwrap();
  s.read_line(&mut buffer).await.unwrap();
  s.read_line(&mut buffer).await.unwrap();
  assert_eq!("VALUE zoo 0 3\r\npet\r\nEND\r\n", buffer);

  s.write_all(b"replace missing 0 0 3\r\nbaz\r\n").await.unwrap();
  s.flush().await.unwrap();
  let mut buffer = String::new();
  s.read_line(&mut buffer).await.unwrap();
  assert_eq!("NOT_STORED\r\n", buffer);

  s.write_all(b"replace foo 0 0 3\r\nbaz\r\n").await.unwrap();
  s.flush().await.unwrap();
  let mut buffer = String::new();
  s.read_line(&mut buffer).await.unwrap();
  assert_eq!("STORED\r\n", buffer);

  s.write_all(b"get foo\r\n").await.unwrap();
  s.flush().await.unwrap();
  let mut buffer = String::new();
  s.read_line(&mut buffer).await.unwrap();
  s.read_line(&mut buffer).await.unwrap();
  s.read_line(&mut buffer).await.unwrap();
  assert_eq!("VALUE foo 0 3\r\nbaz\r\nEND\r\n", buffer);
}
