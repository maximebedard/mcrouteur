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
  let mut conn = Connection::connect(&("tcp://[::]:11211".parse().unwrap()))
    .await
    .unwrap();
  conn.flush().await.unwrap();
  assert_eq!("1.6.19", conn.version().await.unwrap());
  test_binary_commands(&mut conn).await;
  conn.close().await.unwrap();
}

#[tokio::test]
async fn test_text_protocol() {
  let mut stream = TcpStream::connect("[::]:11211").await.map(BufStream::new).unwrap();
  text_flush(&mut stream).await;
  assert_eq!("VERSION 1.6.19\r\n", text_version(&mut stream).await);
  test_text_commands(&mut stream).await;
  stream.shutdown().await.unwrap();
}

#[tokio::test]
// #[ignore]
async fn test_mcrouteur_proxy_configuration() {
  let _command = Command::new(env!("CARGO_BIN_EXE_mcrouteur"))
    .args([
      "--bind-url",
      "tcp://localhost:11210",
      "--config",
      r#"{
        "upstreams": {
          "primary": "tcp://[::]:11211?pool_size=3"
        },
        "wildcard_route":{
          "type": "proxy",
          "upstream": "primary"
        }
      }"#,
    ])
    .kill_on_drop(true)
    .spawn()
    .unwrap();

  let mut upstream_conn = Connection::connect(&("tcp://[::]:11211".parse().unwrap()))
    .await
    .unwrap();

  let mut binary_conn =
    Connection::connect_with_exponential_backoff(&("tcp://[::]:11210".parse().unwrap()), Duration::from_millis(100), 5)
      .await
      .unwrap();

  upstream_conn.flush().await.unwrap();
  test_binary_commands(&mut binary_conn).await;
  upstream_conn.flush().await.unwrap();
  test_binary_quiet_commands(&mut binary_conn).await;

  binary_conn.close().await.unwrap();

  let mut text_conn = TcpStream::connect("[::]:11210").await.map(BufStream::new).unwrap();

  upstream_conn.flush().await.unwrap();
  test_text_commands(&mut text_conn).await;

  text_conn.shutdown().await.unwrap();

  upstream_conn.close().await.unwrap();
}

#[tokio::test]
async fn test_mcrouter_prefix_routes() {
  let _command = Command::new(env!("CARGO_BIN_EXE_mcrouteur"))
    .args([
      "--bind-url",
      "tcp://localhost:11210",
      "--config",
      r#"{
        "upstreams": {
          "a": "tcp://[::]:11211",
          "b": "tcp://[::]:11212",
          "c": "tcp://[::]:11213"
        },
        "routes": {
          "a:": {"type": "proxy", "upstream": "a"},
          "b:": {"type": "proxy", "upstream": "b"},
          "c:": {"type": "proxy", "upstream": "c"}
        }
      }"#,
    ])
    .kill_on_drop(true)
    .spawn()
    .unwrap();

  let mut proxy =
    Connection::connect_with_exponential_backoff(&("tcp://[::]:11210".parse().unwrap()), Duration::from_millis(100), 5)
      .await
      .unwrap();

  let mut a = Connection::connect(&("tcp://[::]:11211".parse().unwrap()))
    .await
    .unwrap();
  let mut b = Connection::connect(&("tcp://[::]:11212".parse().unwrap()))
    .await
    .unwrap();
  let mut c = Connection::connect(&("tcp://[::]:11213".parse().unwrap()))
    .await
    .unwrap();

  tokio::try_join!(a.flush(), b.flush(), c.flush()).unwrap();

  proxy.set("a:foo", "toto", 0, 0, None).await.unwrap();
  assert_eq!(b"toto", a.get("a:foo").await.unwrap().chunk());
  assert_eq!(
    Some(ServerError::KeyNotFound),
    b.get("a:foo").await.unwrap_err().as_server_error()
  );
  assert_eq!(
    Some(ServerError::KeyNotFound),
    c.get("a:foo").await.unwrap_err().as_server_error()
  );

  proxy.set("b:foo", "titi", 0, 0, None).await.unwrap();
  assert_eq!(
    Some(ServerError::KeyNotFound),
    a.get("b:foo").await.unwrap_err().as_server_error()
  );
  assert_eq!(b"titi", b.get("b:foo").await.unwrap().chunk());
  assert_eq!(
    Some(ServerError::KeyNotFound),
    c.get("b:foo").await.unwrap_err().as_server_error()
  );

  proxy.set("c:foo", "tata", 0, 0, None).await.unwrap();
  assert_eq!(
    Some(ServerError::KeyNotFound),
    a.get("c:foo").await.unwrap_err().as_server_error()
  );
  assert_eq!(
    Some(ServerError::KeyNotFound),
    b.get("c:foo").await.unwrap_err().as_server_error()
  );
  assert_eq!(b"tata", c.get("c:foo").await.unwrap().chunk());

  tokio::try_join!(proxy.close(), a.close(), b.close(), c.close()).unwrap();
}

async fn test_binary_commands(conn: &mut Connection) {
  conn.set("foo", b"bar", 0, 0, None).await.unwrap();
  assert_eq!(b"bar", conn.get("foo").await.unwrap().chunk());

  conn.set("bar", b"baz", 0, 0, None).await.unwrap();
  assert_eq!(b"baz", conn.get("bar").await.unwrap().chunk());

  conn.set("toto", b"tata", 0, 0, None).await.unwrap();
  assert_eq!(b"tata", conn.get("toto").await.unwrap().chunk());

  conn.set("empty", b"", 0, 0, None).await.unwrap();
  assert_eq!(b"", conn.get("empty").await.unwrap().chunk());

  assert_eq!(
    Some(ServerError::KeyNotFound),
    conn.get("missing").await.unwrap_err().as_server_error()
  );

  assert_eq!(
    Some(ServerError::KeyExists),
    conn.add("foo", b"baz", 0, 0, None).await.unwrap_err().as_server_error()
  );

  conn.add("zoo", "pet", 0, 0, None).await.unwrap();
  assert_eq!(b"pet", conn.get("zoo").await.unwrap().chunk());

  assert_eq!(
    Some(ServerError::KeyNotFound),
    conn
      .replace("missing", b"baz", 0, 0, None)
      .await
      .unwrap_err()
      .as_server_error()
  );

  conn.replace("foo", b"baz", 0, 0, None).await.unwrap();
  assert_eq!(b"baz", conn.get("foo").await.unwrap().chunk());

  conn.delete("toto").await.unwrap();
  assert_eq!(
    Some(ServerError::KeyNotFound),
    conn.get("toto").await.unwrap_err().as_server_error()
  );

  conn.touch("foo", 1).await.unwrap();
  assert_eq!(b"baz", conn.gat("bar", 1).await.unwrap().chunk());
}

async fn test_binary_quiet_commands(conn: &mut Connection) {
  conn.setq("foo", b"bar", 0, 0, None).await.unwrap();
  assert_eq!(b"bar", conn.get("foo").await.unwrap().chunk());

  conn.addq("foo", b"baz", 0, 0, None).await.unwrap();
  assert_eq!(b"bar", conn.get("foo").await.unwrap().chunk());
}

async fn text_flush(s: &mut BufStream<TcpStream>) {
  s.write_all(b"flush_all\r\n").await.unwrap();
  s.flush().await.unwrap();
  let mut buffer = String::new();
  s.read_line(&mut buffer).await.unwrap();
  assert_eq!("OK\r\n", buffer);
}

async fn text_version(s: &mut BufStream<TcpStream>) -> String {
  s.write_all(b"version\r\n").await.unwrap();
  s.flush().await.unwrap();
  let mut buffer = String::new();
  s.read_line(&mut buffer).await.unwrap();
  buffer
}

async fn test_text_commands(s: &mut BufStream<TcpStream>) {
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
