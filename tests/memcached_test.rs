use bytes::Buf;
use kvp::connection::{Connection, ServerError};
use tokio::process::Command;

#[tokio::test]
async fn test_memcached() {
  let mut conn = Connection::connect("tcp://localhost:11211".parse().unwrap())
    .await
    .unwrap();

  conn.noop().await.unwrap();
  conn.flush().await.unwrap();

  assert_eq!("1.6.19", conn.version().await.unwrap().as_str());

  conn.set("foo", b"bar", 0, None).await.unwrap();
  conn.set("bar", b"baz", 0, None).await.unwrap();
  conn.set("empty", b"", 0, None).await.unwrap();

  assert_eq!(b"bar", conn.get("foo").await.unwrap().chunk());
  assert_eq!(b"baz", conn.get("bar").await.unwrap().chunk());
  assert_eq!(b"", conn.get("empty").await.unwrap().chunk());

  assert_eq!(
    Some(ServerError::KeyNotFound),
    conn.get("missing").await.unwrap_err().as_server_error()
  );

  assert_eq!(
    Some(ServerError::KeyExists),
    conn.add("foo", b"baz", 0, None).await.unwrap_err().as_server_error()
  );

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

  conn.touch("foo", 1).await.unwrap();
  assert_eq!(b"baz", conn.gat("bar", 1).await.unwrap().chunk());

  assert!(!conn.stats().await.unwrap().is_empty());

  conn.close().await.unwrap();
}

#[tokio::test]
async fn test_memcached_proxy() {
  let mut command = Command::new(env!("CARGO_BIN_EXE_memcached"))
    .arg("--bind-url tcp://localhost:11212")
    .arg("--upstream-url primary=tcp://localhost:11211")
    .arg("--upstream-url secondary=tcp://localhost:11211")
    .spawn()
    .unwrap();

  command.kill().await.unwrap();
  command.wait().await.unwrap();
}
