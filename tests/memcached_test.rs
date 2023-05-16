use std::io;

use kvp::memcached::{BinaryConnection, Connection};

#[tokio::test]
async fn test_memcached() {
  let mut conn = BinaryConnection::connect("tcp://localhost:11211".parse().unwrap())
    .await
    .unwrap();

  conn.close().await.unwrap();
}

async fn setup_connection() -> io::Result<Connection> {
  Connection::connect("tcp://localhost:11211".parse().unwrap()).await
}
