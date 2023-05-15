use std::io;

use kvp::memcached::Connection;

#[tokio::test]
async fn test_memcached() {
  let mut conn = setup_connection().await.unwrap();
}

async fn setup_connection() -> io::Result<Connection> {
  Connection::connect("tcp://localhost:11211".parse().unwrap()).await
}
