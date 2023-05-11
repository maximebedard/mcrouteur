use std::{
  collections::{BTreeMap, VecDeque},
  io, ops,
  sync::{Arc, RwLock},
  time::{Duration, Instant},
};

use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};

pub mod memcached;
pub mod redis;

#[derive(Debug)]
pub struct Router<C> {
  prefixes: BTreeMap<String, C>,
  catch_all: Option<C>,
}

impl<C> Default for Router<C> {
  fn default() -> Self {
    let prefixes = Default::default();
    let catch_all = Default::default();
    Self { prefixes, catch_all }
  }
}

impl<C> Router<C> {
  pub fn get_connection(&self, k: &String) -> Option<&C> {
    self.prefixes.get(k).or(self.catch_all.as_ref())
  }

  pub fn insert_prefix_route(&mut self, key: impl Into<String>, value: C) -> Option<C> {
    self.prefixes.insert(key.into(), value)
  }

  pub fn replace_catch_all(&mut self, value: C) -> Option<C> {
    self.catch_all.replace(value)
  }
}

#[derive(Debug)]
pub enum ConnectionPoolManagerEvent<C> {
  Connect {
    sender: oneshot::Sender<io::Result<C>>,
  },
  Disconnect {
    connection: C,
    sender: oneshot::Sender<io::Result<()>>,
  },
  Ping {
    connection: C,
    sender: oneshot::Sender<io::Result<C>>,
  },
}

#[derive(Debug)]
pub struct ConnectionPoolManager<C>(mpsc::Sender<ConnectionPoolManagerEvent<C>>);

impl<C> Clone for ConnectionPoolManager<C> {
  fn clone(&self) -> Self {
    Self(self.0.clone())
  }
}

impl<C> ConnectionPoolManager<C> {
  async fn connect(&self) -> io::Result<C> {
    let (sender, receiver) = oneshot::channel();
    self.0.send(ConnectionPoolManagerEvent::Connect { sender }).await.ok();
    receiver.await.unwrap()
  }

  async fn disconnect(&self, connection: C) -> io::Result<()> {
    let (sender, receiver) = oneshot::channel();
    self
      .0
      .send(ConnectionPoolManagerEvent::Disconnect { connection, sender })
      .await
      .ok();
    receiver.await.unwrap()
  }

  async fn ping(&self, connection: C) -> io::Result<C> {
    let (sender, receiver) = oneshot::channel();
    self
      .0
      .send(ConnectionPoolManagerEvent::Ping { connection, sender })
      .await
      .ok();
    receiver.await.unwrap()
  }

  async fn reconnect(&self, connection: C) -> io::Result<C> {
    self.disconnect(connection).await;
    self.connect().await
  }
}

pub async fn spawn_manager() -> ConnectionPoolManager<memcached::Connection> {
  let (sender, mut receiver) = mpsc::channel::<ConnectionPoolManagerEvent<memcached::Connection>>(32);
  while let Some(evt) = receiver.recv().await {
    tokio::task::spawn(async move {
      match evt {
        ConnectionPoolManagerEvent::Connect { sender } => {
          sender.send(Ok(memcached::Connection)).ok();
        }
        ConnectionPoolManagerEvent::Disconnect { connection: _, sender } => {
          sender.send(Ok(())).ok();
        }
        ConnectionPoolManagerEvent::Ping { mut connection, sender } => {
          let is_ok = connection.ping().await.is_ok();
          sender.send(Ok(connection)).ok();
        }
      }
    });
  }

  ConnectionPoolManager(sender)
}

#[derive(Debug)]
pub struct ConnectionPoolBuilder<C> {
  manager: ConnectionPoolManager<C>,
  size: usize,
  min_idle: Option<usize>,
  max_lifetime: Option<Duration>,
  idle_timeout: Option<Duration>,
  idle_cleanup_interval: Option<Duration>,
  connect_timeout: Option<Duration>,
}

impl<C> ConnectionPoolBuilder<C> {
  pub fn new(manager: ConnectionPoolManager<C>, size: usize) -> Self {
    Self {
      manager,
      size,
      min_idle: None,
      max_lifetime: None,
      idle_timeout: None,
      idle_cleanup_interval: None,
      connect_timeout: None,
    }
  }

  pub fn min_idle(self, min_idle: Option<usize>) -> Self {
    Self { min_idle, ..self }
  }

  pub fn max_lifetime(self, max_lifetime: Option<Duration>) -> Self {
    Self { max_lifetime, ..self }
  }

  pub fn idle_timeout(self, idle_timeout: Option<Duration>) -> Self {
    Self { idle_timeout, ..self }
  }

  pub fn connect_timeout(self, connect_timeout: Option<Duration>) -> Self {
    Self {
      connect_timeout,
      ..self
    }
  }

  pub fn build(self) -> ConnectionPool<C> {
    let Self {
      manager,
      size,
      min_idle,
      max_lifetime,
      idle_timeout,
      idle_cleanup_interval,
      connect_timeout,
    } = self;
    let semaphore = Arc::new(Semaphore::new(size));
    let inner = Arc::new(ConnectionPoolInner {
      connections: RwLock::new(VecDeque::with_capacity(size)),
      min_idle,
      max_lifetime,
      idle_timeout,
      idle_cleanup_interval,
      connect_timeout,
    });
    ConnectionPool {
      manager,
      semaphore,
      inner,
    }
  }
}

pub async fn gc<C>(connection_pool: ConnectionPool<C>) {
  let interval = connection_pool
    .inner
    .idle_cleanup_interval
    .unwrap_or(Duration::from_secs(30));
  let mut interval = tokio::time::interval(interval);

  let interrupt = tokio::signal::ctrl_c();
  tokio::pin!(interrupt);

  loop {
    tokio::select! {
        _ = &mut interrupt => break,
        _ = interval.tick() => {
            connection_pool.remove_idle_connections().await;
        },
    }
  }
}
#[derive(Debug)]
pub struct ConnectionPool<C> {
  manager: ConnectionPoolManager<C>,
  semaphore: Arc<Semaphore>,
  inner: Arc<ConnectionPoolInner<C>>,
}

impl<C> ConnectionPool<C> {
  pub async fn acquire(&self) -> io::Result<PooledConnection<C>> {
    let permit = self.semaphore.clone().acquire_owned().await.unwrap();
    let connection = self.inner.pop_front_connection();
    let pool = self.clone();

    let connection = match connection {
      Some(ConnectionWrapper {
        mut inner, created_at, ..
      }) => {
        let mut created_at = created_at;
        if let Some(max_lifetime) = self.inner.max_lifetime {
          if created_at.elapsed() >= max_lifetime {
            inner = self.manager.reconnect(inner).await?;
            created_at = Instant::now();
          }
        }

        let last_aquired_at = Instant::now();
        ConnectionWrapper {
          inner,
          created_at,
          last_aquired_at,
        }
      }
      None => {
        let last_aquired_at = Instant::now();
        let created_at = last_aquired_at.clone();
        let inner = self.manager.connect().await?;
        ConnectionWrapper {
          inner,
          created_at,
          last_aquired_at,
        }
      }
    };

    Ok(PooledConnection::new(connection, pool, permit))
  }

  async fn remove_idle_connections(&self) {
    if let Some(idle_connections) = self.inner.pop_idle_connections() {
      for ConnectionWrapper { inner, .. } in idle_connections {
        self.manager.disconnect(inner).await;
      }
    }
  }
}

impl<C> Clone for ConnectionPool<C> {
  fn clone(&self) -> Self {
    let manager = self.manager.clone();
    let semaphore = self.semaphore.clone();
    let inner = self.inner.clone();
    Self {
      manager,
      semaphore,
      inner,
    }
  }
}

#[derive(Debug)]
pub struct ConnectionPoolInner<C> {
  connections: RwLock<VecDeque<ConnectionWrapper<C>>>,
  min_idle: Option<usize>,
  max_lifetime: Option<Duration>,
  idle_timeout: Option<Duration>,
  idle_cleanup_interval: Option<Duration>,
  connect_timeout: Option<Duration>,
}

impl<C> ConnectionPoolInner<C> {
  fn pop_front_connection(&self) -> Option<ConnectionWrapper<C>> {
    let mut connections = self.connections.write().unwrap();
    connections.pop_front()
  }

  fn push_back_connection(&self, c: ConnectionWrapper<C>) {
    let mut connections = self.connections.write().unwrap();
    connections.push_back(c);
  }

  fn pop_idle_connections(&self) -> Option<VecDeque<ConnectionWrapper<C>>> {
    if let Some(idle_timeout) = self.idle_timeout {
      let mut connections = self.connections.write().unwrap();

      let mut active_connections = VecDeque::with_capacity(connections.capacity());
      let mut inactive_connections = VecDeque::with_capacity(connections.capacity());
      while let Some(c) = connections.pop_front() {
        if c.last_aquired_at.elapsed() < idle_timeout {
          active_connections.push_back(c);
        } else {
          inactive_connections.push_back(c);
        }
      }

      *connections = active_connections;

      Some(inactive_connections)
    } else {
      None
    }
  }
}

#[derive(Debug)]
struct ConnectionWrapper<C> {
  inner: C,
  created_at: Instant,
  last_aquired_at: Instant,
}

impl<C> ConnectionWrapper<C> {
  fn inner(&self) -> &C {
    &self.inner
  }

  fn inner_mut(&mut self) -> &mut C {
    &mut self.inner
  }
}

pub struct PooledConnection<C> {
  connection: Option<ConnectionWrapper<C>>,
  pool: ConnectionPool<C>,
  permit: OwnedSemaphorePermit,
}
impl<C> PooledConnection<C> {
  fn new(connection: ConnectionWrapper<C>, pool: ConnectionPool<C>, permit: OwnedSemaphorePermit) -> Self {
    let connection = Some(connection);
    Self {
      connection,
      pool,
      permit,
    }
  }
}

impl<C> ops::Deref for PooledConnection<C> {
  type Target = C;

  fn deref(&self) -> &Self::Target {
    self.connection.as_ref().unwrap().inner()
  }
}

impl<C> ops::DerefMut for PooledConnection<C> {
  fn deref_mut(&mut self) -> &mut Self::Target {
    self.connection.as_mut().unwrap().inner_mut()
  }
}

impl<C> Drop for PooledConnection<C> {
  fn drop(&mut self) {
    if let Some(c) = self.connection.take() {
      self.pool.inner.push_back_connection(c);
    }
  }
}
