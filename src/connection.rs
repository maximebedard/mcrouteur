use std::{
  collections::BTreeMap,
  fmt, io,
  net::{SocketAddrV4, SocketAddrV6},
  str::Utf8Error,
  time::Duration,
};

use bytes::{Buf, Bytes, BytesMut};
use tokio::{
  io::{AsyncReadExt, AsyncWriteExt, BufStream},
  net::TcpStream,
  sync::{mpsc, oneshot},
  task::JoinHandle,
  time::Instant,
};
use url::Url;

use crate::codec::{AppendPrependCommand, IncrDecrCommand, KeyCommand, SetCommand, TouchCommand};

#[derive(Debug, PartialEq)]
pub enum ServerError {
  KeyNotFound,
  KeyExists,
  ValueTooLarge,
  InvalidArguments,
  ItemNotStored,
  IncrDecrOnNonNumericValue,
  UnknownCommand,
  OutOfMemory,
  NotSupported,
  InternalError,
  Busy,
  TemporaryFailure,
  Unknown(u16),
}

impl ServerError {
  pub fn code(&self) -> u16 {
    match self {
      Self::KeyNotFound => 0x0001,
      Self::KeyExists => 0x0002,
      Self::ValueTooLarge => 0x0003,
      Self::InvalidArguments => 0x0004,
      Self::ItemNotStored => 0x0005,
      Self::IncrDecrOnNonNumericValue => 0x0006,
      Self::UnknownCommand => 0x0081,
      Self::OutOfMemory => 0x0082,
      Self::NotSupported => 0x0083,
      Self::InternalError => 0x0084,
      Self::Busy => 0x0085,
      Self::TemporaryFailure => 0x0086,
      Self::Unknown(code) => *code,
    }
  }
}

impl fmt::Display for ServerError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      ServerError::KeyNotFound => write!(f, "key not found"),
      ServerError::KeyExists => write!(f, "key exists"),
      ServerError::ValueTooLarge => write!(f, "value too large"),
      ServerError::InvalidArguments => write!(f, "invalid arguments"),
      ServerError::ItemNotStored => write!(f, "item not stored"),
      ServerError::IncrDecrOnNonNumericValue => write!(f, "incr/decr on non-numeric value"),
      ServerError::UnknownCommand => write!(f, "unknown command"),
      ServerError::OutOfMemory => write!(f, "out of memory"),
      ServerError::NotSupported => write!(f, "not supported"),
      ServerError::InternalError => write!(f, "internal error"),
      ServerError::Busy => write!(f, "busy"),
      ServerError::TemporaryFailure => todo!(),
      ServerError::Unknown(code) => write!(f, "unknown error code {code}"),
    }
  }
}

#[derive(Debug)]
pub enum Error {
  Server(ServerError),
  Io(io::Error),
}

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::Server(err) => write!(f, "{}", err),
      Self::Io(err) => write!(f, "{}", err),
    }
  }
}

impl From<ServerError> for Error {
  fn from(value: ServerError) -> Self {
    Self::Server(value)
  }
}

impl From<io::Error> for Error {
  fn from(value: io::Error) -> Self {
    Self::Io(value)
  }
}

impl From<oneshot::error::RecvError> for Error {
  fn from(_value: oneshot::error::RecvError) -> Self {
    Self::Io(io::Error::new(io::ErrorKind::Interrupted, "interrupted"))
  }
}

impl From<Utf8Error> for Error {
  fn from(_value: Utf8Error) -> Self {
    Self::Io(io::Error::new(
      io::ErrorKind::InvalidData,
      "byte sequence is not valid utf-8",
    ))
  }
}

impl Error {
  pub fn as_server_error(self) -> Option<ServerError> {
    match self {
      Self::Server(err) => Some(err),
      _ => None,
    }
  }

  pub fn as_io_error(self) -> Option<io::Error> {
    match self {
      Self::Io(err) => Some(err),
      _ => None,
    }
  }
}

pub enum Command {
  Get(KeyCommand, oneshot::Sender<Result<Bytes>>),
  Delete(KeyCommand, oneshot::Sender<Result<()>>),
  GetAndTouch(TouchCommand, oneshot::Sender<Result<Bytes>>),
  Touch(TouchCommand, oneshot::Sender<Result<()>>),
  Set(SetCommand, oneshot::Sender<Result<()>>),
  Add(SetCommand, oneshot::Sender<Result<()>>),
  Replace(SetCommand, oneshot::Sender<Result<()>>),
  Append(AppendPrependCommand, oneshot::Sender<Result<()>>),
  Prepend(AppendPrependCommand, oneshot::Sender<Result<()>>),
  Increment(IncrDecrCommand, oneshot::Sender<Result<u64>>),
  Decrement(IncrDecrCommand, oneshot::Sender<Result<u64>>),
}

impl Command {
  pub fn key(&self) -> &String {
    match self {
      Command::Get(KeyCommand { ref key }, _) => key,
      Command::Delete(KeyCommand { ref key }, _) => key,
      Command::GetAndTouch(TouchCommand { ref key, .. }, _) => key,
      Command::Touch(TouchCommand { ref key, .. }, _) => key,
      Command::Set(SetCommand { ref key, .. }, _) => key,
      Command::Add(SetCommand { ref key, .. }, _) => key,
      Command::Replace(SetCommand { ref key, .. }, _) => key,
      Command::Append(AppendPrependCommand { ref key, .. }, _) => key,
      Command::Prepend(AppendPrependCommand { ref key, .. }, _) => key,
      Command::Increment(IncrDecrCommand { ref key, .. }, _) => key,
      Command::Decrement(IncrDecrCommand { ref key, .. }, _) => key,
    }
  }
}

impl fmt::Debug for Command {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::Get(args, _) => f.debug_tuple("Get").field(args).finish(),
      Self::Delete(args, _) => f.debug_tuple("Delete").field(args).finish(),
      Self::GetAndTouch(args, _) => f.debug_tuple("GetAndTouch").field(args).finish(),
      Self::Touch(args, _) => f.debug_tuple("Touch").field(args).finish(),
      Self::Set(args, _) => f.debug_tuple("Set").field(args).finish(),
      Self::Add(args, _) => f.debug_tuple("Add").field(args).finish(),
      Self::Replace(args, _) => f.debug_tuple("Replace").field(args).finish(),
      Self::Append(args, _) => f.debug_tuple("Append").field(args).finish(),
      Self::Prepend(args, _) => f.debug_tuple("Prepend").field(args).finish(),
      Self::Increment(args, _) => f.debug_tuple("Increment").field(args).finish(),
      Self::Decrement(args, _) => f.debug_tuple("Decrement").field(args).finish(),
    }
  }
}

pub fn spawn_connection(url: Url) -> (mpsc::Sender<Command>, JoinHandle<()>) {
  let params = url.query_pairs().collect::<BTreeMap<_, _>>();

  let pool_size = params.get("pool_size").and_then(|v| v.parse::<usize>().ok());

  fn spawn(url: Url) -> (mpsc::Sender<Command>, JoinHandle<()>) {
    let params = url.query_pairs().collect::<BTreeMap<_, _>>();

    let max_idle_duration = params
      .get("max_idle")
      .and_then(|v| v.parse().ok().map(Duration::from_secs))
      .unwrap_or(Duration::from_secs(30 * 60));

    let max_age_duration = params
      .get("max_age")
      .and_then(|v| v.parse().ok().map(Duration::from_secs))
      .unwrap_or(Duration::from_secs(30 * 60));

    let (sender, mut receiver) = mpsc::channel(32);

    let handle = tokio::task::spawn(async move {
      while let Some(command) = receiver.recv().await {
        let mut connection = Connection::connect(&url).await.unwrap();
        handle_command(&mut connection, command).await;

        let idle_deadline = tokio::time::sleep(max_idle_duration);
        tokio::pin!(idle_deadline);

        let age_deadline = tokio::time::sleep(max_age_duration);
        tokio::pin!(age_deadline);

        loop {
          tokio::select! {
            _ = &mut idle_deadline => break,
            _ = &mut age_deadline => break,
            command = receiver.recv() => match command {
              Some(command) => {
                idle_deadline.as_mut().reset(Instant::now() + max_idle_duration);
                handle_command(&mut connection, command).await
              },
              None => break,
            },
          }
        }

        connection.close().await.unwrap();
      }
    });

    (sender, handle)
  }

  fn spawn_pool(url: Url, pool_size: usize) -> (mpsc::Sender<Command>, JoinHandle<()>) {
    let (sender, mut receiver) = mpsc::channel::<Command>(32);

    let handle = tokio::task::spawn(async move {
      let mut connections = vec![];
      for _ in 0..pool_size {
        connections.push(spawn(url.clone()));
      }

      let mut i = 0;
      while let Some(command) = receiver.recv().await {
        println!("{:?}", command);
        if let Err(_err) = connections[i].0.send(command).await {
          todo!()
        }

        if i < connections.len() - 1 {
          i += 1;
        } else {
          i = 0;
        }
      }

      for (_, handle) in connections {
        handle.await.unwrap()
      }
    });

    (sender, handle)
  }

  match pool_size {
    Some(pool_size) if pool_size > 1 => spawn_pool(url, pool_size),
    _ => spawn(url),
  }
}

async fn handle_command(connection: &mut Connection, command: Command) {
  match command {
    Command::Get(KeyCommand { key }, sender) => {
      sender.send(connection.get(key).await).ok();
    }
    Command::GetAndTouch(TouchCommand { key, exptime }, sender) => {
      sender.send(connection.gat(key, exptime).await).ok();
    }
    Command::Touch(TouchCommand { key, exptime }, sender) => {
      sender.send(connection.touch(key, exptime).await).ok();
    }
    Command::Set(
      SetCommand {
        key,
        value,
        flags,
        exptime,
        cas,
      },
      sender,
    ) => {
      sender.send(connection.set(key, value, flags, exptime, cas).await).ok();
    }
    Command::Add(
      SetCommand {
        key,
        value,
        flags,
        exptime,
        cas,
      },
      sender,
    ) => {
      sender.send(connection.add(key, value, flags, exptime, cas).await).ok();
    }
    Command::Replace(
      SetCommand {
        key,
        value,
        flags,
        exptime,
        cas,
      },
      sender,
    ) => {
      sender
        .send(connection.replace(key, value, flags, exptime, cas).await)
        .ok();
    }

    Command::Append(AppendPrependCommand { key, value }, sender) => {
      sender.send(connection.append(key, value).await).ok();
    }
    Command::Prepend(AppendPrependCommand { key, value }, sender) => {
      sender.send(connection.prepend(key, value).await).ok();
    }
    Command::Increment(
      IncrDecrCommand {
        key,
        delta,
        init,
        exptime,
      },
      sender,
    ) => {
      sender.send(connection.incr(key, delta, init, exptime).await).ok();
    }
    Command::Decrement(
      IncrDecrCommand {
        key,
        delta,
        init,
        exptime,
      },
      sender,
    ) => {
      sender.send(connection.decr(key, delta, init, exptime).await).ok();
    }
    Command::Delete(KeyCommand { key }, sender) => {
      sender.send(connection.delete(key).await).ok();
    }
  }
}

pub type Result<T> = std::result::Result<T, Error>;

pub struct Connection {
  stream: BufStream<TcpStream>,
}

impl Connection {
  pub async fn connect(url: &Url) -> Result<Self> {
    assert_eq!("tcp", url.scheme()); // only support tcp for now

    let port = url.port().unwrap_or(11211);
    let addr = match url.host() {
      Some(url::Host::Domain(domain)) => {
        tokio::net::lookup_host(format!("{domain}:{port}"))
          .await
          .and_then(|mut v| {
            v.next()
              .ok_or_else(|| io::Error::new(io::ErrorKind::ConnectionReset, "unable to find host"))
          })?
      }
      Some(url::Host::Ipv4(ip)) => SocketAddrV4::new(ip, port).into(),
      Some(url::Host::Ipv6(ip)) => SocketAddrV6::new(ip, port, 0, 0).into(),
      None => format!("[::]:{port}").parse().unwrap(),
    };

    let stream = TcpStream::connect(addr).await.map(BufStream::new)?;

    Ok(Self { stream })
  }

  pub async fn connect_with_exponential_backoff(url: &Url, mut d: Duration, max_retries: usize) -> Result<Self> {
    let mut retries = 0;
    loop {
      match Self::connect(&url).await {
        r @ Ok(_) => return r,
        Err(Error::Io(err)) if err.kind() == io::ErrorKind::ConnectionRefused && retries < max_retries => {
          tokio::time::sleep(d).await;
          d = 2 * d;
          retries += 1;
        }
        r @ Err(_) => return r,
      }
    }
  }

  pub async fn close(mut self) -> Result<()> {
    self.stream.shutdown().await.map_err(Into::into)
  }

  pub async fn get(&mut self, key: impl AsRef<str>) -> Result<Bytes> {
    self.write_key_command(0x00, key.as_ref()).await?;
    self.stream.flush().await?;
    let (_header, mut body) = self.read_response().await?;
    let _flags = body.get_u32();
    Ok(body)
  }

  pub async fn gat(&mut self, key: impl AsRef<str>, exptime: u32) -> Result<Bytes> {
    self.write_touch_command(0x1d, key.as_ref(), exptime).await?;
    self.stream.flush().await?;
    let (_header, mut body) = self.read_response().await?;
    let _flags = body.get_u32();
    Ok(body)
  }

  pub async fn touch(&mut self, key: impl AsRef<str>, exptime: u32) -> Result<()> {
    self.write_touch_command(0x1c, key.as_ref(), exptime).await?;
    self.stream.flush().await?;
    let (_header, mut body) = self.read_response().await?;
    let _flags = body.get_u32();
    assert!(body.is_empty());
    Ok(())
  }

  pub async fn set(
    &mut self,
    key: impl AsRef<str>,
    value: impl AsRef<[u8]>,
    flags: u32,
    exptime: u32,
    cas: Option<u64>,
  ) -> Result<()> {
    self
      .set_command(0x01, key.as_ref(), value.as_ref(), flags, exptime, cas)
      .await
  }

  pub async fn setq(
    &mut self,
    key: impl AsRef<str>,
    value: impl AsRef<[u8]>,
    flags: u32,
    exptime: u32,
    cas: Option<u64>,
  ) -> io::Result<()> {
    self
      .set_command_quiet(0x11, key.as_ref(), value.as_ref(), flags, exptime, cas)
      .await
  }

  pub async fn add(
    &mut self,
    key: impl AsRef<str>,
    value: impl AsRef<[u8]>,
    flags: u32,
    exptime: u32,
    cas: Option<u64>,
  ) -> Result<()> {
    self
      .set_command(0x02, key.as_ref(), value.as_ref(), flags, exptime, cas)
      .await
  }

  pub async fn addq(
    &mut self,
    key: impl AsRef<str>,
    value: impl AsRef<[u8]>,
    flags: u32,
    exptime: u32,
    cas: Option<u64>,
  ) -> io::Result<()> {
    self
      .set_command_quiet(0x12, key.as_ref(), value.as_ref(), flags, exptime, cas)
      .await
  }

  pub async fn replace(
    &mut self,
    key: impl AsRef<str>,
    value: impl AsRef<[u8]>,
    flags: u32,
    exptime: u32,
    cas: Option<u64>,
  ) -> Result<()> {
    self
      .set_command(0x03, key.as_ref(), value.as_ref(), flags, exptime, cas)
      .await
  }

  pub async fn replaceq(
    &mut self,
    key: impl AsRef<str>,
    value: impl AsRef<[u8]>,
    flags: u32,
    exptime: u32,
    cas: Option<u64>,
  ) -> io::Result<()> {
    self
      .set_command_quiet(0x13, key.as_ref(), value.as_ref(), flags, exptime, cas)
      .await
  }

  async fn set_command(
    &mut self,
    op: u8,
    key: &str,
    value: &[u8],
    flags: u32,
    exptime: u32,
    cas: Option<u64>,
  ) -> Result<()> {
    self.write_set_command(op, key, value, flags, exptime, cas).await?;
    self.stream.flush().await?;
    self.read_empty_response().await
  }

  async fn set_command_quiet(
    &mut self,
    op: u8,
    key: &str,
    value: &[u8],
    flags: u32,
    exptime: u32,
    cas: Option<u64>,
  ) -> io::Result<()> {
    self.write_set_command(op, key, value, flags, exptime, cas).await?;
    self.stream.flush().await
  }

  pub async fn stats(&mut self) -> Result<BTreeMap<String, String>> {
    self.write_command(0x10).await?;
    self.stream.flush().await?;

    let mut kv = BTreeMap::new();

    loop {
      let (header, mut body) = self.read_response().await?;
      if header.body_len == 0 {
        break;
      }

      let key = std::str::from_utf8(body.split_to(header.key_len).chunk()).map(ToString::to_string)?;
      let value =
        std::str::from_utf8(body.split_to(header.body_len - header.key_len).chunk()).map(ToString::to_string)?;
      assert!(body.is_empty());
      kv.insert(key, value);
    }

    Ok(kv)
  }

  pub async fn incr(&mut self, key: impl AsRef<str>, delta: u64, init: u64, exptime: u32) -> Result<u64> {
    self.incr_decr_command(0x05, key.as_ref(), delta, init, exptime).await
  }

  pub async fn incrq(&mut self, key: impl AsRef<str>, delta: u64, init: u64, exptime: u32) -> io::Result<()> {
    self
      .incr_decr_command_quiet(0x15, key.as_ref(), delta, init, exptime)
      .await
  }

  pub async fn decr(&mut self, key: impl AsRef<str>, delta: u64, init: u64, exptime: u32) -> Result<u64> {
    self.incr_decr_command(0x06, key.as_ref(), delta, init, exptime).await
  }

  pub async fn decrq(&mut self, key: impl AsRef<str>, delta: u64, init: u64, exptime: u32) -> io::Result<()> {
    self
      .incr_decr_command_quiet(0x16, key.as_ref(), delta, init, exptime)
      .await
  }

  async fn incr_decr_command_quiet(
    &mut self,
    op: u8,
    key: &str,
    delta: u64,
    init: u64,
    exptime: u32,
  ) -> io::Result<()> {
    self.write_incr_decr_command(op, key, delta, init, exptime).await?;
    self.stream.flush().await
  }

  async fn incr_decr_command(&mut self, op: u8, key: &str, delta: u64, init: u64, exptime: u32) -> Result<u64> {
    self.write_incr_decr_command(op, key, delta, init, exptime).await?;
    self.stream.flush().await?;
    let (_header, mut body) = self.read_response().await?;
    let value = body.get_u64();
    assert!(body.is_empty());
    Ok(value)
  }

  pub async fn version(&mut self) -> Result<String> {
    self.write_command(0x0b).await?;
    self.stream.flush().await?;
    let (header, mut body) = self.read_response().await?;
    let version = std::str::from_utf8(body.split_to(header.body_len).chunk())
      .map_err(Into::into)
      .map(ToString::to_string);
    assert!(body.is_empty());
    version
  }

  pub async fn noop(&mut self) -> Result<()> {
    self.command(0x0a).await
  }

  pub async fn append(&mut self, key: impl AsRef<str>, value: impl AsRef<[u8]>) -> Result<()> {
    self.append_prepend_command(0x0e, key.as_ref(), value.as_ref()).await
  }

  pub async fn appendq(&mut self, key: impl AsRef<str>, value: impl AsRef<[u8]>) -> io::Result<()> {
    self
      .append_prepend_command_quiet(0x0e, key.as_ref(), value.as_ref())
      .await
  }

  pub async fn prepend(&mut self, key: impl AsRef<str>, value: impl AsRef<[u8]>) -> Result<()> {
    self.append_prepend_command(0x0f, key.as_ref(), value.as_ref()).await
  }

  pub async fn prependq(&mut self, key: impl AsRef<str>, value: impl AsRef<[u8]>) -> io::Result<()> {
    self
      .append_prepend_command_quiet(0x0e, key.as_ref(), value.as_ref())
      .await
  }

  async fn append_prepend_command(&mut self, op: u8, key: &str, value: &[u8]) -> Result<()> {
    self.write_append_prepend_command(op, key, value).await?;
    self.stream.flush().await?;
    self.read_empty_response().await
  }

  async fn append_prepend_command_quiet(&mut self, op: u8, key: &str, value: &[u8]) -> io::Result<()> {
    self.write_append_prepend_command(op, key, value).await?;
    self.stream.flush().await
  }

  pub async fn delete(&mut self, key: impl AsRef<str>) -> Result<()> {
    self.write_key_command(0x04, key.as_ref()).await?;
    self.stream.flush().await?;
    self.read_empty_response().await
  }

  pub async fn flush(&mut self) -> Result<()> {
    self.command(0x08).await
  }

  pub async fn quit(&mut self) -> Result<()> {
    self.command(0x07).await
  }

  async fn command(&mut self, op: u8) -> Result<()> {
    self.write_command(op).await?;
    self.stream.flush().await?;
    self.read_empty_response().await
  }

  async fn write_command(&mut self, op: u8) -> io::Result<()> {
    self
      .write_request_header(Header {
        op,
        ..Header::request()
      })
      .await
  }

  pub async fn write_incr_decr_command(
    &mut self,
    op: u8,
    key: &str,
    delta: u64,
    init: u64,
    exptime: u32,
  ) -> io::Result<()> {
    let key_len = key.len();
    let extras_len = 0;
    let body_len = key_len + extras_len;
    self
      .write_request_header(Header {
        op,
        key_len,
        extras_len,
        body_len,
        ..Header::request()
      })
      .await?;
    self.stream.write_all(key.as_bytes()).await?;
    self.stream.write_u64(delta).await?;
    self.stream.write_u64(init).await?;
    self.stream.write_u32(exptime).await?;
    Ok(())
  }

  async fn write_set_command(
    &mut self,
    op: u8,
    key: &str,
    value: &[u8],
    flags: u32,
    exptime: u32,
    _cas: Option<u64>,
  ) -> io::Result<()> {
    let key_len = key.len();
    let extras_len = 8;
    let body_len = key_len + extras_len + value.len();
    self
      .write_request_header(Header {
        op,
        key_len,
        extras_len,
        body_len,
        ..Header::request()
      })
      .await?;
    self.stream.write_u32(flags).await?;
    self.stream.write_u32(exptime).await?;
    self.stream.write_all(key.as_bytes()).await?;
    self.stream.write_all(value).await?;
    Ok(())
  }

  async fn write_append_prepend_command(&mut self, op: u8, key: &str, value: &[u8]) -> io::Result<()> {
    let key_len = key.len();
    let body_len = key_len + value.len();
    self
      .write_request_header(Header {
        op,
        key_len,
        body_len,
        ..Header::request()
      })
      .await?;
    self.stream.write_all(key.as_bytes()).await?;
    self.stream.write_all(value).await?;
    Ok(())
  }

  async fn write_key_command(&mut self, op: u8, key: &str) -> io::Result<()> {
    let key_len = key.len();
    let body_len = key_len;
    self
      .write_request_header(Header {
        op,
        key_len,
        body_len,
        ..Header::request()
      })
      .await?;
    self.stream.write_all(key.as_bytes()).await?;
    Ok(())
  }

  async fn write_touch_command(&mut self, op: u8, key: &str, exptime: u32) -> Result<()> {
    let key_len = key.len();
    let extras_len = 4;
    let body_len = key_len + extras_len;
    self
      .write_request_header(Header {
        op,
        key_len,
        extras_len,
        body_len,
        ..Header::request()
      })
      .await?;
    self.stream.write_u32(exptime).await?;
    self.stream.write_all(key.as_bytes()).await?;
    Ok(())
  }

  async fn write_request_header(&mut self, h: Header) -> io::Result<()> {
    self.stream.write_u8(h.magic).await?;
    self.stream.write_u8(h.op).await?;
    self.stream.write_u16(h.key_len.try_into().unwrap()).await?;
    self.stream.write_u8(h.extras_len.try_into().unwrap()).await?;
    self.stream.write_u8(h.data_type).await?;
    self.stream.write_u16(h.status).await?;
    self.stream.write_u32(h.body_len.try_into().unwrap()).await?;
    self.stream.write_u32(h.opaque).await?;
    self.stream.write_u64(h.cas).await?;
    Ok(())
  }

  async fn read_empty_response(&mut self) -> Result<()> {
    let (_header, body) = self.read_response().await?;
    assert!(body.is_empty());
    Ok(())
  }

  async fn read_response(&mut self) -> Result<(Header, Bytes)> {
    let mut buffer = BytesMut::with_capacity(24);
    self.stream.read_buf(&mut buffer).await?;

    let magic = buffer.get_u8();
    let op = buffer.get_u8();
    let key_len = buffer.get_u16();
    let key_len = key_len.into();
    let extras_len = buffer.get_u8();
    let extras_len = extras_len.into();
    let data_type = buffer.get_u8();
    let status = buffer.get_u16();
    let body_len = buffer.get_u32();
    let body_len = body_len.try_into().unwrap();
    let opaque = buffer.get_u32();
    let cas = buffer.get_u64();

    let mut buffer = BytesMut::with_capacity(body_len);
    if body_len > 0 {
      self.stream.read_buf(&mut buffer).await?;
    }

    if magic != 0x81 {
      return Err(io::Error::new(io::ErrorKind::InvalidData, "unexpected magic byte for response").into());
    }

    if data_type != 0x00 {
      return Err(io::Error::new(io::ErrorKind::InvalidData, "unexpected data_type byte for response").into());
    }

    match status {
      0x0000 => Ok((
        Header {
          magic,
          op,
          key_len,
          extras_len,
          data_type,
          status,
          body_len,
          opaque,
          cas,
        },
        buffer.freeze(),
      )),
      0x0001 => Err(Error::Server(ServerError::KeyNotFound)),
      0x0002 => Err(Error::Server(ServerError::KeyExists)),
      code => Err(Error::Server(ServerError::Unknown(code))),
    }
  }
}

#[derive(Debug, Default)]
pub struct Header {
  pub magic: u8,
  pub op: u8,
  pub key_len: usize,
  pub extras_len: usize,
  pub data_type: u8,
  pub status: u16,
  pub body_len: usize,
  pub opaque: u32,
  pub cas: u64,
}

impl Header {
  pub fn request() -> Self {
    Self {
      magic: 0x80,
      ..Default::default()
    }
  }

  pub fn response() -> Self {
    Self {
      magic: 0x81,
      ..Default::default()
    }
  }
}
