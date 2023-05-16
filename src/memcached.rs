use std::{
  collections::BTreeMap,
  io,
  net::{SocketAddrV4, SocketAddrV6},
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::{
  io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufStream},
  net::{self, TcpStream},
};
use url::Url;

pub struct BinaryConnection {
  stream: BufStream<TcpStream>,
}

impl BinaryConnection {
  pub async fn connect(url: Url) -> io::Result<Self> {
    assert_eq!("tcp", url.scheme()); // only support tcp for now

    let port = url.port().unwrap_or(11211);
    let addr = match url.host() {
      Some(url::Host::Domain(domain)) => net::lookup_host(format!("{}:{}", domain, port))
        .await
        .and_then(|mut v| {
          v.next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::ConnectionReset, "unable to find host"))
        })?,
      Some(url::Host::Ipv4(ip)) => SocketAddrV4::new(ip, port).into(),
      Some(url::Host::Ipv6(ip)) => SocketAddrV6::new(ip, port, 0, 0).into(),
      None => format!("[::]:{port}").parse().unwrap(),
    };

    let stream = TcpStream::connect(addr).await?;
    let stream = BufStream::new(stream);

    Ok(Self { stream })
  }

  pub async fn close(mut self) -> io::Result<()> {
    self.stream.shutdown().await
  }

  pub async fn get(&mut self, k: impl AsRef<str>) -> io::Result<()> {
    self
      .write_key_command(0x00, &KeyCommandRef { key: k.as_ref() })
      .await?;
    Ok(())
  }

  pub async fn getq(&mut self, k: impl AsRef<str>) -> io::Result<()> {
    self
      .write_key_command(0x09, &KeyCommandRef { key: k.as_ref() })
      .await?;
    Ok(())
  }

  pub async fn getk(&mut self, k: impl AsRef<str>) -> io::Result<()> {
    self
      .write_key_command(0x0c, &KeyCommandRef { key: k.as_ref() })
      .await?;
    Ok(())
  }

  pub async fn getkq(&mut self, k: impl AsRef<str>) -> io::Result<()> {
    self
      .write_key_command(0x0d, &KeyCommandRef { key: k.as_ref() })
      .await?;
    Ok(())
  }

  pub async fn gat(&mut self, k: impl AsRef<str>, exptime: u32) -> io::Result<()> {
    self
      .write_touch_command(
        0x1d,
        &TouchCommandRef {
          key: k.as_ref(),
          exptime,
        },
      )
      .await?;
    Ok(())
  }

  pub async fn gatq(&mut self, k: impl AsRef<str>, exptime: u32) -> io::Result<()> {
    self
      .write_touch_command(
        0x1e,
        &TouchCommandRef {
          key: k.as_ref(),
          exptime,
        },
      )
      .await?;
    Ok(())
  }

  pub async fn set(
    &mut self,
    k: impl AsRef<str>,
    v: impl AsRef<[u8]>,
    flags: u32,
    exptime: u32,
    cas: Option<u64>,
  ) -> io::Result<()> {
    self
      .write_set_command(
        0x01,
        &SetCommandRef {
          key: k.as_ref(),
          value: v.as_ref(),
          flags,
          exptime,
          cas,
        },
      )
      .await?;
    self.read_empty_response().await
  }

  pub async fn setq(
    &mut self,
    k: impl AsRef<str>,
    v: impl AsRef<[u8]>,
    flags: u32,
    exptime: u32,
    cas: Option<u64>,
  ) -> io::Result<()> {
    self
      .write_set_command(
        0x11,
        &SetCommandRef {
          key: k.as_ref(),
          value: v.as_ref(),
          flags,
          exptime,
          cas,
        },
      )
      .await
  }

  pub async fn add(
    &mut self,
    k: impl AsRef<str>,
    v: impl AsRef<[u8]>,
    flags: u32,
    exptime: u32,
    cas: Option<u64>,
  ) -> io::Result<()> {
    self
      .write_set_command(
        0x02,
        &SetCommandRef {
          key: k.as_ref(),
          value: v.as_ref(),
          flags,
          exptime,
          cas,
        },
      )
      .await?;
    self.read_empty_response().await
  }

  pub async fn addq(
    &mut self,
    k: impl AsRef<str>,
    v: impl AsRef<[u8]>,
    flags: u32,
    exptime: u32,
    cas: Option<u64>,
  ) -> io::Result<()> {
    self
      .write_set_command(
        0x12,
        &SetCommandRef {
          key: k.as_ref(),
          value: v.as_ref(),
          flags,
          exptime,
          cas,
        },
      )
      .await
  }

  pub async fn replace(
    &mut self,
    k: impl AsRef<str>,
    v: impl AsRef<[u8]>,
    flags: u32,
    exptime: u32,
    cas: Option<u64>,
  ) -> io::Result<()> {
    self
      .write_set_command(
        0x03,
        &SetCommandRef {
          key: k.as_ref(),
          value: v.as_ref(),
          flags,
          exptime,
          cas,
        },
      )
      .await?;
    self.read_empty_response().await
  }

  pub async fn replaceq(
    &mut self,
    k: impl AsRef<str>,
    v: impl AsRef<[u8]>,
    flags: u32,
    exptime: u32,
    cas: Option<u64>,
  ) -> io::Result<()> {
    self
      .write_set_command(
        0x13,
        &SetCommandRef {
          key: k.as_ref(),
          value: v.as_ref(),
          flags,
          exptime,
          cas,
        },
      )
      .await
  }

  pub async fn append(&mut self, k: impl AsRef<str>, v: impl AsRef<[u8]>) -> io::Result<()> {
    self
      .write_append_prepend_command(
        0x0e,
        &AppendPrependCommandRef {
          key: k.as_ref(),
          value: v.as_ref(),
        },
      )
      .await?;
    Ok(())
  }

  pub async fn appendq(&mut self, k: impl AsRef<str>, v: impl AsRef<[u8]>) -> io::Result<()> {
    self
      .write_append_prepend_command(
        0x19,
        &AppendPrependCommandRef {
          key: k.as_ref(),
          value: v.as_ref(),
        },
      )
      .await?;
    Ok(())
  }

  pub async fn prepend(&mut self, k: impl AsRef<str>, v: impl AsRef<[u8]>) -> io::Result<()> {
    self
      .write_append_prepend_command(
        0x0f,
        &AppendPrependCommandRef {
          key: k.as_ref(),
          value: v.as_ref(),
        },
      )
      .await?;
    Ok(())
  }

  pub async fn prependq(&mut self, k: impl AsRef<str>, v: impl AsRef<[u8]>) -> io::Result<()> {
    self
      .write_append_prepend_command(
        0x1a,
        &AppendPrependCommandRef {
          key: k.as_ref(),
          value: v.as_ref(),
        },
      )
      .await?;
    Ok(())
  }

  pub async fn delete(&mut self, k: impl AsRef<str>) -> io::Result<()> {
    self
      .write_key_command(0x04, &KeyCommandRef { key: k.as_ref() })
      .await?;
    Ok(())
  }

  pub async fn deleteq(&mut self, k: impl AsRef<str>) -> io::Result<()> {
    self
      .write_key_command(0x14, &KeyCommandRef { key: k.as_ref() })
      .await?;
    Ok(())
  }

  pub async fn incr(&mut self, k: impl AsRef<str>) -> io::Result<()> {
    self
      .write_incr_decr_command(
        0x05,
        &IncrDecrCommandRef {
          key: k.as_ref(),
          delta: 1,
          init: 0,
          exptime: 0,
        },
      )
      .await?;
    Ok(())
  }

  pub async fn incrq(&mut self, k: impl AsRef<str>) -> io::Result<()> {
    self
      .write_incr_decr_command(
        0x15,
        &IncrDecrCommandRef {
          key: k.as_ref(),
          delta: 1,
          init: 0,
          exptime: 0,
        },
      )
      .await?;
    Ok(())
  }

  pub async fn decr(&mut self, k: impl AsRef<str>) -> io::Result<()> {
    self
      .write_incr_decr_command(
        0x06,
        &IncrDecrCommandRef {
          key: k.as_ref(),
          delta: 1,
          init: 0,
          exptime: 0,
        },
      )
      .await?;
    Ok(())
  }

  pub async fn decrq(&mut self, k: impl AsRef<str>) -> io::Result<()> {
    self
      .write_incr_decr_command(
        0x16,
        &IncrDecrCommandRef {
          key: k.as_ref(),
          delta: 1,
          init: 0,
          exptime: 0,
        },
      )
      .await?;
    Ok(())
  }

  pub async fn touch(&mut self, k: impl AsRef<str>, exptime: u32) -> io::Result<()> {
    self
      .write_touch_command(
        0x1c,
        &TouchCommandRef {
          key: k.as_ref(),
          exptime,
        },
      )
      .await?;
    Ok(())
  }

  pub async fn flush(&mut self) -> io::Result<()> {
    self.write_simple_command(0x08).await?;
    self.read_response_header().await.map(|_h| ())
  }

  pub async fn flushq(&mut self) -> io::Result<()> {
    self.write_simple_command(0x18).await
  }

  pub async fn version(&mut self) -> io::Result<String> {
    self.write_simple_command(0x0b).await?;
    let header = self.read_response_header().await?;
    io_assert(header.key_len == 0, "key_len != 0")?;
    io_assert(header.extras_len == 0, "extras_len != 0")?;
    io_assert(header.body_len > 0, "body_len == 0")?;
    let mut version = Vec::with_capacity(header.body_len);
    self.stream.read_buf(&mut version).await?;
    String::from_utf8(version).map_err(|_err| io::Error::new(io::ErrorKind::InvalidData, "utf8 error"))
  }

  pub async fn stats(&mut self) -> io::Result<BTreeMap<String, Vec<u8>>> {
    self.write_simple_command(0x10).await?;
    let stats = BTreeMap::new();
    loop {
      let header = self.read_response_header().await?;
      if header.key_len == 0 && header.body_len == 0 {
        break;
      }
    }
    Ok(stats)
  }

  pub async fn quit(&mut self) -> io::Result<()> {
    self.write_simple_command(0x07).await?;
    self.read_empty_response().await
  }

  pub async fn quitq(&mut self) -> io::Result<()> {
    self.write_simple_command(0x17).await
  }

  pub async fn noop(&mut self) -> io::Result<()> {
    self.write_simple_command(0x0a).await?;
    self.read_empty_response().await
  }

  async fn write_request_header(&mut self, h: &Header) -> io::Result<()> {
    self.stream.write_u8(0x80).await?;
    self.stream.write_u8(h.op).await?;
    self.stream.write_u16(h.key_len.try_into().unwrap()).await?;
    self.stream.write_u8(h.extras_len.try_into().unwrap()).await?;
    self.stream.write_u8(0x00).await?;
    self.stream.write_u16(0x0000).await?;
    self.stream.write_u32(h.body_len.try_into().unwrap()).await?;
    self.stream.write_u32(h.opaque).await?;
    self.stream.write_u64(h.cas).await?;
    Ok(())
  }

  async fn write_key_command(&mut self, op: u8, cmd: &KeyCommandRef<'_>) -> io::Result<()> {
    let key_len = cmd.key.len();
    let body_len = key_len;
    self
      .write_request_header(&Header {
        op,
        key_len,
        body_len,
        ..Default::default()
      })
      .await?;
    self.stream.write(cmd.key.as_bytes()).await?;
    self.stream.flush().await
  }

  async fn write_simple_command(&mut self, op: u8) -> io::Result<()> {
    self
      .write_request_header(&Header {
        op,
        ..Default::default()
      })
      .await?;
    self.stream.flush().await
  }

  async fn write_set_command(&mut self, op: u8, cmd: &SetCommandRef<'_>) -> io::Result<()> {
    let key_len = cmd.key.len();
    let extras_len = 8;
    let value_len = cmd.value.len();
    let body_len = key_len + extras_len + value_len;
    let cas = cmd.cas.unwrap_or_default();
    self
      .write_request_header(&Header {
        op,
        key_len,
        extras_len,
        body_len,
        cas,
        ..Default::default()
      })
      .await?;
    self.stream.write_u32(cmd.flags).await?;
    self.stream.write_u32(cmd.exptime).await?;
    self.stream.write(cmd.key.as_bytes()).await?;
    self.stream.write(cmd.value.as_ref()).await?;
    self.stream.flush().await
  }

  async fn write_append_prepend_command(&mut self, op: u8, cmd: &AppendPrependCommandRef<'_>) -> io::Result<()> {
    let key_len = cmd.key.len();
    let value_len = cmd.value.len();
    let body_len = key_len + value_len;
    self
      .write_request_header(&Header {
        op,
        key_len,
        body_len,
        ..Default::default()
      })
      .await?;
    self.stream.write(cmd.key.as_bytes()).await?;
    self.stream.write(cmd.value.as_ref()).await?;
    self.stream.flush().await
  }

  async fn write_incr_decr_command(&mut self, op: u8, cmd: &IncrDecrCommandRef<'_>) -> io::Result<()> {
    let key_len = cmd.key.len();
    let extras_len = 20;
    let body_len = key_len + extras_len;
    self
      .write_request_header(&Header {
        op,
        key_len,
        extras_len,
        body_len,
        ..Default::default()
      })
      .await?;
    self.stream.write_u64(cmd.delta).await?;
    self.stream.write_u64(cmd.init).await?;
    self.stream.write_u32(cmd.exptime).await?;
    self.stream.write(cmd.key.as_bytes()).await?;
    self.stream.flush().await
  }

  async fn write_touch_command(&mut self, op: u8, cmd: &TouchCommandRef<'_>) -> io::Result<()> {
    let key_len = cmd.key.len();
    let extras_len = 4;
    let body_len = key_len + extras_len;
    self
      .write_request_header(&Header {
        op,
        key_len,
        extras_len,
        body_len,
        ..Default::default()
      })
      .await?;
    self.stream.write_u32(cmd.exptime).await?;
    self.stream.write(cmd.key.as_bytes()).await?;
    self.stream.flush().await
  }

  async fn read_response_header(&mut self) -> io::Result<Header> {
    let magic = self.stream.read_u8().await?;
    assert_eq!(0x81, magic);
    let op = self.stream.read_u8().await?;
    let key_len = self.stream.read_u16().await?;
    let key_len = key_len.into();
    let extras_len = self.stream.read_u8().await?;
    let extras_len = extras_len.into();
    let data_type = self.stream.read_u8().await?;
    let status = self.stream.read_u16().await?;
    let body_len = self.stream.read_u32().await?;
    let body_len = body_len.try_into().unwrap();
    let opaque = self.stream.read_u32().await?;
    let cas = self.stream.read_u64().await?;

    match status {
      0x0000 => Ok(Header {
        op,
        key_len,
        extras_len,
        data_type,
        status,
        body_len,
        opaque,
        cas,
      }),
      code => Err(io::Error::new(
        io::ErrorKind::Other,
        format!("failed with server error {code}"),
      )),
    }
  }

  async fn read_empty_response(&mut self) -> io::Result<()> {
    let header = self.read_response_header().await?;
    io_assert(header.key_len == 0, "key_len != 0")?;
    io_assert(header.extras_len == 0, "extras_len != 0")?;
    io_assert(header.body_len == 0, "body_len != 0")?;
    Ok(())
  }
}

fn io_assert(ok: bool, msg: &str) -> io::Result<()> {
  if ok {
    Ok(())
  } else {
    Err(io::Error::new(
      io::ErrorKind::InvalidData,
      format!("Failed assertion: {msg}"),
    ))
  }
}
// fn spawn_connection(max_idle: Duration, max_lifetime: Duration, url: Url) -> mpsc::Sender<Command> {
//   let (sender, mut receiver) = mpsc::channel(1);
//   tokio::task::spawn(async move {
//     loop {
//       match receiver.recv().await {
//         Some(msg) => {
//           let mut connection = Connection::connect(url.clone()).await.unwrap();
//           process_msg(&mut connection, msg).await;

//           let idle_deadline = tokio::time::sleep(max_idle);
//           tokio::pin!(idle_deadline);

//           let lifetime_deadline = tokio::time::sleep(max_lifetime);
//           tokio::pin!(lifetime_deadline);

//           loop {
//             tokio::select! {
//               _ = &mut idle_deadline => break,
//               _ = &mut lifetime_deadline => break,
//               msg = receiver.recv() => {
//                 idle_deadline.as_mut().reset(Instant::now() + max_idle);

//                 match msg {
//                   None => break,
//                   Some(msg) => process_msg(&mut connection, msg).await,
//                 }
//               }
//             }
//           }

//           connection.close().await.ok();
//         }
//         None => break,
//       }
//     }
//   });
//   sender
// }

pub struct Connection;

impl Connection {
  pub async fn connect(_url: Url) -> io::Result<Self> {
    todo!()
  }
}
// async fn process_msg(conn: &mut Connection, msg: Command) {}

#[derive(Debug, PartialEq)]
pub struct KeyCommandRef<'a> {
  key: &'a str,
}

#[derive(Debug, PartialEq)]
pub struct KeyCommand {
  key: String,
}

#[derive(Debug, PartialEq)]
pub struct TouchCommandRef<'a> {
  key: &'a str,
  exptime: u32,
}

#[derive(Debug, PartialEq)]
pub struct TouchCommand {
  key: String,
  exptime: u32,
}

#[derive(Debug, PartialEq)]
pub struct SetCommandRef<'a> {
  key: &'a str,
  value: &'a [u8],
  flags: u32,
  exptime: u32,
  cas: Option<u64>,
}

#[derive(Debug, PartialEq)]
pub struct SetCommand {
  key: String,
  value: Vec<u8>,
  flags: u32,
  exptime: u32,
  cas: Option<u64>,
}

#[derive(Debug, PartialEq)]
pub struct IncrDecrCommandRef<'a> {
  key: &'a str,
  delta: u64,
  init: u64,
  exptime: u32,
}

#[derive(Debug, PartialEq)]
pub struct IncrDecrCommand {
  key: String,
  delta: u64,
  init: u64,
  exptime: u32,
}

#[derive(Debug, PartialEq)]
pub struct AppendPrependCommandRef<'a> {
  key: &'a str,
  value: &'a [u8],
}

#[derive(Debug, PartialEq)]
pub struct AppendPrependCommand {
  key: String,
  value: Vec<u8>,
}

#[derive(Debug, PartialEq)]
pub enum CommandRef<'a> {
  Get(KeyCommandRef<'a>),
  GetQ(KeyCommandRef<'a>),
  GetK(KeyCommandRef<'a>),
  GetKQ(KeyCommandRef<'a>),
  GetAndTouch(TouchCommandRef<'a>),
  GetAndTouchQ(TouchCommandRef<'a>),
  Set(SetCommandRef<'a>),
  SetQ(SetCommandRef<'a>),
  Add(SetCommandRef<'a>),
  AddQ(SetCommandRef<'a>),
  Replace(SetCommandRef<'a>),
  ReplaceQ(SetCommandRef<'a>),
  Append(AppendPrependCommandRef<'a>),
  AppendQ(AppendPrependCommandRef<'a>),
  Prepend(AppendPrependCommandRef<'a>),
  PrependQ(AppendPrependCommandRef<'a>),
  Delete(KeyCommandRef<'a>),
  DeleteQ(KeyCommandRef<'a>),
  Incr(IncrDecrCommandRef<'a>),
  IncrQ(IncrDecrCommandRef<'a>),
  Decr(IncrDecrCommandRef<'a>),
  DecrQ(IncrDecrCommandRef<'a>),
  Touch(TouchCommandRef<'a>),
  TouchQ(TouchCommandRef<'a>),
  Flush,
  FlushQ,
  Version,
  Stats,
  Quit,
  QuitQ,
  Noop,
}

impl<'a> CommandRef<'a> {
  pub fn to_command(&self) -> Command {
    match self {
      CommandRef::Get(KeyCommandRef { key }) => Command::Get(KeyCommand { key: key.to_string() }),
      CommandRef::GetQ(KeyCommandRef { key }) => Command::GetQ(KeyCommand { key: key.to_string() }),
      CommandRef::GetK(_) => todo!(),
      CommandRef::GetKQ(_) => todo!(),
      CommandRef::GetAndTouch(_) => todo!(),
      CommandRef::GetAndTouchQ(_) => todo!(),
      CommandRef::Set(_) => todo!(),
      CommandRef::SetQ(_) => todo!(),
      CommandRef::Add(_) => todo!(),
      CommandRef::AddQ(_) => todo!(),
      CommandRef::Replace(_) => todo!(),
      CommandRef::ReplaceQ(_) => todo!(),
      CommandRef::Append(_) => todo!(),
      CommandRef::AppendQ(_) => todo!(),
      CommandRef::Prepend(_) => todo!(),
      CommandRef::PrependQ(_) => todo!(),
      CommandRef::Delete(_) => todo!(),
      CommandRef::DeleteQ(_) => todo!(),
      CommandRef::Incr(_) => todo!(),
      CommandRef::IncrQ(_) => todo!(),
      CommandRef::Decr(_) => todo!(),
      CommandRef::DecrQ(_) => todo!(),
      CommandRef::Touch(_) => todo!(),
      CommandRef::TouchQ(_) => todo!(),
      CommandRef::Flush => todo!(),
      CommandRef::FlushQ => todo!(),
      CommandRef::Version => todo!(),
      CommandRef::Stats => todo!(),
      CommandRef::Quit => todo!(),
      CommandRef::QuitQ => todo!(),
      CommandRef::Noop => todo!(),
    }
  }
}

#[derive(Debug, PartialEq)]
pub enum Command {
  Get(KeyCommand),
  GetQ(KeyCommand),
  GetK(KeyCommand),
  GetKQ(KeyCommand),
  GetAndTouch(TouchCommand),
  GetAndTouchQ(TouchCommand),
  Set(SetCommand),
  SetQ(SetCommand),
  Add(SetCommand),
  AddQ(SetCommand),
  Replace(SetCommand),
  ReplaceQ(SetCommand),
  Append(AppendPrependCommand),
  AppendQ(AppendPrependCommand),
  Prepend(AppendPrependCommand),
  PrependQ(AppendPrependCommand),
  Delete(KeyCommand),
  DeleteQ(KeyCommand),
  Incr(IncrDecrCommand),
  IncrQ(IncrDecrCommand),
  Decr(IncrDecrCommand),
  DecrQ(IncrDecrCommand),
  Touch(TouchCommand),
  TouchQ(TouchCommand),
  Flush,
  FlushQ,
  Version,
  Stats,
  Quit,
  QuitQ,
  Noop,
}

impl Command {
  pub fn to_command_ref(&self) -> CommandRef<'_> {
    match self {
      Command::Get(KeyCommand { key }) => CommandRef::Get(KeyCommandRef { key: key.as_str() }),
      Command::GetQ(_) => todo!(),
      Command::GetK(_) => todo!(),
      Command::GetKQ(_) => todo!(),
      Command::GetAndTouch(_) => todo!(),
      Command::GetAndTouchQ(_) => todo!(),
      Command::Set(_) => todo!(),
      Command::SetQ(_) => todo!(),
      Command::Add(_) => todo!(),
      Command::AddQ(_) => todo!(),
      Command::Replace(_) => todo!(),
      Command::ReplaceQ(_) => todo!(),
      Command::Append(_) => todo!(),
      Command::AppendQ(_) => todo!(),
      Command::Prepend(_) => todo!(),
      Command::PrependQ(_) => todo!(),
      Command::Delete(_) => todo!(),
      Command::DeleteQ(_) => todo!(),
      Command::Incr(_) => todo!(),
      Command::IncrQ(_) => todo!(),
      Command::Decr(_) => todo!(),
      Command::DecrQ(_) => todo!(),
      Command::Touch(_) => todo!(),
      Command::TouchQ(_) => todo!(),
      Command::Flush => todo!(),
      Command::FlushQ => todo!(),
      Command::Version => todo!(),
      Command::Stats => todo!(),
      Command::Quit => todo!(),
      Command::QuitQ => todo!(),
      Command::Noop => todo!(),
    }
  }
}

#[derive(Debug, PartialEq)]
pub enum DecodeCommandError {
  UnexpectedEof,
  InvalidFormat,
  InvalidCommand,
}

pub async fn read_command<R>(r: &mut R) -> io::Result<Option<Bytes>>
where
  R: AsyncBufRead + Unpin,
{
  let mut buffer = BytesMut::new();
  let b = r.fill_buf().await?;
  if b.is_empty() {
    return Ok(None);
  }

  buffer.extend_from_slice(b);

  let len = b.len();
  r.consume(len);

  Ok(Some(buffer.freeze()))
}

pub fn decode_command(input: &[u8]) -> Result<Vec<CommandRef>, DecodeCommandError> {
  match input.first() {
    Some(0x80) => decode_binary_command(input).map(|v| vec![v]),
    Some(_) => decode_text_command(input),
    None => Err(DecodeCommandError::UnexpectedEof),
  }
}

pub fn decode_text_command(input: &[u8]) -> Result<Vec<CommandRef>, DecodeCommandError> {
  let mut chunks = input.splitn(2, |v| *v == b' ');
  let command = chunks.next().ok_or(DecodeCommandError::UnexpectedEof)?;
  let input = chunks.next().unwrap_or(&[]);

  fn new_line_str(input: &[u8]) -> Result<&str, DecodeCommandError> {
    let input = std::str::from_utf8(input).map_err(|_| DecodeCommandError::InvalidFormat)?;
    let input = input
      .strip_suffix("\r\n")
      .or_else(|| input.strip_suffix('\n'))
      .ok_or(DecodeCommandError::UnexpectedEof)?;
    Ok(input)
  }

  match command {
    b"get" | b"gets" => {
      let input = new_line_str(input)?;
      Ok(
        input
          .split(' ')
          .map(Into::into)
          .map(|key| CommandRef::Get(KeyCommandRef { key }))
          .collect(),
      )
    }
    b"gat" | b"gats" => {
      let input = new_line_str(input)?;

      let mut chunks = input.split(' ');

      let exptime = chunks.next().ok_or(DecodeCommandError::UnexpectedEof)?;
      let exptime = exptime.parse().map_err(|_| DecodeCommandError::InvalidFormat)?;

      Ok(
        chunks
          .map(Into::into)
          .map(|key| CommandRef::GetAndTouch(TouchCommandRef { key, exptime }))
          .collect(),
      )
    }

    b"set" | b"add" | b"replace" | b"cas" | b"append" | b"prepend" => {
      let pos = input.iter().position(|v| *v == b'\n').unwrap_or(input.len());
      let (input, value) = (&input[..=pos], &input[pos + 1..]);

      let input = new_line_str(input)?;

      let mut chunks = input.split(' ');

      let key = chunks.next().ok_or(DecodeCommandError::UnexpectedEof)?;

      let flags = chunks.next().ok_or(DecodeCommandError::UnexpectedEof)?;
      let flags = flags.parse().map_err(|_| DecodeCommandError::InvalidFormat)?;

      let exptime = chunks.next().ok_or(DecodeCommandError::UnexpectedEof)?;
      let exptime = exptime.parse().map_err(|_| DecodeCommandError::InvalidFormat)?;

      let value_len = chunks.next().ok_or(DecodeCommandError::UnexpectedEof)?;
      let value_len = value_len
        .parse::<usize>()
        .map_err(|_| DecodeCommandError::InvalidFormat)?;

      let value = value
        .strip_suffix(b"\r\n")
        .or_else(|| value.strip_suffix(b"\n"))
        .ok_or(DecodeCommandError::UnexpectedEof)?;

      if value_len != value.len() {
        return Err(DecodeCommandError::InvalidFormat);
      }

      let mut cas = None;
      if let b"cas" = command {
        let cas_s = chunks.next().ok_or(DecodeCommandError::UnexpectedEof)?;
        let cas_s = cas_s.parse().map_err(|_| DecodeCommandError::InvalidFormat)?;
        cas = Some(cas_s);
      }

      let noreply = chunks.next().filter(|v| *v == "noreply").is_some();

      let cmd = match command {
        b"set" | b"cas" if noreply => CommandRef::SetQ(SetCommandRef {
          key,
          value,
          flags,
          exptime,
          cas,
        }),
        b"set" | b"cas" => CommandRef::Set(SetCommandRef {
          key,
          value,
          flags,
          exptime,
          cas,
        }),
        b"add" if noreply => CommandRef::AddQ(SetCommandRef {
          key,
          value,
          flags,
          exptime,
          cas,
        }),
        b"add" => CommandRef::Add(SetCommandRef {
          key,
          value,
          flags,
          exptime,
          cas,
        }),
        b"replace" if noreply => CommandRef::ReplaceQ(SetCommandRef {
          key,
          value,
          flags,
          exptime,
          cas,
        }),
        b"replace" => CommandRef::Replace(SetCommandRef {
          key,
          value,
          flags,
          exptime,
          cas,
        }),
        b"append" if noreply => CommandRef::AppendQ(AppendPrependCommandRef { key, value }),
        b"append" => CommandRef::Append(AppendPrependCommandRef { key, value }),
        b"prepend" if noreply => CommandRef::PrependQ(AppendPrependCommandRef { key, value }),
        b"prepend" => CommandRef::Prepend(AppendPrependCommandRef { key, value }),
        _ => unreachable!(),
      };

      Ok(vec![cmd])
    }

    b"delete" => {
      let input = new_line_str(input)?;

      let mut chunks = input.split(' ');

      let key = chunks.next().ok_or(DecodeCommandError::UnexpectedEof)?;
      let key = key;

      let noreply = chunks.next().filter(|v| *v == "noreply").is_some();
      if noreply {
        Ok(vec![CommandRef::DeleteQ(KeyCommandRef { key })])
      } else {
        Ok(vec![CommandRef::Delete(KeyCommandRef { key })])
      }
    }
    b"incr" | b"decr" => {
      let input = new_line_str(input)?;

      let mut chunks = input.split(' ');

      let key = chunks.next().ok_or(DecodeCommandError::UnexpectedEof)?;
      let key = key;

      let delta = chunks.next().ok_or(DecodeCommandError::UnexpectedEof)?;
      let delta = delta.parse().map_err(|_| DecodeCommandError::InvalidFormat)?;

      let init = 0;
      let exptime = 0;

      let noreply = chunks.next().filter(|v| *v == "noreply").is_some();

      let cmd = IncrDecrCommandRef {
        key,
        delta,
        init,
        exptime,
      };

      match command {
        b"incr" if noreply => Ok(vec![CommandRef::IncrQ(cmd)]),
        b"incr" => Ok(vec![CommandRef::Incr(cmd)]),
        b"decr" if noreply => Ok(vec![CommandRef::DecrQ(cmd)]),
        b"decr" => Ok(vec![CommandRef::Decr(cmd)]),
        _ => unreachable!(),
      }
    }
    b"touch" => {
      let input = new_line_str(input)?;

      let mut chunks = input.split(' ');

      let key = chunks.next().ok_or(DecodeCommandError::UnexpectedEof)?;
      let key = key;

      let exptime = chunks.next().ok_or(DecodeCommandError::UnexpectedEof)?;
      let exptime = exptime.parse().map_err(|_| DecodeCommandError::InvalidFormat)?;

      let noreply = chunks.next().filter(|v| *v == "noreply").is_some();

      if noreply {
        Ok(vec![CommandRef::TouchQ(TouchCommandRef { key, exptime })])
      } else {
        Ok(vec![CommandRef::Touch(TouchCommandRef { key, exptime })])
      }
    }
    b"flush_all\r\n" | b"flush_all\n" => Ok(vec![CommandRef::Flush]),
    b"version\r\n" | b"version\n" => Ok(vec![CommandRef::Version]),
    b"stats\r\n" | b"stats\n" => Ok(vec![CommandRef::Stats]),
    b"quit\r\n" | b"quit\n" => Ok(vec![CommandRef::Quit]),
    _ => Err(DecodeCommandError::InvalidCommand),
  }
}

pub fn decode_binary_command(mut input: &[u8]) -> Result<CommandRef, DecodeCommandError> {
  if input.len() < 24 {
    return Err(DecodeCommandError::UnexpectedEof);
  }

  let magic = input.get_u8();
  assert_eq!(0x80, magic);
  let op = input.get_u8();
  let key_len: usize = input.get_u16().into();
  let extras_len: usize = input.get_u8().into();
  let data_type = input.get_u8();
  assert_eq!(0x00, data_type);
  let _vbucket_id = input.get_u16();
  let body_len: usize = input.get_u32().try_into().unwrap();
  let opaque = input.get_u32();
  let cas = input.get_u64();

  let header = Header {
    op,
    key_len,
    extras_len,
    body_len,
    opaque,
    cas,
    ..Default::default()
  };

  if input.len() != body_len {
    return Err(DecodeCommandError::UnexpectedEof);
  }

  fn decode_key_command<'a>(header: &Header, mut input: &'a [u8]) -> Result<KeyCommandRef<'a>, DecodeCommandError> {
    let key_len = header.key_len;
    let extras_len = header.extras_len;
    let body_len = header.body_len;
    assert_eq!(0, extras_len);
    assert!(key_len > 0);
    assert_eq!(key_len, body_len);

    let key = input.get(0..key_len).ok_or(DecodeCommandError::UnexpectedEof)?;
    let key = std::str::from_utf8(key).map_err(|_| DecodeCommandError::InvalidFormat)?;
    let key = key;

    input.advance(key_len);
    assert!(input.is_empty());

    Ok(KeyCommandRef { key })
  }

  fn decode_set_command<'a>(header: &Header, mut input: &'a [u8]) -> Result<SetCommandRef<'a>, DecodeCommandError> {
    let key_len = header.key_len;
    let extras_len = header.extras_len;
    let body_len = header.body_len;
    assert_eq!(8, extras_len);
    assert!(key_len > 0);
    assert!(body_len >= key_len + extras_len);

    let flags = input.get_u32();
    let exptime = input.get_u32();

    let value_len = body_len - key_len - extras_len;

    let key = input.get(0..key_len).ok_or(DecodeCommandError::UnexpectedEof)?;
    let key = std::str::from_utf8(key).map_err(|_| DecodeCommandError::InvalidFormat)?;
    let key = key;
    input.advance(key_len);

    let value = input.get(0..value_len).ok_or(DecodeCommandError::UnexpectedEof)?;
    let value = value;
    input.advance(value_len);

    assert!(input.is_empty());

    let cas = None;
    Ok(SetCommandRef {
      key,
      value,
      flags,
      exptime,
      cas,
    })
  }

  fn decode_append_prepend_command<'a>(
    header: &Header,
    mut input: &'a [u8],
  ) -> Result<AppendPrependCommandRef<'a>, DecodeCommandError> {
    let key_len = header.key_len;
    let extras_len = header.extras_len;
    let body_len = header.body_len;
    assert_eq!(0, extras_len);
    assert!(key_len > 0);
    assert!(body_len >= key_len);

    let key = input.get(0..key_len).ok_or(DecodeCommandError::UnexpectedEof)?;
    let key = std::str::from_utf8(key).map_err(|_| DecodeCommandError::InvalidFormat)?;
    let key = key;
    input.advance(key_len);

    let value_len = body_len - key_len;
    let value = input.get(0..value_len).ok_or(DecodeCommandError::UnexpectedEof)?;
    let value = value;
    input.advance(value_len);

    assert!(input.is_empty());

    Ok(AppendPrependCommandRef { key, value })
  }

  fn decode_incr_decr_command<'a>(
    header: &Header,
    mut input: &'a [u8],
  ) -> Result<IncrDecrCommandRef<'a>, DecodeCommandError> {
    let key_len = header.key_len;
    let extras_len = header.extras_len;
    let body_len = header.body_len;
    assert_eq!(20, extras_len);
    assert!(key_len > 0);
    assert_eq!(body_len, key_len + extras_len);

    let delta = input.get_u64();
    let init = input.get_u64();
    let exptime = input.get_u32();

    let key = input.get(0..key_len).ok_or(DecodeCommandError::UnexpectedEof)?;
    let key = std::str::from_utf8(key).map_err(|_| DecodeCommandError::InvalidFormat)?;
    let key = key;

    input.advance(key_len);
    assert!(input.is_empty());

    Ok(IncrDecrCommandRef {
      key,
      delta,
      init,
      exptime,
    })
  }

  fn decode_touch_command<'a>(header: &Header, mut input: &'a [u8]) -> Result<TouchCommandRef<'a>, DecodeCommandError> {
    let key_len = header.key_len;
    let extras_len = header.extras_len;
    let body_len = header.body_len;
    assert_eq!(4, extras_len);
    assert!(key_len > 0);
    assert_eq!(body_len, key_len + extras_len);

    let exptime = input.get_u32();

    let key = input.get(0..key_len).ok_or(DecodeCommandError::UnexpectedEof)?;
    let key = std::str::from_utf8(key).map_err(|_| DecodeCommandError::InvalidFormat)?;
    let key = key;

    input.advance(key_len);
    assert!(input.is_empty());

    Ok(TouchCommandRef { key, exptime })
  }

  match op {
    0x00 => decode_key_command(&header, input).map(CommandRef::Get),
    0x09 => decode_key_command(&header, input).map(CommandRef::GetQ),
    0x0c => decode_key_command(&header, input).map(CommandRef::GetK),
    0x0d => decode_key_command(&header, input).map(CommandRef::GetKQ),
    0x04 => decode_key_command(&header, input).map(CommandRef::Delete),
    0x14 => decode_key_command(&header, input).map(CommandRef::DeleteQ),
    0x01 => decode_set_command(&header, input).map(CommandRef::Set),
    0x11 => decode_set_command(&header, input).map(CommandRef::SetQ),
    0x02 => decode_set_command(&header, input).map(CommandRef::Add),
    0x12 => decode_set_command(&header, input).map(CommandRef::AddQ),
    0x03 => decode_set_command(&header, input).map(CommandRef::Replace),
    0x13 => decode_set_command(&header, input).map(CommandRef::ReplaceQ),
    0x0e => decode_append_prepend_command(&header, input).map(CommandRef::Append),
    0x19 => decode_append_prepend_command(&header, input).map(CommandRef::AppendQ),
    0x0f => decode_append_prepend_command(&header, input).map(CommandRef::Prepend),
    0x1a => decode_append_prepend_command(&header, input).map(CommandRef::PrependQ),
    0x05 => decode_incr_decr_command(&header, input).map(CommandRef::Incr),
    0x15 => decode_incr_decr_command(&header, input).map(CommandRef::IncrQ),
    0x06 => decode_incr_decr_command(&header, input).map(CommandRef::Decr),
    0x16 => decode_incr_decr_command(&header, input).map(CommandRef::DecrQ),
    0x1c => decode_touch_command(&header, input).map(CommandRef::Touch),
    0x1d => decode_touch_command(&header, input).map(CommandRef::GetAndTouch),
    0x1e => decode_touch_command(&header, input).map(CommandRef::GetAndTouchQ),
    0x07 => {
      assert_eq!(0, extras_len);
      assert_eq!(0, key_len);
      assert_eq!(0, body_len);
      Ok(CommandRef::Quit)
    }
    0x17 => {
      assert_eq!(0, extras_len);
      assert_eq!(0, key_len);
      assert_eq!(0, body_len);
      Ok(CommandRef::QuitQ)
    }
    0x08 => {
      assert_eq!(0, extras_len);
      assert_eq!(0, key_len);
      assert_eq!(0, body_len);
      Ok(CommandRef::Flush)
    }
    0x18 => {
      assert_eq!(0, extras_len);
      assert_eq!(0, key_len);
      assert_eq!(0, body_len);
      Ok(CommandRef::FlushQ)
    }
    0x0a => {
      assert_eq!(0, extras_len);
      assert_eq!(0, key_len);
      assert_eq!(0, body_len);
      Ok(CommandRef::Noop)
    }
    0x0b => {
      assert_eq!(0, extras_len);
      assert_eq!(0, key_len);
      assert_eq!(0, body_len);
      Ok(CommandRef::Version)
    }
    0x10 => {
      assert_eq!(0, extras_len);
      assert_eq!(0, key_len);
      assert_eq!(0, body_len);
      Ok(CommandRef::Stats)
    }
    _ => Err(DecodeCommandError::InvalidCommand),
  }
}

#[derive(Debug, PartialEq)]
pub enum EncodeCommandError {
  NotSupported,
}

pub fn encode_binary_command(command: &CommandRef) -> Result<Vec<u8>, EncodeCommandError> {
  fn put_request_header(buffer: &mut Vec<u8>, h: &Header) {
    buffer.put_u8(0x80);
    buffer.put_u8(h.op);
    buffer.put_u16(h.key_len.try_into().unwrap());
    buffer.put_u8(h.extras_len.try_into().unwrap());
    buffer.put_u8(h.data_type);
    buffer.put_u16(h.status);
    buffer.put_u32(h.body_len.try_into().unwrap());
    buffer.put_u32(h.opaque);
    buffer.put_u64(h.cas);
  }

  fn encode_key_command(op: u8, cmd: &KeyCommandRef) -> Vec<u8> {
    let mut buffer = Vec::new();
    let key_len = cmd.key.len();
    let body_len = key_len;
    put_request_header(
      &mut buffer,
      &Header {
        op,
        key_len,
        body_len,
        ..Default::default()
      },
    );
    buffer.put(cmd.key.as_bytes());
    buffer
  }

  fn encode_simple_command(op: u8) -> Vec<u8> {
    let mut buffer = Vec::new();
    put_request_header(
      &mut buffer,
      &Header {
        op,
        ..Default::default()
      },
    );
    buffer
  }

  fn encode_set_command(op: u8, cmd: &SetCommandRef) -> Vec<u8> {
    let mut buffer = Vec::new();
    let key_len = cmd.key.len();
    let extras_len = 8;
    let value_len = cmd.value.len();
    let body_len = key_len + extras_len + value_len;
    let cas = cmd.cas.unwrap_or_default();
    put_request_header(
      &mut buffer,
      &Header {
        op,
        key_len,
        extras_len,
        body_len,
        cas,
        ..Default::default()
      },
    );
    buffer.put_u32(cmd.flags);
    buffer.put_u32(cmd.exptime);
    buffer.put(cmd.key.as_bytes());
    buffer.put(cmd.value.as_ref());
    buffer
  }

  fn encode_append_prepend_command(op: u8, cmd: &AppendPrependCommandRef) -> Vec<u8> {
    let mut buffer = Vec::new();
    let key_len = cmd.key.len();
    let value_len = cmd.value.len();
    let body_len = key_len + value_len;
    put_request_header(
      &mut buffer,
      &Header {
        op,
        key_len,
        body_len,
        ..Default::default()
      },
    );
    buffer.put(cmd.key.as_bytes());
    buffer.put(cmd.value.as_ref());
    buffer
  }

  fn encode_incr_decr_command(op: u8, cmd: &IncrDecrCommandRef) -> Vec<u8> {
    let mut buffer = Vec::new();
    let key_len = cmd.key.len();
    let extras_len = 20;
    let body_len = key_len + extras_len;
    put_request_header(
      &mut buffer,
      &Header {
        op,
        key_len,
        extras_len,
        body_len,
        ..Default::default()
      },
    );
    buffer.put_u64(cmd.delta);
    buffer.put_u64(cmd.init);
    buffer.put_u32(cmd.exptime);
    buffer.put(cmd.key.as_bytes());
    buffer
  }

  fn encode_touch_command(op: u8, cmd: &TouchCommandRef) -> Vec<u8> {
    let mut buffer = Vec::new();
    let key_len = cmd.key.len();
    let extras_len = 4;
    let body_len = key_len + extras_len;
    put_request_header(
      &mut buffer,
      &Header {
        op,
        key_len,
        extras_len,
        body_len,
        ..Default::default()
      },
    );
    buffer.put_u32(cmd.exptime);
    buffer.put(cmd.key.as_bytes());
    buffer
  }

  match command {
    CommandRef::GetQ(cmd) => Ok(encode_key_command(0x09, cmd)),
    CommandRef::Get(cmd) => Ok(encode_key_command(0x00, cmd)),
    CommandRef::GetKQ(cmd) => Ok(encode_key_command(0x0d, cmd)),
    CommandRef::GetK(cmd) => Ok(encode_key_command(0x0c, cmd)),
    CommandRef::GetAndTouchQ(cmd) => Ok(encode_touch_command(0x1e, cmd)),
    CommandRef::GetAndTouch(cmd) => Ok(encode_touch_command(0x1d, cmd)),
    CommandRef::Set(cmd) => Ok(encode_set_command(0x01, cmd)),
    CommandRef::SetQ(cmd) => Ok(encode_set_command(0x11, cmd)),
    CommandRef::Add(cmd) => Ok(encode_set_command(0x02, cmd)),
    CommandRef::AddQ(cmd) => Ok(encode_set_command(0x12, cmd)),
    CommandRef::Replace(cmd) => Ok(encode_set_command(0x03, cmd)),
    CommandRef::ReplaceQ(cmd) => Ok(encode_set_command(0x13, cmd)),
    CommandRef::Append(cmd) => Ok(encode_append_prepend_command(0x0e, cmd)),
    CommandRef::AppendQ(cmd) => Ok(encode_append_prepend_command(0x19, cmd)),
    CommandRef::Prepend(cmd) => Ok(encode_append_prepend_command(0x0f, cmd)),
    CommandRef::PrependQ(cmd) => Ok(encode_append_prepend_command(0x1a, cmd)),
    CommandRef::DeleteQ(cmd) => Ok(encode_key_command(0x14, cmd)),
    CommandRef::Delete(cmd) => Ok(encode_key_command(0x04, cmd)),
    CommandRef::IncrQ(cmd) => Ok(encode_incr_decr_command(0x15, cmd)),
    CommandRef::Incr(cmd) => Ok(encode_incr_decr_command(0x05, cmd)),
    CommandRef::DecrQ(cmd) => Ok(encode_incr_decr_command(0x16, cmd)),
    CommandRef::Decr(cmd) => Ok(encode_incr_decr_command(0x06, cmd)),
    CommandRef::TouchQ(_) => Err(EncodeCommandError::NotSupported),
    CommandRef::Touch(cmd) => Ok(encode_touch_command(0x1c, cmd)),
    CommandRef::FlushQ => Ok(encode_simple_command(0x18)),
    CommandRef::Flush => Ok(encode_simple_command(0x08)),
    CommandRef::Version => Ok(encode_simple_command(0x0b)),
    CommandRef::Stats => Ok(encode_simple_command(0x10)),
    CommandRef::QuitQ => Ok(encode_simple_command(0x17)),
    CommandRef::Quit => Ok(encode_simple_command(0x07)),
    CommandRef::Noop => Ok(encode_simple_command(0x0a)),
  }
}

pub fn encode_text_command(command: &CommandRef) -> Result<Vec<u8>, EncodeCommandError> {
  fn encode_set_command(op: &str, noreply: bool, cmd: &SetCommandRef) -> Result<Vec<u8>, EncodeCommandError> {
    let mut buffer = match (op, cmd.cas) {
      ("set", Some(cas)) => Ok(
        format!(
          "cas {} {} {} {} {}",
          cmd.key,
          cmd.flags,
          cmd.exptime,
          cmd.value.len(),
          cas,
        )
        .into_bytes(),
      ),
      (_, Some(_)) => Err(EncodeCommandError::NotSupported),
      (op, None) => Ok(format!("{} {} {} {} {}", op, cmd.key, cmd.flags, cmd.exptime, cmd.value.len()).into_bytes()),
    }?;
    if noreply {
      buffer.extend_from_slice(b" noreply");
    }
    buffer.extend_from_slice(b"\r\n");
    buffer.extend_from_slice(cmd.value.as_ref());
    buffer.extend_from_slice(b"\r\n");
    Ok(buffer)
  }

  fn encode_append_prepend_command(op: &str, noreply: bool, cmd: &AppendPrependCommandRef) -> Vec<u8> {
    let mut buffer = format!("{} {} 0 0 {}", op, cmd.key, cmd.value.len()).into_bytes();
    if noreply {
      buffer.extend_from_slice(b" noreply");
    }
    buffer.extend_from_slice(b"\r\n");
    buffer.extend_from_slice(cmd.value.as_ref());
    buffer.extend_from_slice(b"\r\n");
    buffer
  }

  match command {
    CommandRef::GetQ(_) => Err(EncodeCommandError::NotSupported),
    CommandRef::Get(KeyCommandRef { key }) => Ok(format!("get {key}\r\n").into_bytes()),
    CommandRef::GetKQ(_) => Err(EncodeCommandError::NotSupported),
    CommandRef::GetK(_) => Err(EncodeCommandError::NotSupported),
    CommandRef::GetAndTouchQ(_) => Err(EncodeCommandError::NotSupported),
    CommandRef::GetAndTouch(TouchCommandRef { key, exptime }) => Ok(format!("gat {exptime} {key}\r\n").into_bytes()),
    CommandRef::Set(cmd) => encode_set_command("set", false, cmd),
    CommandRef::SetQ(cmd) => encode_set_command("set", true, cmd),
    CommandRef::Add(cmd) => encode_set_command("add", false, cmd),
    CommandRef::AddQ(cmd) => encode_set_command("add", true, cmd),
    CommandRef::Replace(cmd) => encode_set_command("replace", false, cmd),
    CommandRef::ReplaceQ(cmd) => encode_set_command("replace", true, cmd),
    CommandRef::Append(cmd) => Ok(encode_append_prepend_command("append", false, cmd)),
    CommandRef::AppendQ(cmd) => Ok(encode_append_prepend_command("append", true, cmd)),
    CommandRef::Prepend(cmd) => Ok(encode_append_prepend_command("prepend", false, cmd)),
    CommandRef::PrependQ(cmd) => Ok(encode_append_prepend_command("prepend", true, cmd)),
    CommandRef::DeleteQ(KeyCommandRef { key }) => Ok(format!("delete {key} noreply\r\n").into_bytes()),
    CommandRef::Delete(KeyCommandRef { key }) => Ok(format!("delete {key}\r\n").into_bytes()),
    CommandRef::IncrQ(IncrDecrCommandRef { key, delta, .. }) => {
      Ok(format!("incr {key} {delta} noreply\r\n").into_bytes())
    }
    CommandRef::Incr(IncrDecrCommandRef { key, delta, .. }) => Ok(format!("incr {key} {delta}\r\n").into_bytes()),
    CommandRef::DecrQ(IncrDecrCommandRef { key, delta, .. }) => {
      Ok(format!("decr {key} {delta} noreply\r\n").into_bytes())
    }
    CommandRef::Decr(IncrDecrCommandRef { key, delta, .. }) => Ok(format!("decr {key} {delta}\r\n").into_bytes()),
    CommandRef::TouchQ(TouchCommandRef { key, exptime }) => {
      Ok(format!("touch {key} {exptime} noreply\r\n").into_bytes())
    }
    CommandRef::Touch(TouchCommandRef { key, exptime }) => Ok(format!("touch {key} {exptime}\r\n").into_bytes()),
    CommandRef::FlushQ => Err(EncodeCommandError::NotSupported),
    CommandRef::Flush => Ok("flush_all\r\n".into()),
    CommandRef::Version => Ok("version\r\n".into()),
    CommandRef::Stats => Ok("stats\r\n".into()),
    CommandRef::QuitQ => Err(EncodeCommandError::NotSupported),
    CommandRef::Quit => Ok("quit\r\n".into()),
    CommandRef::Noop => Err(EncodeCommandError::NotSupported),
  }
}

#[derive(Debug, Default)]
struct Header {
  op: u8,
  key_len: usize,
  extras_len: usize,
  data_type: u8,
  status: u16,
  body_len: usize,
  opaque: u32,
  cas: u64,
}

#[cfg(test)]
mod tests {

  

  use super::{
    decode_binary_command, decode_text_command, encode_binary_command, encode_text_command, AppendPrependCommandRef,
    CommandRef, DecodeCommandError, IncrDecrCommandRef, KeyCommandRef, SetCommandRef, TouchCommandRef,
  };

  #[test]
  fn test_decode_text_command() {
    let tests: &[(&[u8], _)] = &[
      (
        b"get foo\r\n",
        Ok(vec![CommandRef::Get(KeyCommandRef { key: "foo" })]),
      ),
      (
        b"get foo bar\r\n",
        Ok(vec![
          CommandRef::Get(KeyCommandRef { key: "foo" }),
          CommandRef::Get(KeyCommandRef { key: "bar" }),
        ]),
      ),
      (
        b"gets foo\r\n",
        Ok(vec![CommandRef::Get(KeyCommandRef { key: "foo" })]),
      ),
      (
        b"gets foo bar\r\n",
        Ok(vec![
          CommandRef::Get(KeyCommandRef { key: "foo" }),
          CommandRef::Get(KeyCommandRef { key: "bar" }),
        ]),
      ),
      (
        b"touch foo 123\r\n",
        Ok(vec![CommandRef::Touch(TouchCommandRef {
          key: "foo",
          exptime: 123,
        })]),
      ),
      (
        b"touch foo 123 noreply\r\n",
        Ok(vec![CommandRef::TouchQ(TouchCommandRef {
          key: "foo",
          exptime: 123,
        })]),
      ),
      (
        b"incr foo 2\r\n",
        Ok(vec![CommandRef::Incr(IncrDecrCommandRef {
          key: "foo",
          delta: 2,
          init: 0,
          exptime: 0,
        })]),
      ),
      (
        b"incr foo 2 noreply\r\n",
        Ok(vec![CommandRef::IncrQ(IncrDecrCommandRef {
          key: "foo",
          delta: 2,
          init: 0,
          exptime: 0,
        })]),
      ),
      (
        b"decr foo 2\r\n",
        Ok(vec![CommandRef::Decr(IncrDecrCommandRef {
          key: "foo",
          delta: 2,
          init: 0,
          exptime: 0,
        })]),
      ),
      (
        b"decr foo 2 noreply\r\n",
        Ok(vec![CommandRef::DecrQ(IncrDecrCommandRef {
          key: "foo",
          delta: 2,
          init: 0,
          exptime: 0,
        })]),
      ),
      (
        b"delete foo\r\n",
        Ok(vec![CommandRef::Delete(KeyCommandRef { key: "foo" })]),
      ),
      (
        b"delete foo noreply\r\n",
        Ok(vec![CommandRef::DeleteQ(KeyCommandRef { key: "foo" })]),
      ),
      (
        b"set foo 123 321 3\r\nbar\r\n",
        Ok(vec![CommandRef::Set(SetCommandRef {
          key: "foo",
          value: b"bar",
          flags: 123,
          exptime: 321,
          cas: None,
        })]),
      ),
      (
        b"set foo 123 321 0\r\n\r\n",
        Ok(vec![CommandRef::Set(SetCommandRef {
          key: "foo",
          value: b"",
          flags: 123,
          exptime: 321,
          cas: None,
        })]),
      ),
      (
        b"set foo 123 321 3 noreply\r\nbar\r\n",
        Ok(vec![CommandRef::SetQ(SetCommandRef {
          key: "foo",
          value: b"bar",
          flags: 123,
          exptime: 321,
          cas: None,
        })]),
      ),
      (b"set foo 123 321 1\r\nbar\r\n", Err(DecodeCommandError::InvalidFormat)),
      (b"set foo 123 321 1\r\nb", Err(DecodeCommandError::UnexpectedEof)),
      (b"set foo 123 321\r\nb\r\n", Err(DecodeCommandError::UnexpectedEof)),
      (
        b"add foo 123 321 3\r\nbar\r\n",
        Ok(vec![CommandRef::Add(SetCommandRef {
          key: "foo",
          value: b"bar",
          flags: 123,
          exptime: 321,
          cas: None,
        })]),
      ),
      (
        b"add foo 123 321 3 noreply\r\nbar\r\n",
        Ok(vec![CommandRef::AddQ(SetCommandRef {
          key: "foo",
          value: b"bar",
          flags: 123,
          exptime: 321,
          cas: None,
        })]),
      ),
      (
        b"replace foo 123 321 3\r\nbar\r\n",
        Ok(vec![CommandRef::Replace(SetCommandRef {
          key: "foo",
          value: b"bar",
          flags: 123,
          exptime: 321,
          cas: None,
        })]),
      ),
      (
        b"replace foo 123 321 3 noreply\r\nbar\r\n",
        Ok(vec![CommandRef::ReplaceQ(SetCommandRef {
          key: "foo",
          value: b"bar",
          flags: 123,
          exptime: 321,
          cas: None,
        })]),
      ),
      (
        b"append foo 123 321 3\r\nbar\r\n",
        Ok(vec![CommandRef::Append(AppendPrependCommandRef {
          key: "foo",
          value: b"bar",
        })]),
      ),
      (
        b"append foo 123 321 3 noreply\r\nbar\r\n",
        Ok(vec![CommandRef::AppendQ(AppendPrependCommandRef {
          key: "foo",
          value: b"bar",
        })]),
      ),
      (
        b"prepend foo 123 321 3\r\nbar\r\n",
        Ok(vec![CommandRef::Prepend(AppendPrependCommandRef {
          key: "foo",
          value: b"bar",
        })]),
      ),
      (
        b"prepend foo 123 321 3 noreply\r\nbar\r\n",
        Ok(vec![CommandRef::PrependQ(AppendPrependCommandRef {
          key: "foo",
          value: b"bar",
        })]),
      ),
      (
        b"cas foo 123 321 3 567\r\nbar\r\n",
        Ok(vec![CommandRef::Set(SetCommandRef {
          key: "foo",
          value: b"bar",
          flags: 123,
          exptime: 321,
          cas: Some(567),
        })]),
      ),
      (
        b"cas foo 123 321 0 567\r\n\r\n",
        Ok(vec![CommandRef::Set(SetCommandRef {
          key: "foo",
          value: b"",
          flags: 123,
          exptime: 321,
          cas: Some(567),
        })]),
      ),
      (
        b"cas foo 123 321 3 567 noreply\r\nbar\r\n",
        Ok(vec![CommandRef::SetQ(SetCommandRef {
          key: "foo",
          value: b"bar",
          flags: 123,
          exptime: 321,
          cas: Some(567),
        })]),
      ),
      (b"flush_all\r\n", Ok(vec![CommandRef::Flush])),
      (b"version\r\n", Ok(vec![CommandRef::Version])),
      (b"stats\r\n", Ok(vec![CommandRef::Stats])),
      (b"quit\r\n", Ok(vec![CommandRef::Quit])),
    ];

    for t in tests {
      println!("{:?}", std::str::from_utf8(t.0));
      assert_eq!(t.1, decode_text_command(t.0));
    }
  }

  #[test]
  fn test_text_command_roundtrip() {
    let tests = &[
      CommandRef::Get(KeyCommandRef { key: "foo" }),
      CommandRef::GetAndTouch(TouchCommandRef {
        key: "foo",
        exptime: 123,
      }),
      CommandRef::Set(SetCommandRef {
        key: "foo",
        value: b"bar",
        flags: 123,
        exptime: 321,
        cas: None,
      }),
      CommandRef::SetQ(SetCommandRef {
        key: "foo",
        value: b"bar",
        flags: 123,
        exptime: 321,
        cas: None,
      }),
      CommandRef::Add(SetCommandRef {
        key: "foo",
        value: b"bar",
        flags: 123,
        exptime: 321,
        cas: None,
      }),
      CommandRef::AddQ(SetCommandRef {
        key: "foo",
        value: b"bar",
        flags: 123,
        exptime: 321,
        cas: None,
      }),
      CommandRef::Replace(SetCommandRef {
        key: "foo",
        value: b"bar",
        flags: 123,
        exptime: 321,
        cas: None,
      }),
      CommandRef::ReplaceQ(SetCommandRef {
        key: "foo",
        value: b"bar",
        flags: 123,
        exptime: 321,
        cas: None,
      }),
      CommandRef::Append(AppendPrependCommandRef {
        key: "foo",
        value: b"bar",
      }),
      CommandRef::AppendQ(AppendPrependCommandRef {
        key: "foo",
        value: b"bar",
      }),
      CommandRef::Prepend(AppendPrependCommandRef {
        key: "foo",
        value: b"bar",
      }),
      CommandRef::PrependQ(AppendPrependCommandRef {
        key: "foo",
        value: b"bar",
      }),
      CommandRef::Set(SetCommandRef {
        key: "foo",
        value: b"bar",
        flags: 123,
        exptime: 321,
        cas: Some(456),
      }),
      CommandRef::SetQ(SetCommandRef {
        key: "foo",
        value: b"bar",
        flags: 123,
        exptime: 321,
        cas: Some(456),
      }),
      CommandRef::Delete(KeyCommandRef { key: "foo" }),
      CommandRef::Incr(IncrDecrCommandRef {
        key: "foo",
        delta: 123,
        init: 0,
        exptime: 0,
      }),
      CommandRef::Decr(IncrDecrCommandRef {
        key: "foo",
        delta: 123,
        init: 0,
        exptime: 0,
      }),
      CommandRef::Touch(TouchCommandRef {
        key: "foo",
        exptime: 123,
      }),
      CommandRef::Flush,
      CommandRef::Version,
      CommandRef::Stats,
      CommandRef::Quit,
    ];

    for expected in tests.iter() {
      let encoded = encode_text_command(expected).unwrap();
      let decoded = decode_text_command(encoded.as_slice()).unwrap();

      assert_eq!(expected, &decoded[0]);
    }
  }

  #[test]
  fn test_binary_command_roundtrip() {
    let tests = &[
      CommandRef::Get(KeyCommandRef { key: "foo" }),
      CommandRef::GetAndTouch(TouchCommandRef {
        key: "foo",
        exptime: 123,
      }),
      CommandRef::Delete(KeyCommandRef { key: "foo" }),
      CommandRef::Set(SetCommandRef {
        key: "foo",
        value: b"bar",
        flags: 123,
        exptime: 321,
        cas: None,
      }),
      CommandRef::SetQ(SetCommandRef {
        key: "foo",
        value: b"bar",
        flags: 123,
        exptime: 321,
        cas: None,
      }),
      CommandRef::Add(SetCommandRef {
        key: "foo",
        value: b"bar",
        flags: 123,
        exptime: 321,
        cas: None,
      }),
      CommandRef::AddQ(SetCommandRef {
        key: "foo",
        value: b"bar",
        flags: 123,
        exptime: 321,
        cas: None,
      }),
      CommandRef::Replace(SetCommandRef {
        key: "foo",
        value: b"bar",
        flags: 123,
        exptime: 321,
        cas: None,
      }),
      CommandRef::ReplaceQ(SetCommandRef {
        key: "foo",
        value: b"bar",
        flags: 123,
        exptime: 321,
        cas: None,
      }),
      CommandRef::Append(AppendPrependCommandRef {
        key: "foo",
        value: b"bar",
      }),
      CommandRef::AppendQ(AppendPrependCommandRef {
        key: "foo",
        value: b"bar",
      }),
      CommandRef::Prepend(AppendPrependCommandRef {
        key: "foo",
        value: b"bar",
      }),
      CommandRef::PrependQ(AppendPrependCommandRef {
        key: "foo",
        value: b"bar",
      }),
      CommandRef::Incr(IncrDecrCommandRef {
        key: "foo",
        delta: 1,
        init: 0,
        exptime: 0,
      }),
      CommandRef::IncrQ(IncrDecrCommandRef {
        key: "foo",
        delta: 1,
        init: 0,
        exptime: 0,
      }),
      CommandRef::Decr(IncrDecrCommandRef {
        key: "foo",
        delta: 1,
        init: 0,
        exptime: 0,
      }),
      CommandRef::DecrQ(IncrDecrCommandRef {
        key: "foo",
        delta: 1,
        init: 0,
        exptime: 0,
      }),
      CommandRef::FlushQ,
      CommandRef::Flush,
      CommandRef::Version,
      CommandRef::Stats,
      CommandRef::QuitQ,
      CommandRef::Quit,
    ];

    for expected in tests.iter() {
      let encoded = encode_binary_command(expected).unwrap();
      let decoded = decode_binary_command(encoded.as_slice()).unwrap();

      assert_eq!(expected, &decoded);
    }
  }
}
