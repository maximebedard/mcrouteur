use std::{collections::VecDeque, io};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::{
  io::{AsyncBufRead, AsyncBufReadExt},
  sync::mpsc,
};
use url::Url;

#[derive(Debug)]
pub struct Connection;

impl Connection {
  pub async fn connect(_url: Url) -> io::Result<Self> {
    todo!()
  }

  pub async fn close(&mut self) -> io::Result<()> {
    todo!()
  }

  pub async fn ping(&mut self) -> io::Result<()> {
    todo!()
  }

  pub async fn get(&mut self, k: impl AsRef<str>) -> io::Result<()> {
    todo!()
  }

  pub async fn set(&mut self, k: impl AsRef<str>, v: impl AsRef<[u8]>) -> io::Result<()> {
    todo!()
  }
}

struct Command2;

fn spawn_connection(queue_size: usize, url: Url) -> mpsc::Sender<Command2> {
  let (sender, mut receiver) = mpsc::channel(queue_size);
  tokio::task::spawn(async move {
    let mut connection = Connection::connect(url).await.unwrap();
    while let Some(msg) = receiver.recv().await {}
    connection.close().await.unwrap();
  });
  sender
}

fn spawn_pool(size: usize, queue_size: usize, connection_queue_size: usize, url: Url) -> mpsc::Sender<Command2> {
  let (sender, mut receiver) = mpsc::channel(queue_size);

  tokio::task::spawn(async move {
    let connections = (0..size)
      .map(|_i| spawn_connection(connection_queue_size, url.clone()))
      .collect::<Vec<_>>();

    let mut i = 0;
    while let Some(msg) = receiver.recv().await {
      connections[i].send(msg).await;
    }
  });
  sender
}

#[derive(Debug, PartialEq)]
pub struct KeyCommand<'a> {
  key: &'a str,
}

#[derive(Debug, PartialEq)]
pub struct TouchCommand<'a> {
  key: &'a str,
  exptime: u32,
}

#[derive(Debug, PartialEq)]
pub struct SetCommand<'a> {
  key: &'a str,
  value: &'a [u8],
  flags: u32,
  exptime: u32,
  cas: Option<u64>,
}

#[derive(Debug, PartialEq)]
pub struct IncrDecrCommand<'a> {
  key: &'a str,
  delta: u64,
  init: u64,
  exptime: u32,
}

#[derive(Debug, PartialEq)]
pub struct AppendPrependCommand<'a> {
  key: &'a str,
  value: &'a [u8],
}

#[derive(Debug, PartialEq)]
pub enum Command<'a> {
  Get(KeyCommand<'a>),
  GetQ(KeyCommand<'a>),
  GetK(KeyCommand<'a>),
  GetKQ(KeyCommand<'a>),
  GetAndTouch(TouchCommand<'a>),
  GetAndTouchQ(TouchCommand<'a>),
  Set(SetCommand<'a>),
  SetQ(SetCommand<'a>),
  Add(SetCommand<'a>),
  AddQ(SetCommand<'a>),
  Replace(SetCommand<'a>),
  ReplaceQ(SetCommand<'a>),
  Append(AppendPrependCommand<'a>),
  AppendQ(AppendPrependCommand<'a>),
  Prepend(AppendPrependCommand<'a>),
  PrependQ(AppendPrependCommand<'a>),
  Delete(KeyCommand<'a>),
  DeleteQ(KeyCommand<'a>),
  Incr(IncrDecrCommand<'a>),
  IncrQ(IncrDecrCommand<'a>),
  Decr(IncrDecrCommand<'a>),
  DecrQ(IncrDecrCommand<'a>),
  Touch(TouchCommand<'a>),
  TouchQ(TouchCommand<'a>),
  Flush,
  FlushQ,
  Version,
  Stats,
  Quit,
  QuitQ,
  Noop,
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

pub fn decode_command(input: &[u8]) -> Result<Vec<Command>, DecodeCommandError> {
  match input.get(0) {
    Some(0x80) => decode_binary_command(input).map(|v| vec![v]),
    Some(_) => decode_text_command(input),
    None => Err(DecodeCommandError::UnexpectedEof),
  }
}

pub fn decode_text_command(input: &[u8]) -> Result<Vec<Command>, DecodeCommandError> {
  let mut chunks = input.splitn(2, |v| *v == b' ');
  let command = chunks.next().ok_or(DecodeCommandError::UnexpectedEof)?;
  let input = chunks.next().unwrap_or(&[]);

  fn new_line_str(input: &[u8]) -> Result<&str, DecodeCommandError> {
    let input = std::str::from_utf8(input).map_err(|_| DecodeCommandError::InvalidFormat)?;
    let input = input
      .strip_suffix("\r\n")
      .or_else(|| input.strip_suffix("\n"))
      .ok_or(DecodeCommandError::UnexpectedEof)?;
    Ok(input)
  }

  match command {
    b"get" | b"gets" => {
      let input = new_line_str(input)?;
      Ok(input.split(' ').map(|key| Command::Get(KeyCommand { key })).collect())
    }
    b"gat" | b"gats" => {
      let input = new_line_str(input)?;

      let mut chunks = input.split(' ');

      let exptime = chunks.next().ok_or(DecodeCommandError::UnexpectedEof)?;
      let exptime = exptime.parse().map_err(|_| DecodeCommandError::InvalidFormat)?;

      Ok(
        chunks
          .map(|key| Command::GetAndTouch(TouchCommand { key, exptime }))
          .collect(),
      )
    }

    b"set" | b"add" | b"replace" | b"cas" | b"append" | b"prepend" => {
      let pos = input.iter().position(|v| *v == b'\n').unwrap_or_else(|| input.len());
      let (input, mut value) = (&input[..=pos], &input[pos + 1..]);

      let input = new_line_str(input)?;

      let mut chunks = input.split(" ");

      let key = chunks.next().ok_or(DecodeCommandError::UnexpectedEof)?;

      let flags = chunks.next().ok_or(DecodeCommandError::UnexpectedEof)?;
      let flags = flags.parse().map_err(|_| DecodeCommandError::InvalidFormat)?;

      let exptime = chunks.next().ok_or(DecodeCommandError::UnexpectedEof)?;
      let exptime = exptime.parse().map_err(|_| DecodeCommandError::InvalidFormat)?;

      let value_len = chunks.next().ok_or(DecodeCommandError::UnexpectedEof)?;
      let value_len = value_len
        .parse::<usize>()
        .map_err(|_| DecodeCommandError::InvalidFormat)?;

      value = value
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

      let set_cmd = SetCommand {
        key,
        value,
        flags,
        exptime,
        cas,
      };

      let append_cmd = AppendPrependCommand { key, value };

      match command {
        b"set" | b"cas" if noreply => Ok(vec![Command::SetQ(set_cmd)]),
        b"set" | b"cas" => Ok(vec![Command::Set(set_cmd)]),
        b"add" if noreply => Ok(vec![Command::AddQ(set_cmd)]),
        b"add" => Ok(vec![Command::Add(set_cmd)]),
        b"replace" if noreply => Ok(vec![Command::ReplaceQ(set_cmd)]),
        b"replace" => Ok(vec![Command::Replace(set_cmd)]),
        b"append" if noreply => Ok(vec![Command::AppendQ(append_cmd)]),
        b"append" => Ok(vec![Command::Append(append_cmd)]),
        b"prepend" if noreply => Ok(vec![Command::PrependQ(append_cmd)]),
        b"prepend" => Ok(vec![Command::Prepend(append_cmd)]),
        _ => unreachable!(),
      }
    }

    b"delete" => {
      let input = new_line_str(input)?;

      let mut chunks = input.split(' ');

      let key = chunks.next().ok_or(DecodeCommandError::UnexpectedEof)?;

      let noreply = chunks.next().filter(|v| *v == "noreply").is_some();
      if noreply {
        Ok(vec![Command::DeleteQ(KeyCommand { key })])
      } else {
        Ok(vec![Command::Delete(KeyCommand { key })])
      }
    }
    b"incr" | b"decr" => {
      let input = new_line_str(input)?;

      let mut chunks = input.split(' ');

      let key = chunks.next().ok_or(DecodeCommandError::UnexpectedEof)?;

      let delta = chunks.next().ok_or(DecodeCommandError::UnexpectedEof)?;
      let delta = delta.parse().map_err(|_| DecodeCommandError::InvalidFormat)?;

      let init = 0;
      let exptime = 0;

      let noreply = chunks.next().filter(|v| *v == "noreply").is_some();

      let cmd = IncrDecrCommand {
        key,
        delta,
        init,
        exptime,
      };

      match command {
        b"incr" if noreply => Ok(vec![Command::IncrQ(cmd)]),
        b"incr" => Ok(vec![Command::Incr(cmd)]),
        b"decr" if noreply => Ok(vec![Command::DecrQ(cmd)]),
        b"decr" => Ok(vec![Command::Decr(cmd)]),
        _ => unreachable!(),
      }
    }
    b"touch" => {
      let input = new_line_str(input)?;

      let mut chunks = input.split(' ');

      let key = chunks.next().ok_or(DecodeCommandError::UnexpectedEof)?;

      let exptime = chunks.next().ok_or(DecodeCommandError::UnexpectedEof)?;
      let exptime = exptime.parse().map_err(|_| DecodeCommandError::InvalidFormat)?;

      let noreply = chunks.next().filter(|v| *v == "noreply").is_some();

      if noreply {
        Ok(vec![Command::TouchQ(TouchCommand { key, exptime })])
      } else {
        Ok(vec![Command::Touch(TouchCommand { key, exptime })])
      }
    }
    b"flush_all\r\n" | b"flush_all\n" => Ok(vec![Command::Flush]),
    b"version\r\n" | b"version\n" => Ok(vec![Command::Version]),
    b"stats\r\n" | b"stats\n" => Ok(vec![Command::Stats]),
    b"quit\r\n" | b"quit\n" => Ok(vec![Command::Quit]),
    _ => Err(DecodeCommandError::InvalidCommand),
  }
}

pub fn decode_binary_command(mut input: &[u8]) -> Result<Command, DecodeCommandError> {
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

  let header = RequestHeader {
    op,
    key_len,
    extras_len,
    body_len,
    opaque,
    cas,
  };

  if input.len() != body_len {
    return Err(DecodeCommandError::UnexpectedEof);
  }

  fn decode_key_command<'a>(header: &RequestHeader, mut input: &'a [u8]) -> Result<KeyCommand<'a>, DecodeCommandError> {
    let key_len = header.key_len;
    let extras_len = header.extras_len;
    let body_len = header.body_len;
    assert_eq!(0, extras_len);
    assert!(key_len > 0);
    assert_eq!(key_len, body_len);

    let key = input.get(0..key_len).ok_or(DecodeCommandError::UnexpectedEof)?;
    input.advance(key_len);
    assert!(input.is_empty());

    let key = std::str::from_utf8(key).map_err(|_| DecodeCommandError::InvalidFormat)?;
    Ok(KeyCommand { key })
  }

  fn decode_set_command<'a>(header: &RequestHeader, mut input: &'a [u8]) -> Result<SetCommand<'a>, DecodeCommandError> {
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
    input.advance(key_len);

    let value = input.get(0..value_len).ok_or(DecodeCommandError::UnexpectedEof)?;
    input.advance(value_len);

    assert!(input.is_empty());

    let cas = None;
    Ok(SetCommand {
      key,
      value,
      flags,
      exptime,
      cas,
    })
  }

  fn decode_append_prepend_command<'a>(
    header: &RequestHeader,
    mut input: &'a [u8],
  ) -> Result<AppendPrependCommand<'a>, DecodeCommandError> {
    let key_len = header.key_len;
    let extras_len = header.extras_len;
    let body_len = header.body_len;
    assert_eq!(0, extras_len);
    assert!(key_len > 0);
    assert!(body_len >= key_len);

    let key = input.get(0..key_len).ok_or(DecodeCommandError::UnexpectedEof)?;
    let key = std::str::from_utf8(key).map_err(|_| DecodeCommandError::InvalidFormat)?;
    input.advance(key_len);

    let value_len = body_len - key_len;
    let value = input.get(0..value_len).ok_or(DecodeCommandError::UnexpectedEof)?;
    input.advance(value_len);

    assert!(input.is_empty());

    Ok(AppendPrependCommand { key, value })
  }

  fn decode_incr_decr_command<'a>(
    header: &RequestHeader,
    mut input: &'a [u8],
  ) -> Result<IncrDecrCommand<'a>, DecodeCommandError> {
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
    input.advance(key_len);
    assert!(input.is_empty());

    Ok(IncrDecrCommand {
      key,
      delta,
      init,
      exptime,
    })
  }

  fn decode_touch_command<'a>(
    header: &RequestHeader,
    mut input: &'a [u8],
  ) -> Result<TouchCommand<'a>, DecodeCommandError> {
    let key_len = header.key_len;
    let extras_len = header.extras_len;
    let body_len = header.body_len;
    assert_eq!(4, extras_len);
    assert!(key_len > 0);
    assert_eq!(body_len, key_len + extras_len);

    let exptime = input.get_u32();

    let key = input.get(0..key_len).ok_or(DecodeCommandError::UnexpectedEof)?;
    let key = std::str::from_utf8(key).map_err(|_| DecodeCommandError::InvalidFormat)?;
    input.advance(key_len);
    assert!(input.is_empty());

    Ok(TouchCommand { key, exptime })
  }

  match op {
    0x00 => decode_key_command(&header, input).map(Command::Get),
    0x09 => decode_key_command(&header, input).map(Command::GetQ),
    0x0c => decode_key_command(&header, input).map(Command::GetK),
    0x0d => decode_key_command(&header, input).map(Command::GetKQ),
    0x04 => decode_key_command(&header, input).map(Command::Delete),
    0x14 => decode_key_command(&header, input).map(Command::DeleteQ),
    0x01 => decode_set_command(&header, input).map(Command::Set),
    0x11 => decode_set_command(&header, input).map(Command::SetQ),
    0x02 => decode_set_command(&header, input).map(Command::Add),
    0x12 => decode_set_command(&header, input).map(Command::AddQ),
    0x03 => decode_set_command(&header, input).map(Command::Replace),
    0x13 => decode_set_command(&header, input).map(Command::ReplaceQ),
    0x0e => decode_append_prepend_command(&header, input).map(Command::Append),
    0x19 => decode_append_prepend_command(&header, input).map(Command::AppendQ),
    0x0f => decode_append_prepend_command(&header, input).map(Command::Prepend),
    0x1a => decode_append_prepend_command(&header, input).map(Command::PrependQ),
    0x05 => decode_incr_decr_command(&header, input).map(Command::Incr),
    0x15 => decode_incr_decr_command(&header, input).map(Command::IncrQ),
    0x06 => decode_incr_decr_command(&header, input).map(Command::Decr),
    0x16 => decode_incr_decr_command(&header, input).map(Command::DecrQ),
    0x1c => decode_touch_command(&header, input).map(Command::Touch),
    0x1d => decode_touch_command(&header, input).map(Command::GetAndTouch),
    0x1e => decode_touch_command(&header, input).map(Command::GetAndTouchQ),
    0x07 => {
      assert_eq!(0, extras_len);
      assert_eq!(0, key_len);
      assert_eq!(0, body_len);
      Ok(Command::Quit)
    }
    0x17 => {
      assert_eq!(0, extras_len);
      assert_eq!(0, key_len);
      assert_eq!(0, body_len);
      Ok(Command::QuitQ)
    }
    0x08 => {
      assert_eq!(0, extras_len);
      assert_eq!(0, key_len);
      assert_eq!(0, body_len);
      Ok(Command::Flush)
    }
    0x18 => {
      assert_eq!(0, extras_len);
      assert_eq!(0, key_len);
      assert_eq!(0, body_len);
      Ok(Command::FlushQ)
    }
    0x0a => {
      assert_eq!(0, extras_len);
      assert_eq!(0, key_len);
      assert_eq!(0, body_len);
      Ok(Command::Noop)
    }
    0x0b => {
      assert_eq!(0, extras_len);
      assert_eq!(0, key_len);
      assert_eq!(0, body_len);
      Ok(Command::Version)
    }
    0x10 => {
      assert_eq!(0, extras_len);
      assert_eq!(0, key_len);
      assert_eq!(0, body_len);
      Ok(Command::Stats)
    }
    _ => Err(DecodeCommandError::InvalidCommand),
  }
}

#[derive(Debug, PartialEq)]
pub enum EncodeCommandError {
  NotSupported,
}

pub fn encode_binary_command(command: &Command<'_>) -> Result<Vec<u8>, EncodeCommandError> {
  fn encode_key_command(op: u8, cmd: &KeyCommand<'_>) -> Vec<u8> {
    let mut buffer = Vec::new();
    let key_len = cmd.key.len();
    let body_len = key_len;
    put_request_header(
      &mut buffer,
      &RequestHeader {
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
      &RequestHeader {
        op,
        ..Default::default()
      },
    );
    buffer
  }

  fn encode_set_command(op: u8, cmd: &SetCommand<'_>) -> Vec<u8> {
    let mut buffer = Vec::new();
    let key_len = cmd.key.len();
    let extras_len = 8;
    let value_len = cmd.value.len();
    let body_len = key_len + extras_len + value_len;
    let cas = cmd.cas.unwrap_or_default();
    put_request_header(
      &mut buffer,
      &RequestHeader {
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
    buffer.put(cmd.value);
    buffer
  }

  fn encode_append_prepend_command(op: u8, cmd: &AppendPrependCommand<'_>) -> Vec<u8> {
    let mut buffer = Vec::new();
    let key_len = cmd.key.len();
    let value_len = cmd.value.len();
    let body_len = key_len + value_len;
    put_request_header(
      &mut buffer,
      &RequestHeader {
        op,
        key_len,
        body_len,
        ..Default::default()
      },
    );
    buffer.put(cmd.key.as_bytes());
    buffer.put(cmd.value);
    buffer
  }

  fn encode_incr_decr_command(op: u8, cmd: &IncrDecrCommand<'_>) -> Vec<u8> {
    let mut buffer = Vec::new();
    let key_len = cmd.key.len();
    let extras_len = 20;
    let body_len = key_len + extras_len;
    put_request_header(
      &mut buffer,
      &RequestHeader {
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

  fn encode_touch_command(op: u8, cmd: &TouchCommand<'_>) -> Vec<u8> {
    let mut buffer = Vec::new();
    let key_len = cmd.key.len();
    let extras_len = 4;
    let body_len = key_len + extras_len;
    put_request_header(
      &mut buffer,
      &RequestHeader {
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
    Command::GetQ(cmd) => Ok(encode_key_command(0x09, cmd)),
    Command::Get(cmd) => Ok(encode_key_command(0x00, cmd)),
    Command::GetKQ(cmd) => Ok(encode_key_command(0x0d, cmd)),
    Command::GetK(cmd) => Ok(encode_key_command(0x0c, cmd)),
    Command::GetAndTouchQ(cmd) => Ok(encode_touch_command(0x1e, cmd)),
    Command::GetAndTouch(cmd) => Ok(encode_touch_command(0x1d, cmd)),
    Command::Set(cmd) => Ok(encode_set_command(0x01, cmd)),
    Command::SetQ(cmd) => Ok(encode_set_command(0x11, cmd)),
    Command::Add(cmd) => Ok(encode_set_command(0x02, cmd)),
    Command::AddQ(cmd) => Ok(encode_set_command(0x12, cmd)),
    Command::Replace(cmd) => Ok(encode_set_command(0x03, cmd)),
    Command::ReplaceQ(cmd) => Ok(encode_set_command(0x13, cmd)),
    Command::Append(cmd) => Ok(encode_append_prepend_command(0x0e, cmd)),
    Command::AppendQ(cmd) => Ok(encode_append_prepend_command(0x19, cmd)),
    Command::Prepend(cmd) => Ok(encode_append_prepend_command(0x0f, cmd)),
    Command::PrependQ(cmd) => Ok(encode_append_prepend_command(0x1a, cmd)),
    Command::DeleteQ(cmd) => Ok(encode_key_command(0x14, cmd)),
    Command::Delete(cmd) => Ok(encode_key_command(0x04, cmd)),
    Command::IncrQ(cmd) => Ok(encode_incr_decr_command(0x15, cmd)),
    Command::Incr(cmd) => Ok(encode_incr_decr_command(0x05, cmd)),
    Command::DecrQ(cmd) => Ok(encode_incr_decr_command(0x16, cmd)),
    Command::Decr(cmd) => Ok(encode_incr_decr_command(0x06, cmd)),
    Command::TouchQ(_) => Err(EncodeCommandError::NotSupported),
    Command::Touch(cmd) => Ok(encode_touch_command(0x1c, cmd)),
    Command::FlushQ => Ok(encode_simple_command(0x18)),
    Command::Flush => Ok(encode_simple_command(0x08)),
    Command::Version => Ok(encode_simple_command(0x0b)),
    Command::Stats => Ok(encode_simple_command(0x10)),
    Command::QuitQ => Ok(encode_simple_command(0x17)),
    Command::Quit => Ok(encode_simple_command(0x07)),
    Command::Noop => Ok(encode_simple_command(0x0a)),
  }
}

pub fn encode_text_command(command: &Command<'_>) -> Result<Vec<u8>, EncodeCommandError> {
  fn encode_set_command(op: &str, noreply: bool, cmd: &SetCommand<'_>) -> Result<Vec<u8>, EncodeCommandError> {
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
    buffer.extend_from_slice(cmd.value);
    buffer.extend_from_slice(b"\r\n");
    Ok(buffer)
  }

  fn encode_append_prepend_command(op: &str, noreply: bool, cmd: &AppendPrependCommand<'_>) -> Vec<u8> {
    let mut buffer = format!("{} {} 0 0 {}", op, cmd.key, cmd.value.len()).into_bytes();
    if noreply {
      buffer.extend_from_slice(b" noreply");
    }
    buffer.extend_from_slice(b"\r\n");
    buffer.extend_from_slice(cmd.value);
    buffer.extend_from_slice(b"\r\n");
    buffer
  }

  match command {
    Command::GetQ(_) => Err(EncodeCommandError::NotSupported),
    Command::Get(KeyCommand { key }) => Ok(format!("get {key}\r\n").into_bytes()),
    Command::GetKQ(_) => Err(EncodeCommandError::NotSupported),
    Command::GetK(_) => Err(EncodeCommandError::NotSupported),
    Command::GetAndTouchQ(_) => Err(EncodeCommandError::NotSupported),
    Command::GetAndTouch(TouchCommand { key, exptime }) => Ok(format!("gat {exptime} {key}\r\n").into_bytes()),
    Command::Set(cmd) => encode_set_command("set", false, cmd),
    Command::SetQ(cmd) => encode_set_command("set", true, cmd),
    Command::Add(cmd) => encode_set_command("add", false, cmd),
    Command::AddQ(cmd) => encode_set_command("add", true, cmd),
    Command::Replace(cmd) => encode_set_command("replace", false, cmd),
    Command::ReplaceQ(cmd) => encode_set_command("replace", true, cmd),
    Command::Append(cmd) => Ok(encode_append_prepend_command("append", false, cmd)),
    Command::AppendQ(cmd) => Ok(encode_append_prepend_command("append", true, cmd)),
    Command::Prepend(cmd) => Ok(encode_append_prepend_command("prepend", false, cmd)),
    Command::PrependQ(cmd) => Ok(encode_append_prepend_command("prepend", true, cmd)),
    Command::DeleteQ(KeyCommand { key }) => Ok(format!("delete {key} noreply\r\n").into_bytes()),
    Command::Delete(KeyCommand { key }) => Ok(format!("delete {key}\r\n").into_bytes()),
    Command::IncrQ(IncrDecrCommand { key, delta, .. }) => Ok(format!("incr {key} {delta} noreply\r\n").into_bytes()),
    Command::Incr(IncrDecrCommand { key, delta, .. }) => Ok(format!("incr {key} {delta}\r\n").into_bytes()),
    Command::DecrQ(IncrDecrCommand { key, delta, .. }) => Ok(format!("decr {key} {delta} noreply\r\n").into_bytes()),
    Command::Decr(IncrDecrCommand { key, delta, .. }) => Ok(format!("decr {key} {delta}\r\n").into_bytes()),
    Command::TouchQ(TouchCommand { key, exptime }) => Ok(format!("touch {key} {exptime} noreply\r\n").into_bytes()),
    Command::Touch(TouchCommand { key, exptime }) => Ok(format!("touch {key} {exptime}\r\n").into_bytes()),
    Command::FlushQ => Err(EncodeCommandError::NotSupported),
    Command::Flush => Ok("flush_all\r\n".into()),
    Command::Version => Ok("version\r\n".into()),
    Command::Stats => Ok("stats\r\n".into()),
    Command::QuitQ => Err(EncodeCommandError::NotSupported),
    Command::Quit => Ok("quit\r\n".into()),
    Command::Noop => Err(EncodeCommandError::NotSupported),
  }
}

#[derive(Debug, Default)]
struct RequestHeader {
  op: u8,
  key_len: usize,
  extras_len: usize,
  body_len: usize,
  opaque: u32,
  cas: u64,
}

fn put_request_header(buffer: &mut Vec<u8>, h: &RequestHeader) {
  buffer.put_u8(0x80);
  buffer.put_u8(h.op);
  buffer.put_u16(h.key_len.try_into().unwrap());
  buffer.put_u8(h.extras_len.try_into().unwrap());
  buffer.put_u8(0x00);
  buffer.put_u16(0x0000);
  buffer.put_u32(h.body_len.try_into().unwrap());
  buffer.put_u32(h.opaque);
  buffer.put_u64(h.cas);
}

#[cfg(test)]
mod tests {

  use super::{
    decode_binary_command, decode_text_command, encode_binary_command, encode_text_command, AppendPrependCommand,
    Command, DecodeCommandError, IncrDecrCommand, KeyCommand, SetCommand, TouchCommand,
  };

  #[test]
  fn test_decode_text_command() {
    let tests: &[(&[u8], _)] = &[
      (b"get foo\r\n", Ok(vec![Command::Get(KeyCommand { key: "foo" })])),
      (
        b"get foo bar\r\n",
        Ok(vec![
          Command::Get(KeyCommand { key: "foo" }),
          Command::Get(KeyCommand { key: "bar" }),
        ]),
      ),
      (b"gets foo\r\n", Ok(vec![Command::Get(KeyCommand { key: "foo" })])),
      (
        b"gets foo bar\r\n",
        Ok(vec![
          Command::Get(KeyCommand { key: "foo" }),
          Command::Get(KeyCommand { key: "bar" }),
        ]),
      ),
      (
        b"touch foo 123\r\n",
        Ok(vec![Command::Touch(TouchCommand {
          key: "foo",
          exptime: 123,
        })]),
      ),
      (
        b"touch foo 123 noreply\r\n",
        Ok(vec![Command::TouchQ(TouchCommand {
          key: "foo",
          exptime: 123,
        })]),
      ),
      (
        b"incr foo 2\r\n",
        Ok(vec![Command::Incr(IncrDecrCommand {
          key: "foo",
          delta: 2,
          init: 0,
          exptime: 0,
        })]),
      ),
      (
        b"incr foo 2 noreply\r\n",
        Ok(vec![Command::IncrQ(IncrDecrCommand {
          key: "foo",
          delta: 2,
          init: 0,
          exptime: 0,
        })]),
      ),
      (
        b"decr foo 2\r\n",
        Ok(vec![Command::Decr(IncrDecrCommand {
          key: "foo",
          delta: 2,
          init: 0,
          exptime: 0,
        })]),
      ),
      (
        b"decr foo 2 noreply\r\n",
        Ok(vec![Command::DecrQ(IncrDecrCommand {
          key: "foo",
          delta: 2,
          init: 0,
          exptime: 0,
        })]),
      ),
      (b"delete foo\r\n", Ok(vec![Command::Delete(KeyCommand { key: "foo" })])),
      (
        b"delete foo noreply\r\n",
        Ok(vec![Command::DeleteQ(KeyCommand { key: "foo" })]),
      ),
      (
        b"set foo 123 321 3\r\nbar\r\n",
        Ok(vec![Command::Set(SetCommand {
          key: "foo",
          value: b"bar",
          flags: 123,
          exptime: 321,
          cas: None,
        })]),
      ),
      (
        b"set foo 123 321 0\r\n\r\n",
        Ok(vec![Command::Set(SetCommand {
          key: "foo",
          value: b"",
          flags: 123,
          exptime: 321,
          cas: None,
        })]),
      ),
      (
        b"set foo 123 321 3 noreply\r\nbar\r\n",
        Ok(vec![Command::SetQ(SetCommand {
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
        Ok(vec![Command::Add(SetCommand {
          key: "foo",
          value: b"bar",
          flags: 123,
          exptime: 321,
          cas: None,
        })]),
      ),
      (
        b"add foo 123 321 3 noreply\r\nbar\r\n",
        Ok(vec![Command::AddQ(SetCommand {
          key: "foo",
          value: b"bar",
          flags: 123,
          exptime: 321,
          cas: None,
        })]),
      ),
      (
        b"replace foo 123 321 3\r\nbar\r\n",
        Ok(vec![Command::Replace(SetCommand {
          key: "foo",
          value: b"bar",
          flags: 123,
          exptime: 321,
          cas: None,
        })]),
      ),
      (
        b"replace foo 123 321 3 noreply\r\nbar\r\n",
        Ok(vec![Command::ReplaceQ(SetCommand {
          key: "foo",
          value: b"bar",
          flags: 123,
          exptime: 321,
          cas: None,
        })]),
      ),
      (
        b"append foo 123 321 3\r\nbar\r\n",
        Ok(vec![Command::Append(AppendPrependCommand {
          key: "foo",
          value: b"bar",
        })]),
      ),
      (
        b"append foo 123 321 3 noreply\r\nbar\r\n",
        Ok(vec![Command::AppendQ(AppendPrependCommand {
          key: "foo",
          value: b"bar",
        })]),
      ),
      (
        b"prepend foo 123 321 3\r\nbar\r\n",
        Ok(vec![Command::Prepend(AppendPrependCommand {
          key: "foo",
          value: b"bar",
        })]),
      ),
      (
        b"prepend foo 123 321 3 noreply\r\nbar\r\n",
        Ok(vec![Command::PrependQ(AppendPrependCommand {
          key: "foo",
          value: b"bar",
        })]),
      ),
      (
        b"cas foo 123 321 3 567\r\nbar\r\n",
        Ok(vec![Command::Set(SetCommand {
          key: "foo",
          value: b"bar",
          flags: 123,
          exptime: 321,
          cas: Some(567),
        })]),
      ),
      (
        b"cas foo 123 321 0 567\r\n\r\n",
        Ok(vec![Command::Set(SetCommand {
          key: "foo",
          value: b"",
          flags: 123,
          exptime: 321,
          cas: Some(567),
        })]),
      ),
      (
        b"cas foo 123 321 3 567 noreply\r\nbar\r\n",
        Ok(vec![Command::SetQ(SetCommand {
          key: "foo",
          value: b"bar",
          flags: 123,
          exptime: 321,
          cas: Some(567),
        })]),
      ),
      (b"flush_all\r\n", Ok(vec![Command::Flush])),
      (b"version\r\n", Ok(vec![Command::Version])),
      (b"stats\r\n", Ok(vec![Command::Stats])),
      (b"quit\r\n", Ok(vec![Command::Quit])),
    ];

    for t in tests {
      println!("{:?}", std::str::from_utf8(t.0));
      assert_eq!(t.1, decode_text_command(t.0));
    }
  }

  #[test]
  fn test_text_command_roundtrip() {
    let tests = &[
      Command::Get(KeyCommand { key: "foo" }),
      Command::GetAndTouch(TouchCommand {
        key: "foo",
        exptime: 123,
      }),
      Command::Set(SetCommand {
        key: "foo",
        value: b"bar",
        flags: 123,
        exptime: 321,
        cas: None,
      }),
      Command::SetQ(SetCommand {
        key: "foo",
        value: b"bar",
        flags: 123,
        exptime: 321,
        cas: None,
      }),
      Command::Add(SetCommand {
        key: "foo",
        value: b"bar",
        flags: 123,
        exptime: 321,
        cas: None,
      }),
      Command::AddQ(SetCommand {
        key: "foo",
        value: b"bar",
        flags: 123,
        exptime: 321,
        cas: None,
      }),
      Command::Replace(SetCommand {
        key: "foo",
        value: b"bar",
        flags: 123,
        exptime: 321,
        cas: None,
      }),
      Command::ReplaceQ(SetCommand {
        key: "foo",
        value: b"bar",
        flags: 123,
        exptime: 321,
        cas: None,
      }),
      Command::Append(AppendPrependCommand {
        key: "foo",
        value: b"bar",
      }),
      Command::AppendQ(AppendPrependCommand {
        key: "foo",
        value: b"bar",
      }),
      Command::Prepend(AppendPrependCommand {
        key: "foo",
        value: b"bar",
      }),
      Command::PrependQ(AppendPrependCommand {
        key: "foo",
        value: b"bar",
      }),
      Command::Set(SetCommand {
        key: "foo",
        value: b"bar",
        flags: 123,
        exptime: 321,
        cas: Some(456),
      }),
      Command::SetQ(SetCommand {
        key: "foo",
        value: b"bar",
        flags: 123,
        exptime: 321,
        cas: Some(456),
      }),
      Command::Delete(KeyCommand { key: "foo" }),
      Command::Incr(IncrDecrCommand {
        key: "foo",
        delta: 123,
        init: 0,
        exptime: 0,
      }),
      Command::Decr(IncrDecrCommand {
        key: "foo",
        delta: 123,
        init: 0,
        exptime: 0,
      }),
      Command::Touch(TouchCommand {
        key: "foo",
        exptime: 123,
      }),
      Command::Flush,
      Command::Version,
      Command::Stats,
      Command::Quit,
    ];

    for expected in tests.into_iter() {
      let encoded = encode_text_command(expected).unwrap();
      let decoded = decode_text_command(encoded.as_slice()).unwrap();

      assert_eq!(expected, &decoded[0]);
    }
  }

  #[test]
  fn test_binary_command_roundtrip() {
    let tests = &[
      Command::Get(KeyCommand { key: "foo" }),
      Command::GetAndTouch(TouchCommand {
        key: "foo",
        exptime: 123,
      }),
      Command::Delete(KeyCommand { key: "foo" }),
      Command::Set(SetCommand {
        key: "foo",
        value: b"bar",
        flags: 123,
        exptime: 321,
        cas: None,
      }),
      Command::SetQ(SetCommand {
        key: "foo",
        value: b"bar",
        flags: 123,
        exptime: 321,
        cas: None,
      }),
      Command::Add(SetCommand {
        key: "foo",
        value: b"bar",
        flags: 123,
        exptime: 321,
        cas: None,
      }),
      Command::AddQ(SetCommand {
        key: "foo",
        value: b"bar",
        flags: 123,
        exptime: 321,
        cas: None,
      }),
      Command::Replace(SetCommand {
        key: "foo",
        value: b"bar",
        flags: 123,
        exptime: 321,
        cas: None,
      }),
      Command::ReplaceQ(SetCommand {
        key: "foo",
        value: b"bar",
        flags: 123,
        exptime: 321,
        cas: None,
      }),
      Command::Append(AppendPrependCommand {
        key: "foo",
        value: b"bar",
      }),
      Command::AppendQ(AppendPrependCommand {
        key: "foo",
        value: b"bar",
      }),
      Command::Prepend(AppendPrependCommand {
        key: "foo",
        value: b"bar",
      }),
      Command::PrependQ(AppendPrependCommand {
        key: "foo",
        value: b"bar",
      }),
      Command::Incr(IncrDecrCommand {
        key: "foo",
        delta: 1,
        init: 0,
        exptime: 0,
      }),
      Command::IncrQ(IncrDecrCommand {
        key: "foo",
        delta: 1,
        init: 0,
        exptime: 0,
      }),
      Command::Decr(IncrDecrCommand {
        key: "foo",
        delta: 1,
        init: 0,
        exptime: 0,
      }),
      Command::DecrQ(IncrDecrCommand {
        key: "foo",
        delta: 1,
        init: 0,
        exptime: 0,
      }),
      Command::FlushQ,
      Command::Flush,
      Command::Version,
      Command::Stats,
      Command::QuitQ,
      Command::Quit,
    ];

    for expected in tests.into_iter() {
      let encoded = encode_binary_command(expected).unwrap();
      let decoded = decode_binary_command(encoded.as_slice()).unwrap();

      assert_eq!(expected, &decoded);
    }
  }
}
