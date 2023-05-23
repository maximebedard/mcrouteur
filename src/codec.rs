use std::io;

use bytes::{Buf, Bytes, BytesMut};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[derive(Debug, PartialEq)]
pub struct KeyCommand {
  pub key: String,
}

#[derive(Debug, PartialEq)]
pub struct TouchCommand {
  pub key: String,
  pub exptime: u32,
}

#[derive(Debug, PartialEq)]
pub struct SetCommand {
  pub key: String,
  pub value: Vec<u8>,
  pub flags: u32,
  pub exptime: u32,
  pub cas: Option<u64>,
}

#[derive(Debug, PartialEq)]
pub struct TextIncrDecrCommand {
  pub key: String,
  pub delta: u64,
}

#[derive(Debug, PartialEq)]
pub struct IncrDecrCommand {
  pub key: String,
  pub delta: u64,
  pub init: u64,
  pub exptime: u32,
}

#[derive(Debug, PartialEq)]
pub struct AppendPrependCommand {
  pub key: String,
  pub value: Vec<u8>,
}

#[derive(Debug, PartialEq)]
pub enum TextCommand {
  GetM { keys: Vec<String> },
  GetAndTouchM { keys: Vec<String>, exptime: u32 },
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
  Incr(TextIncrDecrCommand),
  IncrQ(TextIncrDecrCommand),
  Decr(TextIncrDecrCommand),
  DecrQ(TextIncrDecrCommand),
  Touch(TouchCommand),
  TouchQ(TouchCommand),
  Flush,
  Version,
  Stats,
  Quit,
}

#[derive(Debug, PartialEq)]
pub enum BinaryCommand {
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
  Flush,
  FlushQ,
  Version,
  Stats,
  Quit,
  QuitQ,
  Noop,
}

#[derive(Debug, PartialEq)]
pub enum Command {
  Text(TextCommand),
  Binary(BinaryCommand),
}

fn unexpected_eof() -> io::Error {
  io::Error::new(io::ErrorKind::UnexpectedEof, "unexpected eof")
}

fn invalid_data() -> io::Error {
  io::Error::new(io::ErrorKind::InvalidData, "invalid_data")
}

pub async fn read_command(mut r: impl AsyncBufRead + Unpin) -> io::Result<Command> {
  match r.fill_buf().await {
    Ok(bytes) if bytes.starts_with(&[0x80]) => read_binary_command(&mut r).await.map(Command::Binary),
    Ok(&[]) => Err(io::Error::new(io::ErrorKind::UnexpectedEof, "unexpected eof")),
    Ok(_) => read_text_command(&mut r).await.map(Command::Text),
    Err(err) => Err(err),
  }
}

pub async fn read_text_command(mut r: impl AsyncBufRead + Unpin) -> io::Result<TextCommand> {
  fn string_trim_end_in_place(v: &mut String) {
    if v.ends_with("\r\n") {
      v.truncate(v.len() - 2);
    } else if v.ends_with("\n") {
      v.truncate(v.len() - 1);
    }
  }

  fn bytes_trim_end_in_place(v: &mut Vec<u8>) {
    if v.ends_with(b"\r\n") {
      v.truncate(v.len() - 2);
    } else if v.ends_with(b"\n") {
      v.truncate(v.len() - 1);
    }
  }

  let mut line = String::new();
  r.read_line(&mut line).await?;
  string_trim_end_in_place(&mut line);

  let mut chunks = line.splitn(2, " ");
  let command = chunks.next().ok_or_else(|| unexpected_eof())?;
  let args = chunks.next().unwrap_or("");

  fn decode_delete_command(args: &str) -> io::Result<KeyCommand> {
    let key = args.split(' ').next().ok_or_else(|| unexpected_eof())?;
    let key = key.to_string();
    Ok(KeyCommand { key })
  }

  fn decode_incr_decr_command(args: &str) -> io::Result<TextIncrDecrCommand> {
    let mut chunks = args.split(' ');

    let key = chunks.next().ok_or_else(|| unexpected_eof())?;
    let key = key.to_string();

    let delta = chunks.next().ok_or_else(|| unexpected_eof())?;
    let delta = delta.parse().map_err(|_| invalid_data())?;

    Ok(TextIncrDecrCommand { key, delta })
  }

  fn decode_touch_command(args: &str) -> io::Result<TouchCommand> {
    let mut chunks = args.split(' ');

    let key = chunks.next().ok_or_else(|| unexpected_eof())?;
    let key = key.to_string();

    let exptime = chunks.next().ok_or_else(|| unexpected_eof())?;
    let exptime = exptime.parse().map_err(|_| invalid_data())?;

    Ok(TouchCommand { key, exptime })
  }

  async fn read_append_prepend_command(
    mut r: impl AsyncBufRead + Unpin,
    args: &str,
  ) -> io::Result<AppendPrependCommand> {
    let mut chunks = args.split(' ');

    let key = chunks.next().ok_or_else(|| unexpected_eof())?;
    let key = key.to_string();

    let _flags = chunks.next().ok_or_else(|| unexpected_eof())?;
    let _flags = _flags.parse::<u32>().map_err(|_| invalid_data())?;

    let _exptime = chunks.next().ok_or_else(|| unexpected_eof())?;
    let _exptime = _exptime.parse::<u32>().map_err(|_| invalid_data())?;

    let value_len = chunks.next().ok_or_else(|| unexpected_eof())?;
    let value_len = value_len.parse::<usize>().map_err(|_| invalid_data())?;

    let mut value = Vec::with_capacity(value_len);
    r.read_buf(&mut value).await?;
    bytes_trim_end_in_place(&mut value);

    Ok(AppendPrependCommand { key, value })
  }

  async fn read_set_command(mut r: impl AsyncBufRead + Unpin, args: &str) -> io::Result<SetCommand> {
    let mut chunks = args.split(' ');

    let key = chunks.next().ok_or_else(|| unexpected_eof())?;
    let key = key.to_string();

    let flags = chunks.next().ok_or_else(|| unexpected_eof())?;
    let flags = flags.parse().map_err(|_| invalid_data())?;

    let exptime = chunks.next().ok_or_else(|| unexpected_eof())?;
    let exptime = exptime.parse().map_err(|_| invalid_data())?;

    let value_len = chunks.next().ok_or_else(|| unexpected_eof())?;
    let value_len = value_len.parse::<usize>().map_err(|_| invalid_data())?;

    let mut value = Vec::with_capacity(value_len);
    r.read_buf(&mut value).await?;
    bytes_trim_end_in_place(&mut value);

    let cas = None;

    Ok(SetCommand {
      key,
      value,
      flags,
      exptime,
      cas,
    })
  }

  match command {
    "get" | "gets" => {
      let keys = args.split(' ').map(Into::into).collect();
      Ok(TextCommand::GetM { keys })
    }
    "gat" | "gats" => {
      let mut chunks = args.split(' ');

      let exptime = chunks.next().ok_or_else(|| unexpected_eof())?;
      let exptime = exptime.parse().map_err(|_| invalid_data())?;

      let keys = chunks.map(Into::into).collect();

      Ok(TextCommand::GetAndTouchM { keys, exptime })
    }

    "cas" if args.ends_with("noreply") => todo!(),
    "cas" => todo!(),
    "set" if args.ends_with("noreply") => read_set_command(&mut r, args).await.map(TextCommand::SetQ),
    "set" => read_set_command(&mut r, args).await.map(TextCommand::Set),
    "add" if args.ends_with("noreply") => read_set_command(&mut r, args).await.map(TextCommand::AddQ),
    "add" => read_set_command(&mut r, args).await.map(TextCommand::Add),
    "replace" if args.ends_with("noreply") => read_set_command(&mut r, args).await.map(TextCommand::ReplaceQ),
    "replace" => read_set_command(&mut r, args).await.map(TextCommand::Replace),
    "append" if args.ends_with("noreply") => read_append_prepend_command(&mut r, args)
      .await
      .map(TextCommand::AppendQ),
    "append" => read_append_prepend_command(&mut r, args).await.map(TextCommand::Append),
    "prepend" if args.ends_with("noreply") => read_append_prepend_command(&mut r, args)
      .await
      .map(TextCommand::PrependQ),
    "prepend" => read_append_prepend_command(&mut r, args)
      .await
      .map(TextCommand::Prepend),
    "delete" if args.ends_with("noreply") => decode_delete_command(args).map(TextCommand::DeleteQ),
    "delete" => decode_delete_command(args).map(TextCommand::Delete),
    "incr" if args.ends_with("noreply") => decode_incr_decr_command(args).map(TextCommand::IncrQ),
    "incr" => decode_incr_decr_command(args).map(TextCommand::Incr),
    "decr" if args.ends_with("noreply") => decode_incr_decr_command(args).map(TextCommand::DecrQ),
    "decr" => decode_incr_decr_command(args).map(TextCommand::Decr),
    "touch" if args.ends_with("noreply") => decode_touch_command(args).map(TextCommand::TouchQ),
    "touch" => decode_touch_command(args).map(TextCommand::Touch),
    "flush_all" => Ok(TextCommand::Flush),
    "version" => Ok(TextCommand::Version),
    "stats" => Ok(TextCommand::Stats),
    "quit" => Ok(TextCommand::Quit),
    _ => Err(io::Error::new(io::ErrorKind::Other, "invalid command")),
  }
}

pub async fn read_binary_command(mut r: impl AsyncRead + Unpin) -> io::Result<BinaryCommand> {
  let mut buffer = BytesMut::with_capacity(24);
  r.read_buf(&mut buffer).await?;

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
    r.read_buf(&mut buffer).await?;
  }

  if magic != 0x80 {
    return Err(io::Error::new(io::ErrorKind::InvalidData, "unexpected magic byte for request").into());
  }

  if data_type != 0x00 {
    return Err(io::Error::new(io::ErrorKind::InvalidData, "unexpected data_type byte for request").into());
  }

  let header = Header {
    op,
    key_len,
    extras_len,
    data_type,
    status,
    body_len,
    opaque,
    cas,
  };

  let buffer = buffer.freeze();

  fn decode_key_command(header: &Header, mut body: Bytes) -> io::Result<KeyCommand> {
    let key_len = header.key_len;
    let extras_len = header.extras_len;
    let body_len = header.body_len;
    assert_eq!(0, extras_len);
    assert!(key_len > 0);
    assert_eq!(key_len, body_len);

    let key = body.get(0..key_len).ok_or_else(|| unexpected_eof())?;
    let key = std::str::from_utf8(key).map_err(|_| invalid_data())?;
    let key = key.to_string();

    body.advance(key_len);
    assert!(body.is_empty());

    Ok(KeyCommand { key })
  }

  fn decode_set_command(header: &Header, mut body: Bytes) -> io::Result<SetCommand> {
    let key_len = header.key_len;
    let extras_len = header.extras_len;
    let body_len = header.body_len;
    assert_eq!(8, extras_len);
    assert!(key_len > 0);
    assert!(body_len >= key_len + extras_len);

    let flags = body.get_u32();
    let exptime = body.get_u32();

    let value_len = body_len - key_len - extras_len;

    let key = body.get(0..key_len).ok_or_else(|| unexpected_eof())?;
    let key = std::str::from_utf8(key).map_err(|_| invalid_data())?;
    let key = key.to_string();
    body.advance(key_len);

    let value = body.get(0..value_len).ok_or_else(|| unexpected_eof())?;
    let value = value.to_vec();
    body.advance(value_len);

    assert!(body.is_empty());

    let cas = None;
    Ok(SetCommand {
      key,
      value,
      flags,
      exptime,
      cas,
    })
  }

  fn decode_append_prepend_command(header: &Header, mut body: Bytes) -> io::Result<AppendPrependCommand> {
    let key_len = header.key_len;
    let extras_len = header.extras_len;
    let body_len = header.body_len;
    assert_eq!(0, extras_len);
    assert!(key_len > 0);
    assert!(body_len >= key_len);

    let key = body.get(0..key_len).ok_or_else(|| unexpected_eof())?;
    let key = std::str::from_utf8(key).map_err(|_| invalid_data())?;
    let key = key.to_string();
    body.advance(key_len);

    let value_len = body_len - key_len;
    let value = body.get(0..value_len).ok_or_else(|| unexpected_eof())?;
    let value = value.to_vec();
    body.advance(value_len);

    assert!(body.is_empty());

    Ok(AppendPrependCommand { key, value })
  }

  fn decode_incr_decr_command(header: &Header, mut body: Bytes) -> io::Result<IncrDecrCommand> {
    let key_len = header.key_len;
    let extras_len = header.extras_len;
    let body_len = header.body_len;
    assert_eq!(20, extras_len);
    assert!(key_len > 0);
    assert_eq!(body_len, key_len + extras_len);

    let delta = body.get_u64();
    let init = body.get_u64();
    let exptime = body.get_u32();

    let key = body.get(0..key_len).ok_or_else(|| unexpected_eof())?;
    let key = std::str::from_utf8(key).map_err(|_| invalid_data())?;
    let key = key.to_string();

    body.advance(key_len);
    assert!(body.is_empty());

    Ok(IncrDecrCommand {
      key,
      delta,
      init,
      exptime,
    })
  }

  fn decode_touch_command(header: &Header, mut body: Bytes) -> io::Result<TouchCommand> {
    let key_len = header.key_len;
    let extras_len = header.extras_len;
    let body_len = header.body_len;
    assert_eq!(4, extras_len);
    assert!(key_len > 0);
    assert_eq!(body_len, key_len + extras_len);

    let exptime = body.get_u32();

    let key = body.get(0..key_len).ok_or_else(|| unexpected_eof())?;
    let key = std::str::from_utf8(key).map_err(|_| invalid_data())?;
    let key = key.to_string();

    body.advance(key_len);
    assert!(body.is_empty());

    Ok(TouchCommand { key, exptime })
  }

  fn decode_command(header: &Header, mut _body: Bytes) -> io::Result<()> {
    assert_eq!(0, header.extras_len);
    assert_eq!(0, header.key_len);
    assert_eq!(0, header.body_len);
    Ok(())
  }

  match op {
    0x00 => decode_key_command(&header, buffer).map(BinaryCommand::Get),
    0x09 => decode_key_command(&header, buffer).map(BinaryCommand::GetQ),
    0x0c => decode_key_command(&header, buffer).map(BinaryCommand::GetK),
    0x0d => decode_key_command(&header, buffer).map(BinaryCommand::GetKQ),
    0x04 => decode_key_command(&header, buffer).map(BinaryCommand::Delete),
    0x14 => decode_key_command(&header, buffer).map(BinaryCommand::DeleteQ),
    0x01 => decode_set_command(&header, buffer).map(BinaryCommand::Set),
    0x11 => decode_set_command(&header, buffer).map(BinaryCommand::SetQ),
    0x02 => decode_set_command(&header, buffer).map(BinaryCommand::Add),
    0x12 => decode_set_command(&header, buffer).map(BinaryCommand::AddQ),
    0x03 => decode_set_command(&header, buffer).map(BinaryCommand::Replace),
    0x13 => decode_set_command(&header, buffer).map(BinaryCommand::ReplaceQ),
    0x0e => decode_append_prepend_command(&header, buffer).map(BinaryCommand::Append),
    0x19 => decode_append_prepend_command(&header, buffer).map(BinaryCommand::AppendQ),
    0x0f => decode_append_prepend_command(&header, buffer).map(BinaryCommand::Prepend),
    0x1a => decode_append_prepend_command(&header, buffer).map(BinaryCommand::PrependQ),
    0x05 => decode_incr_decr_command(&header, buffer).map(BinaryCommand::Incr),
    0x15 => decode_incr_decr_command(&header, buffer).map(BinaryCommand::IncrQ),
    0x06 => decode_incr_decr_command(&header, buffer).map(BinaryCommand::Decr),
    0x16 => decode_incr_decr_command(&header, buffer).map(BinaryCommand::DecrQ),
    0x1c => decode_touch_command(&header, buffer).map(BinaryCommand::Touch),
    0x1d => decode_touch_command(&header, buffer).map(BinaryCommand::GetAndTouch),
    0x1e => decode_touch_command(&header, buffer).map(BinaryCommand::GetAndTouchQ),
    0x07 => decode_command(&header, buffer).map(|_| BinaryCommand::Quit),
    0x17 => decode_command(&header, buffer).map(|_| BinaryCommand::QuitQ),
    0x08 => decode_command(&header, buffer).map(|_| BinaryCommand::Flush),
    0x18 => decode_command(&header, buffer).map(|_| BinaryCommand::FlushQ),
    0x0a => decode_command(&header, buffer).map(|_| BinaryCommand::Noop),
    0x0b => decode_command(&header, buffer).map(|_| BinaryCommand::Version),
    0x10 => decode_command(&header, buffer).map(|_| BinaryCommand::Stats),
    _ => Err(io::Error::new(io::ErrorKind::Other, "invalid command")),
  }
}

pub async fn write_binary_command(mut w: impl AsyncWrite + Unpin, command: &BinaryCommand) -> io::Result<()> {
  async fn write_request_header(mut w: impl AsyncWrite + Unpin, h: &Header) -> io::Result<()> {
    w.write_u8(0x80).await?;
    w.write_u8(h.op).await?;
    w.write_u16(h.key_len.try_into().unwrap()).await?;
    w.write_u8(h.extras_len.try_into().unwrap()).await?;
    w.write_u8(h.data_type).await?;
    w.write_u16(h.status).await?;
    w.write_u32(h.body_len.try_into().unwrap()).await?;
    w.write_u32(h.opaque).await?;
    w.write_u64(h.cas).await?;
    Ok(())
  }

  async fn write_key_command(mut w: impl AsyncWrite + Unpin, op: u8, cmd: &KeyCommand) -> io::Result<()> {
    let key_len = cmd.key.len();
    let body_len = key_len;
    write_request_header(
      &mut w,
      &Header {
        op,
        key_len,
        body_len,
        ..Default::default()
      },
    )
    .await?;
    w.write_all(cmd.key.as_bytes()).await?;
    Ok(())
  }

  async fn write_simple_command(mut w: impl AsyncWrite + Unpin, op: u8) -> io::Result<()> {
    write_request_header(
      &mut w,
      &Header {
        op,
        ..Default::default()
      },
    )
    .await
  }

  async fn write_set_command(mut w: impl AsyncWrite + Unpin, op: u8, cmd: &SetCommand) -> io::Result<()> {
    let key_len = cmd.key.len();
    let extras_len = 8;
    let value_len = cmd.value.len();
    let body_len = key_len + extras_len + value_len;
    let cas = cmd.cas.unwrap_or_default();
    write_request_header(
      &mut w,
      &Header {
        op,
        key_len,
        extras_len,
        body_len,
        cas,
        ..Default::default()
      },
    )
    .await?;
    w.write_u32(cmd.flags).await?;
    w.write_u32(cmd.exptime).await?;
    w.write_all(cmd.key.as_bytes()).await?;
    w.write_all(cmd.value.as_slice()).await?;
    Ok(())
  }

  async fn write_append_prepend_command(
    mut w: impl AsyncWrite + Unpin,
    op: u8,
    cmd: &AppendPrependCommand,
  ) -> io::Result<()> {
    let key_len = cmd.key.len();
    let value_len = cmd.value.len();
    let body_len = key_len + value_len;
    write_request_header(
      &mut w,
      &Header {
        op,
        key_len,
        body_len,
        ..Default::default()
      },
    )
    .await?;
    w.write_all(cmd.key.as_bytes()).await?;
    w.write_all(cmd.value.as_slice()).await?;
    Ok(())
  }

  async fn write_incr_decr_command(mut w: impl AsyncWrite + Unpin, op: u8, cmd: &IncrDecrCommand) -> io::Result<()> {
    let key_len = cmd.key.len();
    let extras_len = 20;
    let body_len = key_len + extras_len;
    write_request_header(
      &mut w,
      &Header {
        op,
        key_len,
        extras_len,
        body_len,
        ..Default::default()
      },
    )
    .await?;
    w.write_u64(cmd.delta).await?;
    w.write_u64(cmd.init).await?;
    w.write_u32(cmd.exptime).await?;
    w.write_all(cmd.key.as_bytes()).await?;
    Ok(())
  }

  async fn write_touch_command(mut w: impl AsyncWrite + Unpin, op: u8, cmd: &TouchCommand) -> io::Result<()> {
    let key_len = cmd.key.len();
    let extras_len = 4;
    let body_len = key_len + extras_len;
    write_request_header(
      &mut w,
      &Header {
        op,
        key_len,
        extras_len,
        body_len,
        ..Default::default()
      },
    )
    .await?;
    w.write_u32(cmd.exptime).await?;
    w.write_all(cmd.key.as_bytes()).await?;
    Ok(())
  }

  match command {
    BinaryCommand::GetQ(cmd) => write_key_command(&mut w, 0x09, cmd).await,
    BinaryCommand::Get(cmd) => write_key_command(&mut w, 0x00, cmd).await,
    BinaryCommand::GetKQ(cmd) => write_key_command(&mut w, 0x0d, cmd).await,
    BinaryCommand::GetK(cmd) => write_key_command(&mut w, 0x0c, cmd).await,
    BinaryCommand::GetAndTouchQ(cmd) => write_touch_command(&mut w, 0x1e, cmd).await,
    BinaryCommand::GetAndTouch(cmd) => write_touch_command(&mut w, 0x1d, cmd).await,
    BinaryCommand::Set(cmd) => write_set_command(&mut w, 0x01, cmd).await,
    BinaryCommand::SetQ(cmd) => write_set_command(&mut w, 0x11, cmd).await,
    BinaryCommand::Add(cmd) => write_set_command(&mut w, 0x02, cmd).await,
    BinaryCommand::AddQ(cmd) => write_set_command(&mut w, 0x12, cmd).await,
    BinaryCommand::Replace(cmd) => write_set_command(&mut w, 0x03, cmd).await,
    BinaryCommand::ReplaceQ(cmd) => write_set_command(&mut w, 0x13, cmd).await,
    BinaryCommand::Append(cmd) => write_append_prepend_command(&mut w, 0x0e, cmd).await,
    BinaryCommand::AppendQ(cmd) => write_append_prepend_command(&mut w, 0x19, cmd).await,
    BinaryCommand::Prepend(cmd) => write_append_prepend_command(&mut w, 0x0f, cmd).await,
    BinaryCommand::PrependQ(cmd) => write_append_prepend_command(&mut w, 0x1a, cmd).await,
    BinaryCommand::DeleteQ(cmd) => write_key_command(&mut w, 0x14, cmd).await,
    BinaryCommand::Delete(cmd) => write_key_command(&mut w, 0x04, cmd).await,
    BinaryCommand::IncrQ(cmd) => write_incr_decr_command(&mut w, 0x15, cmd).await,
    BinaryCommand::Incr(cmd) => write_incr_decr_command(&mut w, 0x05, cmd).await,
    BinaryCommand::DecrQ(cmd) => write_incr_decr_command(&mut w, 0x16, cmd).await,
    BinaryCommand::Decr(cmd) => write_incr_decr_command(&mut w, 0x06, cmd).await,
    BinaryCommand::Touch(cmd) => write_touch_command(&mut w, 0x1c, cmd).await,
    BinaryCommand::FlushQ => write_simple_command(&mut w, 0x18).await,
    BinaryCommand::Flush => write_simple_command(&mut w, 0x08).await,
    BinaryCommand::Version => write_simple_command(&mut w, 0x0b).await,
    BinaryCommand::Stats => write_simple_command(&mut w, 0x10).await,
    BinaryCommand::QuitQ => write_simple_command(&mut w, 0x17).await,
    BinaryCommand::Quit => write_simple_command(&mut w, 0x07).await,
    BinaryCommand::Noop => write_simple_command(&mut w, 0x0a).await,
  }
}

pub async fn write_text_command(mut w: impl AsyncWrite + Unpin, command: &TextCommand) -> io::Result<()> {
  async fn write_set_command(
    mut w: impl AsyncWrite + Unpin,
    op: &str,
    noreply: bool,
    cmd: &SetCommand,
  ) -> io::Result<()> {
    match (op, cmd.cas) {
      ("set", Some(cas)) => {
        w.write_all(
          format!(
            "cas {} {} {} {} {}",
            cmd.key,
            cmd.flags,
            cmd.exptime,
            cmd.value.len(),
            cas,
          )
          .as_bytes(),
        )
        .await?
      }
      (_, Some(_)) => return Err(io::Error::new(io::ErrorKind::InvalidInput, "not supported")),
      (op, None) => {
        w.write_all(format!("{} {} {} {} {}", op, cmd.key, cmd.flags, cmd.exptime, cmd.value.len()).as_bytes())
          .await?;
      }
    };
    if noreply {
      w.write_all(b" noreply").await?;
    }
    w.write_all(b"\r\n").await?;
    w.write_all(cmd.value.as_slice()).await?;
    w.write_all(b"\r\n").await?;
    Ok(())
  }

  async fn write_append_prepend_command(
    mut w: impl AsyncWrite + Unpin,
    op: &str,
    noreply: bool,
    cmd: &AppendPrependCommand,
  ) -> io::Result<()> {
    w.write_all(format!("{} {} 0 0 {}", op, cmd.key, cmd.value.len()).as_bytes())
      .await?;
    if noreply {
      w.write_all(b" noreply").await?;
    }
    w.write_all(b"\r\n").await?;
    w.write_all(cmd.value.as_slice()).await?;
    w.write_all(b"\r\n").await?;
    Ok(())
  }

  match command {
    TextCommand::GetM { keys } => {
      w.write_all(format!("get {}\r\n", keys.as_slice().join(" ")).as_bytes())
        .await
    }
    TextCommand::GetAndTouchM { keys, exptime } => {
      w.write_all(format!("gat {} {}\r\n", exptime, keys.as_slice().join(" ")).as_bytes())
        .await
    }
    TextCommand::Set(cmd) => write_set_command(&mut w, "set", false, cmd).await,
    TextCommand::SetQ(cmd) => write_set_command(&mut w, "set", true, cmd).await,
    TextCommand::Add(cmd) => write_set_command(&mut w, "add", false, cmd).await,
    TextCommand::AddQ(cmd) => write_set_command(&mut w, "add", true, cmd).await,
    TextCommand::Replace(cmd) => write_set_command(&mut w, "replace", false, cmd).await,
    TextCommand::ReplaceQ(cmd) => write_set_command(&mut w, "replace", true, cmd).await,
    TextCommand::Append(cmd) => write_append_prepend_command(&mut w, "append", false, cmd).await,
    TextCommand::AppendQ(cmd) => write_append_prepend_command(&mut w, "append", true, cmd).await,
    TextCommand::Prepend(cmd) => write_append_prepend_command(&mut w, "prepend", false, cmd).await,
    TextCommand::PrependQ(cmd) => write_append_prepend_command(&mut w, "prepend", true, cmd).await,
    TextCommand::DeleteQ(KeyCommand { key }) => w.write_all(format!("delete {key} noreply\r\n").as_bytes()).await,
    TextCommand::Delete(KeyCommand { key }) => w.write_all(format!("delete {key}\r\n").as_bytes()).await,
    TextCommand::IncrQ(TextIncrDecrCommand { key, delta, .. }) => {
      w.write_all(format!("incr {key} {delta} noreply\r\n").as_bytes()).await
    }
    TextCommand::Incr(TextIncrDecrCommand { key, delta, .. }) => {
      w.write_all(format!("incr {key} {delta}\r\n").as_bytes()).await
    }
    TextCommand::DecrQ(TextIncrDecrCommand { key, delta, .. }) => {
      w.write_all(format!("decr {key} {delta} noreply\r\n").as_bytes()).await
    }
    TextCommand::Decr(TextIncrDecrCommand { key, delta, .. }) => {
      w.write_all(format!("decr {key} {delta}\r\n").as_bytes()).await
    }
    TextCommand::TouchQ(TouchCommand { key, exptime }) => {
      w.write_all(format!("touch {key} {exptime} noreply\r\n").as_bytes())
        .await
    }
    TextCommand::Touch(TouchCommand { key, exptime }) => {
      w.write_all(format!("touch {key} {exptime}\r\n").as_bytes()).await
    }
    TextCommand::Flush => w.write_all(b"flush_all\r\n").await,
    TextCommand::Version => w.write_all(b"version\r\n").await,
    TextCommand::Stats => w.write_all(b"stats\r\n").await,
    TextCommand::Quit => w.write_all(b"quit\r\n").await,
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

  use crate::codec::Command;

  use super::{
    read_binary_command, read_command, read_text_command, write_binary_command, write_text_command,
    AppendPrependCommand, BinaryCommand, IncrDecrCommand, KeyCommand, SetCommand, TextCommand, TextIncrDecrCommand,
    TouchCommand,
  };

  #[tokio::test]
  async fn test_decode_text_command() {
    let tests: &[(&[u8], _)] = &[
      (
        b"get foo\r\n",
        Ok(TextCommand::GetM {
          keys: vec!["foo".to_string()],
        }),
      ),
      (
        b"get foo bar\r\n",
        Ok(TextCommand::GetM {
          keys: vec!["foo".to_string(), "bar".to_string()],
        }),
      ),
      (
        b"gets foo\r\n",
        Ok(TextCommand::GetM {
          keys: vec!["foo".to_string()],
        }),
      ),
      (
        b"gets foo bar\r\n",
        Ok(TextCommand::GetM {
          keys: vec!["foo".to_string(), "bar".to_string()],
        }),
      ),
      (
        b"touch foo 123\r\n",
        Ok(TextCommand::Touch(TouchCommand {
          key: "foo".to_string(),
          exptime: 123,
        })),
      ),
      (
        b"touch foo 123 noreply\r\n",
        Ok(TextCommand::TouchQ(TouchCommand {
          key: "foo".to_string(),
          exptime: 123,
        })),
      ),
      (
        b"incr foo 2\r\n",
        Ok(TextCommand::Incr(TextIncrDecrCommand {
          key: "foo".to_string(),
          delta: 2,
        })),
      ),
      (
        b"incr foo 2 noreply\r\n",
        Ok(TextCommand::IncrQ(TextIncrDecrCommand {
          key: "foo".to_string(),
          delta: 2,
        })),
      ),
      (
        b"decr foo 2\r\n",
        Ok(TextCommand::Decr(TextIncrDecrCommand {
          key: "foo".to_string(),
          delta: 2,
        })),
      ),
      (
        b"decr foo 2 noreply\r\n",
        Ok(TextCommand::DecrQ(TextIncrDecrCommand {
          key: "foo".to_string(),
          delta: 2,
        })),
      ),
      (
        b"delete foo\r\n",
        Ok(TextCommand::Delete(KeyCommand { key: "foo".to_string() })),
      ),
      (
        b"delete foo noreply\r\n",
        Ok(TextCommand::DeleteQ(KeyCommand { key: "foo".to_string() })),
      ),
      (
        b"set foo 123 321 3\r\nbar\r\n",
        Ok(TextCommand::Set(SetCommand {
          key: "foo".to_string(),
          value: b"bar".to_vec(),
          flags: 123,
          exptime: 321,
          cas: None,
        })),
      ),
      (
        b"set foo 123 321 0\r\n\r\n",
        Ok(TextCommand::Set(SetCommand {
          key: "foo".to_string(),
          value: b"".to_vec(),
          flags: 123,
          exptime: 321,
          cas: None,
        })),
      ),
      (
        b"set foo 123 321 3 noreply\r\nbar\r\n",
        Ok(TextCommand::SetQ(SetCommand {
          key: "foo".to_string(),
          value: b"bar".to_vec(),
          flags: 123,
          exptime: 321,
          cas: None,
        })),
      ),
      // (b"set foo 123 321 1\r\nbar\r\n", Err(io::ErrorKind::InvalidData)),
      // (b"set foo 123 321 1\r\nb", Err(io::ErrorKind::UnexpectedEof)),
      // (b"set foo 123 321\r\nb\r\n", Err(io::ErrorKind::UnexpectedEof)),
      (
        b"add foo 123 321 3\r\nbar\r\n",
        Ok(TextCommand::Add(SetCommand {
          key: "foo".to_string(),
          value: b"bar".to_vec(),
          flags: 123,
          exptime: 321,
          cas: None,
        })),
      ),
      (
        b"add foo 123 321 3 noreply\r\nbar\r\n",
        Ok(TextCommand::AddQ(SetCommand {
          key: "foo".to_string(),
          value: b"bar".to_vec(),
          flags: 123,
          exptime: 321,
          cas: None,
        })),
      ),
      (
        b"replace foo 123 321 3\r\nbar\r\n",
        Ok(TextCommand::Replace(SetCommand {
          key: "foo".to_string(),
          value: b"bar".to_vec(),
          flags: 123,
          exptime: 321,
          cas: None,
        })),
      ),
      (
        b"replace foo 123 321 3 noreply\r\nbar\r\n",
        Ok(TextCommand::ReplaceQ(SetCommand {
          key: "foo".to_string(),
          value: b"bar".to_vec(),
          flags: 123,
          exptime: 321,
          cas: None,
        })),
      ),
      (
        b"append foo 123 321 3\r\nbar\r\n",
        Ok(TextCommand::Append(AppendPrependCommand {
          key: "foo".to_string(),
          value: b"bar".to_vec(),
        })),
      ),
      (
        b"append foo 123 321 3 noreply\r\nbar\r\n",
        Ok(TextCommand::AppendQ(AppendPrependCommand {
          key: "foo".to_string(),
          value: b"bar".to_vec(),
        })),
      ),
      (
        b"prepend foo 123 321 3\r\nbar\r\n",
        Ok(TextCommand::Prepend(AppendPrependCommand {
          key: "foo".to_string(),
          value: b"bar".to_vec(),
        })),
      ),
      (
        b"prepend foo 123 321 3 noreply\r\nbar\r\n",
        Ok(TextCommand::PrependQ(AppendPrependCommand {
          key: "foo".to_string(),
          value: b"bar".to_vec(),
        })),
      ),
      // (
      //   b"cas foo 123 321 3 567\r\nbar\r\n",
      //   Ok(TextCommand::Set(SetCommand {
      //     key: "foo".to_string(),
      //     value: b"bar".to_vec(),
      //     flags: 123,
      //     exptime: 321,
      //     cas: Some(567),
      //   })),
      // ),
      // (
      //   b"cas foo 123 321 0 567\r\n\r\n",
      //   Ok(TextCommand::Set(SetCommand {
      //     key: "foo".to_string(),
      //     value: b"".to_vec(),
      //     flags: 123,
      //     exptime: 321,
      //     cas: Some(567),
      //   })),
      // ),
      // (
      //   b"cas foo 123 321 3 567 noreply\r\nbar\r\n",
      //   Ok(TextCommand::SetQ(SetCommand {
      //     key: "foo".to_string(),
      //     value: b"bar".to_vec(),
      //     flags: 123,
      //     exptime: 321,
      //     cas: Some(567),
      //   })),
      // ),
      (b"flush_all\r\n", Ok(TextCommand::Flush)),
      (b"version\r\n", Ok(TextCommand::Version)),
      (b"stats\r\n", Ok(TextCommand::Stats)),
      (b"quit\r\n", Ok(TextCommand::Quit)),
    ];

    for t in tests {
      assert_eq!(t.1, read_text_command(t.0).await.map_err(|err| err.kind()));
    }
  }

  #[tokio::test]
  async fn test_text_command_roundtrip() {
    let tests = &[
      TextCommand::GetM {
        keys: vec!["foo".to_string()],
      },
      TextCommand::GetAndTouchM {
        keys: vec!["foo".to_string()],
        exptime: 123,
      },
      TextCommand::Set(SetCommand {
        key: "foo".to_string(),
        value: b"bar".to_vec(),
        flags: 123,
        exptime: 321,
        cas: None,
      }),
      TextCommand::SetQ(SetCommand {
        key: "foo".to_string(),
        value: b"bar".to_vec(),
        flags: 123,
        exptime: 321,
        cas: None,
      }),
      TextCommand::Add(SetCommand {
        key: "foo".to_string(),
        value: b"bar".to_vec(),
        flags: 123,
        exptime: 321,
        cas: None,
      }),
      TextCommand::AddQ(SetCommand {
        key: "foo".to_string(),
        value: b"bar".to_vec(),
        flags: 123,
        exptime: 321,
        cas: None,
      }),
      TextCommand::Replace(SetCommand {
        key: "foo".to_string(),
        value: b"bar".to_vec(),
        flags: 123,
        exptime: 321,
        cas: None,
      }),
      TextCommand::ReplaceQ(SetCommand {
        key: "foo".to_string(),
        value: b"bar".to_vec(),
        flags: 123,
        exptime: 321,
        cas: None,
      }),
      TextCommand::Append(AppendPrependCommand {
        key: "foo".to_string(),
        value: b"bar".to_vec(),
      }),
      TextCommand::AppendQ(AppendPrependCommand {
        key: "foo".to_string(),
        value: b"bar".to_vec(),
      }),
      TextCommand::Prepend(AppendPrependCommand {
        key: "foo".to_string(),
        value: b"bar".to_vec(),
      }),
      TextCommand::PrependQ(AppendPrependCommand {
        key: "foo".to_string(),
        value: b"bar".to_vec(),
      }),
      // TextCommand::Set(SetCommand {
      //   key: "foo".to_string(),
      //   value: b"bar".to_vec(),
      //   flags: 123,
      //   exptime: 321,
      //   cas: Some(456),
      // }),
      // TextCommand::SetQ(SetCommand {
      //   key: "foo".to_string(),
      //   value: b"bar".to_vec(),
      //   flags: 123,
      //   exptime: 321,
      //   cas: Some(456),
      // }),
      TextCommand::Delete(KeyCommand { key: "foo".to_string() }),
      TextCommand::Incr(TextIncrDecrCommand {
        key: "foo".to_string(),
        delta: 123,
      }),
      TextCommand::Decr(TextIncrDecrCommand {
        key: "foo".to_string(),
        delta: 123,
      }),
      TextCommand::Touch(TouchCommand {
        key: "foo".to_string(),
        exptime: 123,
      }),
      TextCommand::Flush,
      TextCommand::Version,
      TextCommand::Stats,
      TextCommand::Quit,
    ];

    for expected in tests.iter() {
      let mut buffer = Vec::new();
      write_text_command(&mut buffer, expected).await.unwrap();
      let decoded = read_text_command(buffer.as_slice()).await.unwrap();

      assert_eq!(expected, &decoded);
    }
  }

  #[tokio::test]
  async fn test_binary_command_roundtrip() {
    let tests = &[
      BinaryCommand::Get(KeyCommand { key: "foo".to_string() }),
      BinaryCommand::GetAndTouch(TouchCommand {
        key: "foo".to_string(),
        exptime: 123,
      }),
      BinaryCommand::Delete(KeyCommand { key: "foo".to_string() }),
      BinaryCommand::Set(SetCommand {
        key: "foo".to_string(),
        value: b"bar".to_vec(),
        flags: 123,
        exptime: 321,
        cas: None,
      }),
      BinaryCommand::SetQ(SetCommand {
        key: "foo".to_string(),
        value: b"bar".to_vec(),
        flags: 123,
        exptime: 321,
        cas: None,
      }),
      BinaryCommand::Add(SetCommand {
        key: "foo".to_string(),
        value: b"bar".to_vec(),
        flags: 123,
        exptime: 321,
        cas: None,
      }),
      BinaryCommand::AddQ(SetCommand {
        key: "foo".to_string(),
        value: b"bar".to_vec(),
        flags: 123,
        exptime: 321,
        cas: None,
      }),
      BinaryCommand::Replace(SetCommand {
        key: "foo".to_string(),
        value: b"bar".to_vec(),
        flags: 123,
        exptime: 321,
        cas: None,
      }),
      BinaryCommand::ReplaceQ(SetCommand {
        key: "foo".to_string(),
        value: b"bar".to_vec(),
        flags: 123,
        exptime: 321,
        cas: None,
      }),
      BinaryCommand::Append(AppendPrependCommand {
        key: "foo".to_string(),
        value: b"bar".to_vec(),
      }),
      BinaryCommand::AppendQ(AppendPrependCommand {
        key: "foo".to_string(),
        value: b"bar".to_vec(),
      }),
      BinaryCommand::Prepend(AppendPrependCommand {
        key: "foo".to_string(),
        value: b"bar".to_vec(),
      }),
      BinaryCommand::PrependQ(AppendPrependCommand {
        key: "foo".to_string(),
        value: b"bar".to_vec(),
      }),
      BinaryCommand::Incr(IncrDecrCommand {
        key: "foo".to_string(),
        delta: 1,
        init: 0,
        exptime: 0,
      }),
      BinaryCommand::IncrQ(IncrDecrCommand {
        key: "foo".to_string(),
        delta: 1,
        init: 0,
        exptime: 0,
      }),
      BinaryCommand::Decr(IncrDecrCommand {
        key: "foo".to_string(),
        delta: 1,
        init: 0,
        exptime: 0,
      }),
      BinaryCommand::DecrQ(IncrDecrCommand {
        key: "foo".to_string(),
        delta: 1,
        init: 0,
        exptime: 0,
      }),
      BinaryCommand::FlushQ,
      BinaryCommand::Flush,
      BinaryCommand::Version,
      BinaryCommand::Stats,
      BinaryCommand::QuitQ,
      BinaryCommand::Quit,
    ];

    for expected in tests.iter() {
      let mut buffer = Vec::new();
      write_binary_command(&mut buffer, expected).await.unwrap();
      let decoded = read_binary_command(buffer.as_slice()).await.unwrap();

      assert_eq!(expected, &decoded);
    }
  }

  #[tokio::test]
  async fn test_read_command() {
    let command = TextCommand::Set(SetCommand {
      key: "foo".to_string(),
      value: b"bar".to_vec(),
      flags: 0,
      exptime: 0,
      cas: None,
    });
    let mut buffer = Vec::new();
    write_text_command(&mut buffer, &command).await.unwrap();
    assert_eq!(Command::Text(command), read_command(buffer.as_slice()).await.unwrap());

    let command = BinaryCommand::Set(SetCommand {
      key: "foo".to_string(),
      value: b"bar".to_vec(),
      flags: 0,
      exptime: 0,
      cas: None,
    });
    let mut buffer = Vec::new();
    write_binary_command(&mut buffer, &command).await.unwrap();
    assert_eq!(Command::Binary(command), read_command(buffer.as_slice()).await.unwrap());
  }
}
