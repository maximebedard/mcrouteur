use std::io;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt};

#[derive(Debug)]
pub struct Connection;

#[derive(Debug, PartialEq)]
pub enum Command<'a> {
  Get {
    key: &'a str,
    noreply: bool,
  },
  GetK {
    key: &'a str,
    noreply: bool,
  },
  GetAndTouch {
    key: &'a str,
    exptime: u32,
    noreply: bool,
  },
  Set {
    key: &'a str,
    value: &'a [u8],
    flags: u32,
    exptime: u32,
    noreply: bool,
    cas: Option<u64>,
  },
  Add {
    key: &'a str,
    value: &'a [u8],
    flags: u32,
    exptime: u32,
    noreply: bool,
    cas: Option<u64>,
  },
  Replace {
    key: &'a str,
    value: &'a [u8],
    flags: u32,
    exptime: u32,
    noreply: bool,
    cas: Option<u64>,
  },
  Append {
    key: &'a str,
    value: &'a [u8],
    flags: u32,
    exptime: u32,
    noreply: bool,
    cas: Option<u64>,
  },
  Prepend {
    key: &'a str,
    value: &'a [u8],
    flags: u32,
    exptime: u32,
    noreply: bool,
    cas: Option<u64>,
  },
  Delete {
    key: &'a str,
    noreply: bool,
  },
  Incr {
    key: &'a str,
    value: u64,
    noreply: bool,
  },
  Decr {
    key: &'a str,
    value: u64,
    noreply: bool,
  },
  Touch {
    key: &'a str,
    exptime: u32,
    noreply: bool,
  },
  Flush {
    noreply: bool,
  },
  Version,
  Stats,
  Quit {
    noreply: bool,
  },
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

      let noreply = false;

      Ok(input.split(' ').map(|key| Command::Get { key, noreply }).collect())
    }
    b"gat" | b"gats" => {
      let input = new_line_str(input)?;

      let mut chunks = input.split(' ');

      let exptime = chunks.next().ok_or(DecodeCommandError::UnexpectedEof)?;
      let exptime = exptime.parse().map_err(|_| DecodeCommandError::InvalidFormat)?;

      let noreply = false;

      Ok(
        chunks
          .map(|key| Command::GetAndTouch { key, exptime, noreply })
          .collect(),
      )
    }

    b"set" | b"add" | b"replace" | b"append" | b"prepend" | b"cas" => {
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

      match command {
        b"set" | b"cas" => Ok(vec![Command::Set {
          key,
          value,
          flags,
          exptime,
          noreply,
          cas,
        }]),
        b"add" => Ok(vec![Command::Add {
          key,
          value,
          flags,
          exptime,
          noreply,
          cas,
        }]),
        b"replace" => Ok(vec![Command::Replace {
          key,
          value,
          flags,
          exptime,
          noreply,
          cas,
        }]),
        b"append" => Ok(vec![Command::Append {
          key,
          value,
          flags,
          exptime,
          noreply,
          cas,
        }]),
        b"prepend" => Ok(vec![Command::Prepend {
          key,
          value,
          flags,
          exptime,
          noreply,
          cas,
        }]),
        _ => unreachable!(),
      }
    }

    b"delete" => {
      let input = new_line_str(input)?;

      let mut chunks = input.split(' ');

      let key = chunks.next().ok_or(DecodeCommandError::UnexpectedEof)?;

      let noreply = chunks.next().filter(|v| *v == "noreply").is_some();

      Ok(vec![Command::Delete { key, noreply }])
    }
    b"incr" | b"decr" => {
      let input = new_line_str(input)?;

      let mut chunks = input.split(' ');

      let key = chunks.next().ok_or(DecodeCommandError::UnexpectedEof)?;

      let value = chunks.next().ok_or(DecodeCommandError::UnexpectedEof)?;
      let value = value.parse().map_err(|_| DecodeCommandError::InvalidFormat)?;

      let noreply = chunks.next().filter(|v| *v == "noreply").is_some();

      match command {
        b"incr" => Ok(vec![Command::Incr { key, value, noreply }]),
        b"decr" => Ok(vec![Command::Decr { key, value, noreply }]),
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

      Ok(vec![Command::Touch { key, exptime, noreply }])
    }
    b"flush_all\r\n" | b"flush_all\n" => Ok(vec![Command::Flush { noreply: false }]),
    b"version\r\n" | b"version\n" => Ok(vec![Command::Version]),
    b"stats\r\n" | b"stats\n" => Ok(vec![Command::Stats]),
    b"quit\r\n" | b"quit\n" => Ok(vec![Command::Quit { noreply: false }]),
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
  let _opaque = input.get_u32();
  let _cas = input.get_u64();

  if input.len() != body_len {
    return Err(DecodeCommandError::UnexpectedEof);
  }

  match op {
    0x00 | 0x09 | 0x0c | 0x0d | 0x04 | 0x14 => {
      // Get/GetQ/GetK/GetKQ/Delete/DeleteQ
      assert_eq!(0, extras_len);
      assert_eq!(key_len, body_len);
      assert!(key_len > 0);

      let key = input.get(0..key_len).ok_or(DecodeCommandError::UnexpectedEof)?;
      input.advance(key_len);
      assert!(input.is_empty());

      let key = std::str::from_utf8(key).map_err(|_| DecodeCommandError::InvalidFormat)?;

      let noreply = 0x09 == op || 0x0d == op || 0x14 == op;
      match op {
        0x00 | 0x09 => Ok(Command::Get { key, noreply }),
        0x0c | 0x0d => Ok(Command::GetK { key, noreply }),
        0x04 | 0x14 => Ok(Command::Delete { key, noreply }),
        _ => unreachable!(),
      }
    }

    0x01 | 0x02 | 0x03 | 0x11 | 0x12 | 0x13 => {
      // Set/Add/Replace/SetQ/AddQ/ReplaceQ
      assert_eq!(8, extras_len);
      assert!(key_len > 0);
      assert!(body_len >= key_len + extras_len);

      let flags = input.get_u32();
      let exptime = input.get_u32();

      let value_len = body_len - key_len - extras_len;

      let key = input.get(0..key_len).ok_or(DecodeCommandError::UnexpectedEof)?;
      input.advance(key_len);

      let value = input.get(0..value_len).ok_or(DecodeCommandError::UnexpectedEof)?;
      input.advance(value_len);

      assert!(input.is_empty());

      let key = std::str::from_utf8(key).map_err(|_| DecodeCommandError::InvalidFormat)?;

      let noreply = 0x11 == op || 0x12 == op || 0x13 == op;

      let cas = None;

      match op {
        0x01 | 0x11 => Ok(Command::Set {
          key,
          value,
          flags,
          exptime,
          noreply,
          cas,
        }),
        0x02 | 0x12 => Ok(Command::Add {
          key,
          value,
          flags,
          exptime,
          noreply,
          cas,
        }),
        0x03 | 0x13 => Ok(Command::Replace {
          key,
          value,
          flags,
          exptime,
          noreply,
          cas,
        }),
        _ => unreachable!(),
      }
    }

    0x05 | 0x06 | 0x15 | 0x16 => {
      // Incr/Decr/IncrQ/DecrQ
      todo!("Incr/Decr/IncrQ/DecrQ")
    }
    0x07 | 0x17 => {
      // Quit/QuitQ
      assert_eq!(0, extras_len);
      assert_eq!(0, key_len);
      assert_eq!(0, body_len);
      let noreply = 0x17 == op;
      Ok(Command::Quit { noreply })
    }
    0x08 | 0x18 => {
      // Flush/FlushQ
      assert_eq!(0, extras_len);
      assert_eq!(0, key_len);
      assert_eq!(0, body_len);
      let noreply = 0x18 == op;
      Ok(Command::Flush { noreply })
    }
    0x0e | 0x19 | 0x0f | 0x1a => {
      // Append/AppendQ/Prepend/PrependQ
      todo!("Append/AppendQ/Prepend/PrependQ")
    }
    0x0a => {
      // Noop
      assert_eq!(0, extras_len);
      assert_eq!(0, key_len);
      assert_eq!(0, body_len);
      Ok(Command::Noop)
    }
    0x0b => {
      // Version
      assert_eq!(0, extras_len);
      assert_eq!(0, key_len);
      assert_eq!(0, body_len);
      Ok(Command::Version)
    }
    0x10 => {
      // Version
      assert_eq!(0, extras_len);
      assert_eq!(0, key_len);
      assert_eq!(0, body_len);
      Ok(Command::Stats)
    }
    0x1b => todo!("Verbosity *"),
    0x1c => todo!("Touch *"),
    0x1d => todo!("GAT *"),
    0x1e => todo!("GATQ *"),
    _ => Err(DecodeCommandError::InvalidCommand),
  }
}

#[derive(Debug, PartialEq)]
pub enum EncodeCommandError {
  NotSupported,
}

pub fn encode_binary_command(command: &Command<'_>) -> Result<Vec<u8>, EncodeCommandError> {
  fn encode_key_command(op: u8, key: &str) -> Vec<u8> {
    let mut buffer = Vec::new();
    let key_len = key.len();
    write_header(
      &mut buffer,
      Header {
        op,
        key_len,
        body_len: key_len,
        ..Default::default()
      },
    );
    buffer.put(key.as_bytes());
    buffer
  }

  fn encode_simple_command(op: u8) -> Vec<u8> {
    let mut buffer = Vec::new();
    write_header(
      &mut buffer,
      Header {
        op,
        ..Default::default()
      },
    );
    buffer
  }

  fn encode_incr_decr_command(op: u8, key: &str, value: u64) -> Vec<u8> {
    todo!()
    // let mut buffer = Vec::new();
    // let key_len = key.len();
    // let extras_len = 8;
    // write_header(
    //   &mut buffer,
    //   Header {
    //     op,
    //     key_len,
    //     extras_len,
    //     body_len: key_len + extras_len,
    //     ..Default::default()
    //   },
    // );
    // buffer.put_u32(exptime);
    // buffer.put(key.as_bytes());
    // buffer
  }

  fn encode_touch_command(op: u8, key: &str, exptime: u32) -> Vec<u8> {
    let mut buffer = Vec::new();
    let key_len = key.len();
    let extras_len = 4;
    write_header(
      &mut buffer,
      Header {
        op,
        key_len,
        extras_len,
        body_len: key_len + extras_len,
        ..Default::default()
      },
    );
    buffer.put_u32(exptime);
    buffer.put(key.as_bytes());
    buffer
  }

  match command {
    Command::Get { key, noreply: true } => Ok(encode_key_command(0x09, key)),
    Command::Get { key, noreply: _ } => Ok(encode_key_command(0x00, key)),
    Command::GetK { key, noreply: true } => Ok(encode_key_command(0x0d, key)),
    Command::GetK { key, noreply: _ } => Ok(encode_key_command(0x0c, key)),
    Command::GetAndTouch {
      key,
      exptime,
      noreply: true,
    } => Ok(encode_touch_command(0x1d, key, *exptime)),
    Command::GetAndTouch {
      key,
      exptime,
      noreply: _,
    } => Ok(encode_touch_command(0x1e, key, *exptime)),
    Command::Set {
      key,
      value,
      flags,
      exptime,
      noreply,
      cas,
    } => todo!(),
    Command::Add {
      key,
      value,
      flags,
      exptime,
      noreply,
      cas,
    } => todo!(),
    Command::Replace {
      key,
      value,
      flags,
      exptime,
      noreply,
      cas,
    } => todo!(),
    Command::Append {
      key,
      value,
      flags,
      exptime,
      noreply,
      cas,
    } => todo!(),
    Command::Prepend {
      key,
      value,
      flags,
      exptime,
      noreply,
      cas,
    } => todo!(),
    Command::Delete { key, noreply: true } => Ok(encode_key_command(0x14, key)),
    Command::Delete { key, noreply: _ } => Ok(encode_key_command(0x04, key)),
    Command::Incr {
      key,
      value,
      noreply: true,
    } => Ok(encode_incr_decr_command(0x15, key, *value)),
    Command::Incr { key, value, noreply: _ } => Ok(encode_incr_decr_command(0x05, key, *value)),
    Command::Decr {
      key,
      value,
      noreply: true,
    } => Ok(encode_incr_decr_command(0x16, key, *value)),
    Command::Decr { key, value, noreply: _ } => Ok(encode_incr_decr_command(0x06, key, *value)),
    Command::Touch { noreply: true, .. } => Err(EncodeCommandError::NotSupported),
    Command::Touch {
      key,
      exptime,
      noreply: _,
    } => Ok(encode_touch_command(0x1c, key, *exptime)),
    Command::Flush { noreply: true } => Ok(encode_simple_command(0x18)),
    Command::Flush { noreply: _ } => Ok(encode_simple_command(0x08)),
    Command::Version => Ok(encode_simple_command(0x0b)),
    Command::Stats => Ok(encode_simple_command(0x10)),
    Command::Quit { noreply: true } => Ok(encode_simple_command(0x17)),
    Command::Quit { noreply: _ } => Ok(encode_simple_command(0x07)),
    Command::Noop => Ok(encode_simple_command(0x0a)),
  }
}

pub fn encode_text_command(command: &Command<'_>) -> Result<Vec<u8>, EncodeCommandError> {
  match command {
    Command::Get { noreply: true, .. } => Err(EncodeCommandError::NotSupported),
    Command::Get { key, noreply: _ } => Ok(format!("get {key}\r\n").into_bytes()),
    Command::GetK { .. } => Err(EncodeCommandError::NotSupported),
    Command::GetAndTouch { noreply: true, .. } => Err(EncodeCommandError::NotSupported),
    Command::GetAndTouch {
      key,
      exptime,
      noreply: _,
    } => Ok(format!("gat {exptime} {key}\r\n").into_bytes()),
    Command::Set {
      key,
      value,
      flags,
      exptime,
      noreply,
      cas,
    } => {
      let value_len = value.len();
      let op = if cas.is_some() { "cas" } else { "set" };
      let mut buffer = format!("{} {} {} {} {}", op, key, flags, exptime, value_len);
      if *noreply {
        buffer.push_str(" noreply");
      }
      if let Some(cas) = cas {
        buffer.push_str(format!(" {cas}").as_str());
      }
      buffer.push_str("\r\n");
      let mut buffer = buffer.into_bytes();
      buffer.extend_from_slice(value);
      buffer.extend_from_slice(b"\r\n");
      Ok(buffer)
    }
    Command::Add {
      key,
      value,
      flags,
      exptime,
      noreply,
      cas,
    } => todo!(),
    Command::Replace {
      key,
      value,
      flags,
      exptime,
      noreply,
      cas,
    } => todo!(),
    Command::Append {
      key,
      value,
      flags,
      exptime,
      noreply,
      cas,
    } => todo!(),
    Command::Prepend {
      key,
      value,
      flags,
      exptime,
      noreply,
      cas,
    } => todo!(),
    Command::Delete { key, noreply: true } => Ok(format!("delete {key} noreply\r\n").into_bytes()),
    Command::Delete { key, noreply: _ } => Ok(format!("delete {key}\r\n").into_bytes()),
    Command::Incr {
      key,
      value,
      noreply: true,
    } => Ok(format!("incr {key} {value} noreply\r\n").into_bytes()),
    Command::Incr { key, value, noreply: _ } => Ok(format!("incr {key} {value}\r\n").into_bytes()),
    Command::Decr {
      key,
      value,
      noreply: true,
    } => Ok(format!("decr {key} {value} noreply\r\n").into_bytes()),
    Command::Decr { key, value, noreply: _ } => Ok(format!("decr {key} {value}\r\n").into_bytes()),
    Command::Touch {
      key,
      exptime,
      noreply: true,
    } => Ok(format!("touch {key} {exptime} noreply\r\n").into_bytes()),
    Command::Touch {
      key,
      exptime,
      noreply: _,
    } => Ok(format!("touch {key} {exptime}\r\n").into_bytes()),
    Command::Flush { noreply: true } => Err(EncodeCommandError::NotSupported),
    Command::Flush { .. } => Ok("flush_all\r\n".into()),
    Command::Version => Ok("version\r\n".into()),
    Command::Stats => Ok("stats\r\n".into()),
    Command::Quit { noreply: true } => Err(EncodeCommandError::NotSupported),
    Command::Quit { .. } => Ok("quit\r\n".into()),
    Command::Noop => Err(EncodeCommandError::NotSupported),
  }
}

#[derive(Debug, Default)]
struct Header {
  op: u8,
  key_len: usize,
  extras_len: usize,
  body_len: usize,
  opaque: u32,
  cas: u64,
}

fn write_header(buffer: &mut Vec<u8>, h: Header) {
  let Header {
    op,
    key_len,
    extras_len,
    body_len,
    opaque,
    cas,
  } = h;
  buffer.put_u8(0x80);
  buffer.put_u8(op);
  buffer.put_u16(key_len.try_into().unwrap());
  buffer.put_u8(extras_len.try_into().unwrap());
  buffer.put_u8(0x00);
  buffer.put_u16(0x0000);
  buffer.put_u32(body_len.try_into().unwrap());
  buffer.put_u32(opaque);
  buffer.put_u64(cas);
}

#[cfg(test)]
mod tests {

  use crate::memcached::{decode_binary_command, encode_binary_command};

  use super::{decode_text_command, encode_text_command, Command, DecodeCommandError};

  #[test]
  fn test_decode_text_command() {
    let tests: &[(&[u8], _)] = &[
      (
        b"get foo\r\n",
        Ok(vec![Command::Get {
          key: "foo",
          noreply: false,
        }]),
      ),
      (
        b"get foo bar\r\n",
        Ok(vec![
          Command::Get {
            key: "foo",
            noreply: false,
          },
          Command::Get {
            key: "bar",
            noreply: false,
          },
        ]),
      ),
      (
        b"gets foo\r\n",
        Ok(vec![Command::Get {
          key: "foo",
          noreply: false,
        }]),
      ),
      (
        b"gets foo bar\r\n",
        Ok(vec![
          Command::Get {
            key: "foo",
            noreply: false,
          },
          Command::Get {
            key: "bar",
            noreply: false,
          },
        ]),
      ),
      (
        b"touch foo 123\r\n",
        Ok(vec![Command::Touch {
          key: "foo",
          exptime: 123,
          noreply: false,
        }]),
      ),
      (
        b"touch foo 123 noreply\r\n",
        Ok(vec![Command::Touch {
          key: "foo",
          exptime: 123,
          noreply: true,
        }]),
      ),
      (
        b"incr foo 2\r\n",
        Ok(vec![Command::Incr {
          key: "foo",
          value: 2,
          noreply: false,
        }]),
      ),
      (
        b"incr foo 2 noreply\r\n",
        Ok(vec![Command::Incr {
          key: "foo",
          value: 2,
          noreply: true,
        }]),
      ),
      (
        b"decr foo 2\r\n",
        Ok(vec![Command::Decr {
          key: "foo",
          value: 2,
          noreply: false,
        }]),
      ),
      (
        b"decr foo 2 noreply\r\n",
        Ok(vec![Command::Decr {
          key: "foo",
          value: 2,
          noreply: true,
        }]),
      ),
      (
        b"delete foo\r\n",
        Ok(vec![Command::Delete {
          key: "foo",
          noreply: false,
        }]),
      ),
      (
        b"delete foo noreply\r\n",
        Ok(vec![Command::Delete {
          key: "foo",
          noreply: true,
        }]),
      ),
      (
        b"set foo 123 321 3\r\nbar\r\n",
        Ok(vec![Command::Set {
          key: "foo",
          value: b"bar",
          flags: 123,
          exptime: 321,
          noreply: false,
          cas: None,
        }]),
      ),
      (
        b"set foo 123 321 0\r\n\r\n",
        Ok(vec![Command::Set {
          key: "foo",
          value: b"",
          flags: 123,
          exptime: 321,
          noreply: false,
          cas: None,
        }]),
      ),
      (
        b"set foo 123 321 3 noreply\r\nbar\r\n",
        Ok(vec![Command::Set {
          key: "foo",
          value: b"bar",
          flags: 123,
          exptime: 321,
          noreply: true,
          cas: None,
        }]),
      ),
      (b"set foo 123 321 1\r\nbar\r\n", Err(DecodeCommandError::InvalidFormat)),
      (b"set foo 123 321 1\r\nb", Err(DecodeCommandError::UnexpectedEof)),
      (b"set foo 123 321\r\nb\r\n", Err(DecodeCommandError::UnexpectedEof)),
      (
        b"add foo 123 321 3\r\nbar\r\n",
        Ok(vec![Command::Add {
          key: "foo",
          value: b"bar",
          flags: 123,
          exptime: 321,
          noreply: false,
          cas: None,
        }]),
      ),
      (
        b"add foo 123 321 3 noreply\r\nbar\r\n",
        Ok(vec![Command::Add {
          key: "foo",
          value: b"bar",
          flags: 123,
          exptime: 321,
          noreply: true,
          cas: None,
        }]),
      ),
      (
        b"replace foo 123 321 3\r\nbar\r\n",
        Ok(vec![Command::Replace {
          key: "foo",
          value: b"bar",
          flags: 123,
          exptime: 321,
          noreply: false,
          cas: None,
        }]),
      ),
      (
        b"replace foo 123 321 3 noreply\r\nbar\r\n",
        Ok(vec![Command::Replace {
          key: "foo",
          value: b"bar",
          flags: 123,
          exptime: 321,
          noreply: true,
          cas: None,
        }]),
      ),
      (
        b"append foo 123 321 3\r\nbar\r\n",
        Ok(vec![Command::Append {
          key: "foo",
          value: b"bar",
          flags: 123,
          exptime: 321,
          noreply: false,
          cas: None,
        }]),
      ),
      (
        b"append foo 123 321 3 noreply\r\nbar\r\n",
        Ok(vec![Command::Append {
          key: "foo",
          value: b"bar",
          flags: 123,
          exptime: 321,
          noreply: true,
          cas: None,
        }]),
      ),
      (
        b"prepend foo 123 321 3\r\nbar\r\n",
        Ok(vec![Command::Prepend {
          key: "foo",
          value: b"bar",
          flags: 123,
          exptime: 321,
          noreply: false,
          cas: None,
        }]),
      ),
      (
        b"prepend foo 123 321 3 noreply\r\nbar\r\n",
        Ok(vec![Command::Prepend {
          key: "foo",
          value: b"bar",
          flags: 123,
          exptime: 321,
          noreply: true,
          cas: None,
        }]),
      ),
      (
        b"cas foo 123 321 3 567\r\nbar\r\n",
        Ok(vec![Command::Set {
          key: "foo",
          value: b"bar",
          flags: 123,
          exptime: 321,
          cas: Some(567),
          noreply: false,
        }]),
      ),
      (
        b"cas foo 123 321 0 567\r\n\r\n",
        Ok(vec![Command::Set {
          key: "foo",
          value: b"",
          flags: 123,
          exptime: 321,
          cas: Some(567),
          noreply: false,
        }]),
      ),
      (
        b"cas foo 123 321 3 567 noreply\r\nbar\r\n",
        Ok(vec![Command::Set {
          key: "foo",
          value: b"bar",
          flags: 123,
          exptime: 321,
          cas: Some(567),
          noreply: true,
        }]),
      ),
      (b"flush_all\r\n", Ok(vec![Command::Flush { noreply: false }])),
      (b"version\r\n", Ok(vec![Command::Version])),
      (b"stats\r\n", Ok(vec![Command::Stats])),
      (b"quit\r\n", Ok(vec![Command::Quit { noreply: false }])),
    ];

    for t in tests {
      assert_eq!(t.1, decode_text_command(t.0));
    }
  }

  #[test]
  fn test_text_command_roundtrip() {
    let tests = &[
      Command::Get {
        key: "foo",
        noreply: false,
      },
      Command::GetAndTouch {
        key: "foo",
        exptime: 123,
        noreply: false,
      },
      Command::Delete {
        key: "foo",
        noreply: false,
      },
      Command::Incr {
        key: "foo",
        value: 123,
        noreply: false,
      },
      Command::Decr {
        key: "foo",
        value: 123,
        noreply: false,
      },
      Command::Touch {
        key: "foo",
        exptime: 123,
        noreply: false,
      },
      Command::Flush { noreply: false },
      Command::Version,
      Command::Stats,
      Command::Quit { noreply: false },
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
      Command::Get {
        key: "foo",
        noreply: false,
      },
      // Command::GetAndTouch {
      //   key: "foo",
      //   exptime: 123,
      //   noreply: false,
      // },
      Command::Delete {
        key: "foo",
        noreply: false,
      },
      // Command::Incr {
      //   key: "foo",
      //   value: 123,
      //   noreply: false,
      // },
      // Command::Decr {
      //   key: "foo",
      //   value: 123,
      //   noreply: false,
      // },
      // Command::Touch {
      //   key: "foo",
      //   exptime: 123,
      //   noreply: false,
      // },
      Command::Flush { noreply: true },
      Command::Flush { noreply: false },
      Command::Version,
      Command::Stats,
      Command::Quit { noreply: true },
      Command::Quit { noreply: false },
    ];

    for expected in tests.into_iter() {
      let encoded = encode_binary_command(expected).unwrap();
      let decoded = decode_binary_command(encoded.as_slice()).unwrap();

      assert_eq!(expected, &decoded);
    }
  }
}
