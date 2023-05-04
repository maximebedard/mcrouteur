use std::io;

use bytes::{Buf, Bytes, BytesMut};
use tokio::io::{AsyncBufRead, AsyncBufReadExt};

#[derive(Debug)]
pub struct Connection;

#[derive(Debug, PartialEq)]
pub struct StoreCommand<'a> {
  key: &'a str,
  value: &'a [u8],
  flags: u32,
  exptime: u32,
  noreply: bool,
}

#[derive(Debug, PartialEq)]
pub struct IncrDecrCommand<'a> {
  key: &'a str,
  value: u64,
  noreply: bool,
}

#[derive(Debug, PartialEq)]
pub struct GetCommand<'a> {
  keys: Vec<&'a str>,
  noreply: bool,
}

#[derive(Debug, PartialEq)]
pub enum Command<'a> {
  Get(GetCommand<'a>),
  GetK(GetCommand<'a>),
  GetAndTouch {
    keys: Vec<&'a str>,
    exptime: u32,
    noreply: bool,
  },
  Set(StoreCommand<'a>),
  Add(StoreCommand<'a>),
  Replace(StoreCommand<'a>),
  Append(StoreCommand<'a>),
  Prepend(StoreCommand<'a>),
  Cas {
    key: &'a str,
    value: &'a [u8],
    flags: u16,
    exptime: u32,
    cas: u64,
    noreply: bool,
  },
  Delete {
    key: &'a str,
    noreply: bool,
  },
  Incr(IncrDecrCommand<'a>),
  Decr(IncrDecrCommand<'a>),
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
pub enum ParseCommandError {
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

pub fn parse_command(input: &[u8]) -> Result<Command, ParseCommandError> {
  match input.get(0) {
    Some(0x80) => parse_binary_command(input),
    Some(_) => parse_text_command(input),
    None => Err(ParseCommandError::UnexpectedEof),
  }
}

pub fn parse_text_command(input: &[u8]) -> Result<Command, ParseCommandError> {
  let mut chunks = input.splitn(2, |v| *v == b' ');
  let command = chunks.next().ok_or(ParseCommandError::UnexpectedEof)?;
  let input = chunks.next().unwrap_or(&[]);

  fn new_line_str(input: &[u8]) -> Result<&str, ParseCommandError> {
    let input = std::str::from_utf8(input).map_err(|_| ParseCommandError::InvalidFormat)?;
    let input = input
      .strip_suffix("\r\n")
      .or_else(|| input.strip_suffix("\n"))
      .ok_or(ParseCommandError::UnexpectedEof)?;
    Ok(input)
  }

  match command {
    b"get" | b"gets" => {
      let input = new_line_str(input)?;

      let keys = input.split(' ').collect();

      let noreply = false;

      Ok(Command::Get(GetCommand { keys, noreply }))
    }
    b"gat" | b"gats" => {
      let input = new_line_str(input)?;

      let mut chunks = input.split(' ');

      let exptime = chunks.next().ok_or(ParseCommandError::UnexpectedEof)?;
      let exptime = exptime.parse().map_err(|_| ParseCommandError::InvalidFormat)?;

      let keys = chunks.collect();

      let noreply = false;

      Ok(Command::GetAndTouch { keys, exptime, noreply })
    }

    b"set" | b"add" | b"replace" | b"append" | b"prepend" => {
      let pos = input.iter().position(|v| *v == b'\n').unwrap_or_else(|| input.len());
      let (input, mut value) = (&input[..=pos], &input[pos + 1..]);

      let input = new_line_str(input)?;

      let mut chunks = input.split(" ");

      let key = chunks.next().ok_or(ParseCommandError::UnexpectedEof)?;

      let flags = chunks.next().ok_or(ParseCommandError::UnexpectedEof)?;
      let flags = flags.parse().map_err(|_| ParseCommandError::InvalidFormat)?;

      let exptime = chunks.next().ok_or(ParseCommandError::UnexpectedEof)?;
      let exptime = exptime.parse().map_err(|_| ParseCommandError::InvalidFormat)?;

      let value_len = chunks.next().ok_or(ParseCommandError::UnexpectedEof)?;
      let value_len = value_len
        .parse::<usize>()
        .map_err(|_| ParseCommandError::InvalidFormat)?;

      value = value.strip_suffix(b"\r\n").ok_or(ParseCommandError::UnexpectedEof)?;

      if value_len != value.len() {
        return Err(ParseCommandError::InvalidFormat);
      }

      let noreply = chunks.next().filter(|v| *v == "noreply").is_some();

      let cmd = StoreCommand {
        key,
        value,
        flags,
        exptime,
        noreply,
      };

      match command {
        b"set" => Ok(Command::Set(cmd)),
        b"add" => Ok(Command::Add(cmd)),
        b"replace" => Ok(Command::Replace(cmd)),
        b"append" => Ok(Command::Append(cmd)),
        b"prepend" => Ok(Command::Prepend(cmd)),
        _ => unreachable!(),
      }
    }

    b"cas" => {
      let pos = input.iter().position(|v| *v == b'\n').unwrap_or_else(|| input.len());
      let (input, mut value) = (&input[..=pos], &input[pos + 1..]);

      let input = new_line_str(input)?;

      let mut chunks = input.split(" ");

      let key = chunks.next().ok_or(ParseCommandError::UnexpectedEof)?;

      let flags = chunks.next().ok_or(ParseCommandError::UnexpectedEof)?;
      let flags = flags.parse().map_err(|_| ParseCommandError::InvalidFormat)?;

      let exptime = chunks.next().ok_or(ParseCommandError::UnexpectedEof)?;
      let exptime = exptime.parse().map_err(|_| ParseCommandError::InvalidFormat)?;

      let value_len = chunks.next().ok_or(ParseCommandError::UnexpectedEof)?;
      let value_len = value_len
        .parse::<usize>()
        .map_err(|_| ParseCommandError::InvalidFormat)?;

      value = value
        .strip_suffix(b"\r\n")
        .or_else(|| value.strip_suffix(b"\n"))
        .ok_or(ParseCommandError::UnexpectedEof)?;

      if value_len != value.len() {
        return Err(ParseCommandError::InvalidFormat);
      }

      let cas = chunks.next().ok_or(ParseCommandError::UnexpectedEof)?;
      let cas = cas.parse().map_err(|_| ParseCommandError::InvalidFormat)?;

      let noreply = chunks.next().filter(|v| *v == "noreply").is_some();

      Ok(Command::Cas {
        key,
        value,
        flags,
        exptime,
        cas,
        noreply,
      })
    }
    b"delete" => {
      let input = new_line_str(input)?;

      let mut chunks = input.split(' ');

      let key = chunks.next().ok_or(ParseCommandError::UnexpectedEof)?;

      let noreply = chunks.next().filter(|v| *v == "noreply").is_some();

      Ok(Command::Delete { key, noreply })
    }
    b"incr" | b"decr" => {
      let input = new_line_str(input)?;

      let mut chunks = input.split(' ');

      let key = chunks.next().ok_or(ParseCommandError::UnexpectedEof)?;

      let value = chunks.next().ok_or(ParseCommandError::UnexpectedEof)?;
      let value = value.parse().map_err(|_| ParseCommandError::InvalidFormat)?;

      let noreply = chunks.next().filter(|v| *v == "noreply").is_some();

      let cmd = IncrDecrCommand { key, value, noreply };

      match command {
        b"incr" => Ok(Command::Incr(cmd)),
        b"decr" => Ok(Command::Decr(cmd)),
        _ => unreachable!(),
      }
    }
    b"touch" => {
      let input = new_line_str(input)?;

      let mut chunks = input.split(' ');

      let key = chunks.next().ok_or(ParseCommandError::UnexpectedEof)?;

      let exptime = chunks.next().ok_or(ParseCommandError::UnexpectedEof)?;
      let exptime = exptime.parse().map_err(|_| ParseCommandError::InvalidFormat)?;

      let noreply = chunks.next().filter(|v| *v == "noreply").is_some();

      Ok(Command::Touch { key, exptime, noreply })
    }
    b"flush_all\r\n" | b"flush_all\n" => Ok(Command::Flush { noreply: false }),
    b"version\r\n" | b"version\n" => Ok(Command::Version),
    b"stats\r\n" | b"stats\n" => Ok(Command::Stats),
    b"quit\r\n" | b"quit\n" => Ok(Command::Quit { noreply: false }),
    _ => Err(ParseCommandError::InvalidCommand),
  }
}

pub fn parse_binary_command(mut input: &[u8]) -> Result<Command, ParseCommandError> {
  if input.len() < 24 {
    return Err(ParseCommandError::UnexpectedEof);
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

  if input.len() != body_len + 24 {
    return Err(ParseCommandError::UnexpectedEof);
  }

  match op {
    0x00 | 0x09 | 0x0c | 0x0d | 0x04 | 0x14 => {
      // Get/GetQ/GetK/GetKQ/Delete/DeleteQ
      assert_eq!(0, extras_len);
      assert_eq!(key_len, body_len);
      assert!(key_len > 0);

      let key = input.get(0..key_len).ok_or(ParseCommandError::UnexpectedEof)?;
      input.advance(key_len);
      assert!(input.is_empty());

      let key = std::str::from_utf8(key).map_err(|_| ParseCommandError::InvalidFormat)?;
      let keys = vec![key];

      let noreply = 0x09 == op || 0x0d == op || 0x14 == op;
      match op {
        0x00 | 0x09 => Ok(Command::Get(GetCommand { keys, noreply })),
        0x0c | 0x0d => Ok(Command::GetK(GetCommand { keys, noreply })),
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

      let key = input.get(0..key_len).ok_or(ParseCommandError::UnexpectedEof)?;
      input.advance(key_len);

      let value = input.get(0..value_len).ok_or(ParseCommandError::UnexpectedEof)?;
      input.advance(value_len);

      assert!(input.is_empty());

      let key = std::str::from_utf8(key).map_err(|_| ParseCommandError::InvalidFormat)?;

      let noreply = 0x11 == op || 0x12 == op || 0x13 == op;

      let cmd = StoreCommand {
        key,
        value,
        flags,
        exptime,
        noreply,
      };

      match op {
        0x01 | 0x11 => Ok(Command::Set(cmd)),
        0x02 | 0x12 => Ok(Command::Add(cmd)),
        0x03 | 0x13 => Ok(Command::Replace(cmd)),
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
    _ => Err(ParseCommandError::InvalidCommand),
  }
}

#[cfg(test)]
mod tests {

  use super::{parse_text_command, Command, GetCommand, IncrDecrCommand, ParseCommandError, StoreCommand};

  #[test]
  fn test_parse_text_command() {
    let tests: &[(&[u8], _)] = &[
      (
        b"get foo\r\n",
        Ok(Command::Get(GetCommand {
          keys: vec!["foo"],
          noreply: false,
        })),
      ),
      (
        b"get foo bar\r\n",
        Ok(Command::Get(GetCommand {
          keys: vec!["foo", "bar"],
          noreply: false,
        })),
      ),
      (
        b"gets foo\r\n",
        Ok(Command::Get(GetCommand {
          keys: vec!["foo"],
          noreply: false,
        })),
      ),
      (
        b"gets foo bar\r\n",
        Ok(Command::Get(GetCommand {
          keys: vec!["foo", "bar"],
          noreply: false,
        })),
      ),
      (
        b"touch foo 123\r\n",
        Ok(Command::Touch {
          key: "foo",
          exptime: 123,
          noreply: false,
        }),
      ),
      (
        b"touch foo 123 noreply\r\n",
        Ok(Command::Touch {
          key: "foo",
          exptime: 123,
          noreply: true,
        }),
      ),
      (
        b"incr foo 2\r\n",
        Ok(Command::Incr(IncrDecrCommand {
          key: "foo",
          value: 2,
          noreply: false,
        })),
      ),
      (
        b"incr foo 2 noreply\r\n",
        Ok(Command::Incr(IncrDecrCommand {
          key: "foo",
          value: 2,
          noreply: true,
        })),
      ),
      (
        b"decr foo 2\r\n",
        Ok(Command::Decr(IncrDecrCommand {
          key: "foo",
          value: 2,
          noreply: false,
        })),
      ),
      (
        b"decr foo 2 noreply\r\n",
        Ok(Command::Decr(IncrDecrCommand {
          key: "foo",
          value: 2,
          noreply: true,
        })),
      ),
      (
        b"delete foo\r\n",
        Ok(Command::Delete {
          key: "foo",
          noreply: false,
        }),
      ),
      (
        b"delete foo noreply\r\n",
        Ok(Command::Delete {
          key: "foo",
          noreply: true,
        }),
      ),
      (
        b"set foo 123 321 3\r\nbar\r\n",
        Ok(Command::Set(StoreCommand {
          key: "foo",
          value: b"bar",
          flags: 123,
          exptime: 321,
          noreply: false,
        })),
      ),
      (
        b"set foo 123 321 0\r\n\r\n",
        Ok(Command::Set(StoreCommand {
          key: "foo",
          value: b"",
          flags: 123,
          exptime: 321,
          noreply: false,
        })),
      ),
      (
        b"set foo 123 321 3 noreply\r\nbar\r\n",
        Ok(Command::Set(StoreCommand {
          key: "foo",
          value: b"bar",
          flags: 123,
          exptime: 321,
          noreply: true,
        })),
      ),
      (b"set foo 123 321 1\r\nbar\r\n", Err(ParseCommandError::InvalidFormat)),
      (b"set foo 123 321 1\r\nb", Err(ParseCommandError::UnexpectedEof)),
      (b"set foo 123 321\r\nb\r\n", Err(ParseCommandError::UnexpectedEof)),
      (
        b"add foo 123 321 3\r\nbar\r\n",
        Ok(Command::Add(StoreCommand {
          key: "foo",
          value: b"bar",
          flags: 123,
          exptime: 321,
          noreply: false,
        })),
      ),
      (
        b"add foo 123 321 3 noreply\r\nbar\r\n",
        Ok(Command::Add(StoreCommand {
          key: "foo",
          value: b"bar",
          flags: 123,
          exptime: 321,
          noreply: true,
        })),
      ),
      (
        b"replace foo 123 321 3\r\nbar\r\n",
        Ok(Command::Replace(StoreCommand {
          key: "foo",
          value: b"bar",
          flags: 123,
          exptime: 321,
          noreply: false,
        })),
      ),
      (
        b"replace foo 123 321 3 noreply\r\nbar\r\n",
        Ok(Command::Replace(StoreCommand {
          key: "foo",
          value: b"bar",
          flags: 123,
          exptime: 321,
          noreply: true,
        })),
      ),
      (
        b"append foo 123 321 3\r\nbar\r\n",
        Ok(Command::Append(StoreCommand {
          key: "foo",
          value: b"bar",
          flags: 123,
          exptime: 321,
          noreply: false,
        })),
      ),
      (
        b"append foo 123 321 3 noreply\r\nbar\r\n",
        Ok(Command::Append(StoreCommand {
          key: "foo",
          value: b"bar",
          flags: 123,
          exptime: 321,
          noreply: true,
        })),
      ),
      (
        b"prepend foo 123 321 3\r\nbar\r\n",
        Ok(Command::Prepend(StoreCommand {
          key: "foo",
          value: b"bar",
          flags: 123,
          exptime: 321,
          noreply: false,
        })),
      ),
      (
        b"prepend foo 123 321 3 noreply\r\nbar\r\n",
        Ok(Command::Prepend(StoreCommand {
          key: "foo",
          value: b"bar",
          flags: 123,
          exptime: 321,
          noreply: true,
        })),
      ),
      (
        b"cas foo 123 321 3 567\r\nbar\r\n",
        Ok(Command::Cas {
          key: "foo",
          value: b"bar",
          flags: 123,
          exptime: 321,
          cas: 567,
          noreply: false,
        }),
      ),
      (
        b"cas foo 123 321 0 567\r\n\r\n",
        Ok(Command::Cas {
          key: "foo",
          value: b"",
          flags: 123,
          exptime: 321,
          cas: 567,
          noreply: false,
        }),
      ),
      (
        b"cas foo 123 321 3 567 noreply\r\nbar\r\n",
        Ok(Command::Cas {
          key: "foo",
          value: b"bar",
          flags: 123,
          exptime: 321,
          cas: 567,
          noreply: true,
        }),
      ),
      (b"flush_all\r\n", Ok(Command::Flush { noreply: false })),
      (b"version\r\n", Ok(Command::Version)),
      (b"stats\r\n", Ok(Command::Stats)),
      (b"quit\r\n", Ok(Command::Quit { noreply: false })),
    ];

    for t in tests {
      assert_eq!(t.1, parse_text_command(t.0));
    }
  }
}
