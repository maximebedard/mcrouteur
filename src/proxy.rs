use std::{collections::BTreeMap, io};

use bytes::{Buf, Bytes};
use tokio::{
  io::{AsyncWrite, AsyncWriteExt},
  sync::{mpsc, oneshot},
};

use crate::{
  codec::{
    self, AppendPrependCommand, BinaryCommand, IncrDecrCommand, KeyCommand, SetCommand, TextCommand,
    TextIncrDecrCommand, TouchCommand,
  },
  connection::{self, Error, Header, ServerError},
};

struct BinaryResponseWriter<W> {
  w: W,
}

impl<W> BinaryResponseWriter<W>
where
  W: AsyncWrite + Unpin,
{
  async fn write_status_response(&mut self, op: u8, r: connection::Result<()>) -> io::Result<()> {
    match r {
      Ok(()) => self.write_ok(op).await,
      Err(Error::Io(err)) => Err(err),
      Err(Error::Server(err)) => self.write_server_error(op, err).await,
    }
  }

  async fn write_incr_decr_command_response(&mut self, op: u8, r: connection::Result<u64>) -> io::Result<()> {
    match r {
      Ok(v) => {
        let extras_len = 8;
        let body_len = extras_len;
        self
          .write_response_header(Header {
            op,
            extras_len,
            body_len,
            ..Header::response()
          })
          .await?;
        self.w.write_u64(v).await?;
        Ok(())
      }
      Err(Error::Io(err)) => Err(err),
      Err(Error::Server(err)) => self.write_server_error(op, err).await,
    }
  }

  async fn write_get_response(&mut self, op: u8, _key: &str, r: connection::Result<Bytes>) -> io::Result<()> {
    match r {
      Ok(value) => {
        let extras_len = 4;
        let key_len = 0;
        let body_len = key_len + extras_len + value.len();
        self
          .write_response_header(Header {
            op,
            key_len,
            extras_len,
            body_len,
            ..Header::response()
          })
          .await?;
        self.w.write_u32(0).await?; // TODO: flags
        self.w.write_all(value.chunk()).await?;
        Ok(())
      }
      Err(Error::Io(err)) => Err(err),
      Err(Error::Server(err)) => self.write_server_error(op, err).await,
    }
  }

  async fn write_touch_response(&mut self, op: u8, r: connection::Result<()>) -> io::Result<()> {
    match r {
      Ok(()) => {
        let extras_len = 4;
        let body_len = extras_len;
        self
          .write_response_header(Header {
            op,
            extras_len,
            body_len,
            ..Header::response()
          })
          .await?;
        self.w.write_u32(0).await?; // TODO: flags
        Ok(())
      }
      Err(Error::Io(err)) => Err(err),
      Err(Error::Server(err)) => self.write_server_error(op, err).await,
    }
  }

  async fn write_version_response(&mut self, op: u8, r: connection::Result<String>) -> io::Result<()> {
    match r {
      Ok(version) => {
        let body_len = version.len();
        self
          .write_response_header(Header {
            op,
            body_len,
            ..Header::response()
          })
          .await?;
        self.w.write_all(version.as_bytes()).await?;
        Ok(())
      }
      Err(Error::Io(err)) => Err(err),
      Err(Error::Server(err)) => self.write_server_error(op, err).await,
    }
  }

  async fn write_stats_response(&mut self, op: u8, r: connection::Result<BTreeMap<String, String>>) -> io::Result<()> {
    match r {
      Ok(stats) => {
        for (k, v) in stats {
          let key_len = k.len();
          let body_len = key_len + v.len();
          self
            .write_response_header(Header {
              op,
              key_len,
              body_len,
              ..Header::response()
            })
            .await?;
          self.w.write_all(k.as_bytes()).await?;
          self.w.write_all(v.as_bytes()).await?;
        }

        self
          .write_response_header(Header {
            op,
            ..Header::response()
          })
          .await?;
        Ok(())
      }
      Err(Error::Io(err)) => Err(err),
      Err(Error::Server(err)) => self.write_server_error(op, err).await,
    }
  }

  async fn write_ok(&mut self, op: u8) -> io::Result<()> {
    self
      .write_response_header(Header {
        op,
        ..Header::response()
      })
      .await
  }

  async fn write_server_error(&mut self, op: u8, err: ServerError) -> io::Result<()> {
    self
      .write_response_header(Header {
        op,
        status: err.code(),
        ..Header::response()
      })
      .await
  }

  async fn write_response_header(&mut self, h: Header) -> io::Result<()> {
    self.w.write_u8(h.magic).await?;
    self.w.write_u8(h.op).await?;
    self.w.write_u16(h.key_len.try_into().unwrap()).await?;
    self.w.write_u8(h.extras_len.try_into().unwrap()).await?;
    self.w.write_u8(h.data_type).await?;
    self.w.write_u16(h.status).await?;
    self.w.write_u32(h.body_len.try_into().unwrap()).await?;
    self.w.write_u32(h.opaque).await?;
    self.w.write_u64(h.cas).await?;
    Ok(())
  }
}

struct TextResponseWriter<W> {
  w: W,
}

impl<W> TextResponseWriter<W>
where
  W: AsyncWrite + Unpin,
{
  async fn write_set_command_response(&mut self, r: connection::Result<()>) -> io::Result<()> {
    match r {
      Ok(()) => self.w.write_all(b"STORED\r\n").await,
      Err(connection::Error::Io(err)) => Err(err),
      Err(connection::Error::Server(ServerError::KeyExists))
      | Err(connection::Error::Server(ServerError::KeyNotFound))
      | Err(connection::Error::Server(ServerError::ItemNotStored)) => self.w.write_all(b"NOT_STORED\r\n").await,
      Err(connection::Error::Server(err)) => self.w.write_all(format!("SERVER_ERROR {err}\r\n").as_bytes()).await,
    }
  }

  async fn write_delete_command_response(&mut self, r: connection::Result<()>) -> io::Result<()> {
    match r {
      Ok(()) => self.w.write_all(b"DELETED\r\n").await,
      Err(connection::Error::Io(err)) => Err(err),
      Err(connection::Error::Server(ServerError::KeyExists)) => self.w.write_all(b"EXISTS\r\n").await,
      Err(connection::Error::Server(ServerError::KeyNotFound)) => self.w.write_all(b"NOT_FOUND\r\n").await,
      Err(connection::Error::Server(err)) => self.w.write_all(format!("SERVER_ERROR {err}\r\n").as_bytes()).await,
    }
  }

  async fn write_incr_decr_command_response(&mut self, r: connection::Result<u64>) -> io::Result<()> {
    match r {
      Ok(value) => self.w.write_all(format!("{value}\r\n").as_bytes()).await,
      Err(connection::Error::Io(err)) => Err(err),
      Err(connection::Error::Server(ServerError::KeyNotFound)) => self.w.write_all(b"NOT_FOUND\r\n").await,
      Err(connection::Error::Server(err)) => self.w.write_all(format!("SERVER_ERROR {err}\r\n").as_bytes()).await,
    }
  }

  async fn write_get_command_response(&mut self, key: &str, r: connection::Result<Bytes>) -> io::Result<()> {
    match r {
      Ok(bytes) => {
        self
          .w
          .write_all(format!("VALUE {} 0 {}\r\n", key, bytes.len()).as_bytes())
          .await?;
        self.w.write_all(bytes.chunk()).await?;
        self.w.write_all(b"\r\n").await
      }
      Err(connection::Error::Io(err)) => Err(err),
      Err(connection::Error::Server(ServerError::KeyNotFound)) => Ok(()),
      Err(connection::Error::Server(err)) => self.w.write_all(format!("SERVER_ERROR {err}\r\n").as_bytes()).await,
    }
  }

  async fn write_touch_command_response(&mut self, r: connection::Result<()>) -> io::Result<()> {
    match r {
      Ok(()) => self.w.write_all(b"TOUCHED\r\n").await,
      Err(connection::Error::Io(err)) => Err(err),
      Err(connection::Error::Server(ServerError::KeyNotFound)) => self.w.write_all(b"NOT_FOUND\r\n").await,
      Err(connection::Error::Server(err)) => self.w.write_all(format!("SERVER_ERROR {err}\r\n").as_bytes()).await,
    }
  }

  async fn write_version_response(&mut self, r: connection::Result<String>) -> io::Result<()> {
    match r {
      Ok(version) => {
        self.w.write_all(b"VERSION ").await?;
        self.w.write_all(version.as_bytes()).await?;
        self.w.write_all(b"\r\n").await
      }
      Err(connection::Error::Io(err)) => Err(err),
      Err(connection::Error::Server(err)) => self.w.write_all(format!("SERVER_ERROR {err}\r\n").as_bytes()).await,
    }
  }

  async fn write_stats_response(&mut self, r: connection::Result<BTreeMap<String, String>>) -> io::Result<()> {
    match r {
      Ok(stats) => {
        for (k, v) in stats.iter() {
          self.w.write_all(b"STAT ").await?;
          self.w.write_all(k.as_bytes()).await?;
          self.w.write_all(b" ").await?;
          self.w.write_all(v.as_bytes()).await?;
          self.w.write_all(b"\r\n").await?;
        }
        self.write_end().await
      }
      Err(connection::Error::Io(err)) => Err(err),
      Err(connection::Error::Server(err)) => self.w.write_all(format!("SERVER_ERROR {err}\r\n").as_bytes()).await,
    }
  }

  async fn write_ok_response(&mut self, r: connection::Result<()>) -> io::Result<()> {
    match r {
      Ok(()) => self.w.write_all(b"OK\r\n").await,
      Err(connection::Error::Io(err)) => Err(err),
      Err(connection::Error::Server(err)) => self.w.write_all(format!("SERVER_ERROR {err}\r\n").as_bytes()).await,
    }
  }

  async fn write_end(&mut self) -> io::Result<()> {
    self.w.write_all(b"END\r\n").await
  }
}

pub async fn proxy_command(
  w: impl AsyncWrite + Unpin,
  client: &mut mpsc::Sender<connection::Command>,
  command: codec::Command,
) -> io::Result<()> {
  match command {
    codec::Command::Text(TextCommand::GetM { keys }) => {
      let mut receivers = Vec::with_capacity(keys.len());
      for key in keys {
        let (sender, receiver) = oneshot::channel();
        client
          .send(connection::Command::Get(
            connection::KeyCommand { key: key.to_string() },
            sender,
          ))
          .await
          .ok();
        receivers.push((key, receiver));
      }

      let mut w = TextResponseWriter { w };
      for (key, receiver) in receivers {
        w.write_get_command_response(key.as_str(), receiver.await.unwrap())
          .await?;
      }
      w.write_end().await
    }

    codec::Command::Text(TextCommand::GetAndTouchM { keys, exptime }) => {
      let mut receivers = Vec::with_capacity(keys.len());
      for key in keys {
        let (sender, receiver) = oneshot::channel();
        client
          .send(connection::Command::GetAndTouch(
            connection::TouchCommand {
              key: key.to_string(),
              exptime,
            },
            sender,
          ))
          .await
          .ok();
        receivers.push((key, receiver));
      }

      let mut w = TextResponseWriter { w };
      for (key, receiver) in receivers {
        w.write_get_command_response(key.as_str(), receiver.await.unwrap())
          .await?;
      }
      w.write_end().await
    }

    codec::Command::Binary(BinaryCommand::Get(KeyCommand { key })) => {
      let (sender, receiver) = oneshot::channel();
      client
        .send(connection::Command::Get(
          connection::KeyCommand { key: key.clone() },
          sender,
        ))
        .await
        .ok();
      BinaryResponseWriter { w }
        .write_get_response(0x00, key.as_str(), receiver.await.unwrap())
        .await
    }

    codec::Command::Binary(BinaryCommand::GetK(KeyCommand { key })) => {
      let (sender, receiver) = oneshot::channel();
      client
        .send(connection::Command::Get(
          connection::KeyCommand { key: key.clone() },
          sender,
        ))
        .await
        .ok();
      BinaryResponseWriter { w }
        .write_get_response(0x0c, key.as_str(), receiver.await.unwrap())
        .await
    }

    codec::Command::Binary(BinaryCommand::GetAndTouch(TouchCommand { key, exptime })) => {
      let (sender, receiver) = oneshot::channel();
      client
        .send(connection::Command::GetAndTouch(
          connection::TouchCommand {
            key: key.clone(),
            exptime,
          },
          sender,
        ))
        .await
        .ok();
      BinaryResponseWriter { w }
        .write_get_response(0x1d, key.as_str(), receiver.await.unwrap())
        .await
    }

    codec::Command::Binary(BinaryCommand::Set(SetCommand {
      key,
      value,
      flags: _,
      exptime,
      cas,
    })) => {
      let (sender, receiver) = oneshot::channel();
      client
        .send(connection::Command::Set(
          connection::SetCommand {
            key,
            value,
            exptime,
            cas,
          },
          sender,
        ))
        .await
        .ok();
      BinaryResponseWriter { w }
        .write_status_response(0x01, receiver.await.unwrap())
        .await
    }

    codec::Command::Text(TextCommand::Set(SetCommand {
      key,
      value,
      flags: _,
      exptime,
      cas,
    })) => {
      let (sender, receiver) = oneshot::channel();
      client
        .send(connection::Command::Set(
          connection::SetCommand {
            key,
            value,
            exptime,
            cas,
          },
          sender,
        ))
        .await
        .ok();
      TextResponseWriter { w }
        .write_set_command_response(receiver.await.unwrap())
        .await
    }

    codec::Command::Binary(BinaryCommand::Add(SetCommand {
      key,
      value,
      flags: _,
      exptime,
      cas,
    })) => {
      let (sender, receiver) = oneshot::channel();
      client
        .send(connection::Command::Add(
          connection::SetCommand {
            key,
            value,
            exptime,
            cas,
          },
          sender,
        ))
        .await
        .ok();
      BinaryResponseWriter { w }
        .write_status_response(0x02, receiver.await.unwrap())
        .await
    }

    codec::Command::Text(TextCommand::Add(SetCommand {
      key,
      value,
      flags: _,
      exptime,
      cas,
    })) => {
      let (sender, receiver) = oneshot::channel();
      client
        .send(connection::Command::Add(
          connection::SetCommand {
            key,
            value,
            exptime,
            cas,
          },
          sender,
        ))
        .await
        .ok();
      TextResponseWriter { w }
        .write_set_command_response(receiver.await.unwrap())
        .await
    }

    codec::Command::Binary(BinaryCommand::Replace(SetCommand {
      key,
      value,
      flags: _,
      exptime,
      cas,
    })) => {
      let (sender, receiver) = oneshot::channel();
      client
        .send(connection::Command::Replace(
          connection::SetCommand {
            key,
            value,
            exptime,
            cas,
          },
          sender,
        ))
        .await
        .ok();
      BinaryResponseWriter { w }
        .write_status_response(0x03, receiver.await.unwrap())
        .await
    }

    codec::Command::Text(TextCommand::Replace(SetCommand {
      key,
      value,
      flags: _,
      exptime,
      cas,
    })) => {
      let (sender, receiver) = oneshot::channel();
      client
        .send(connection::Command::Replace(
          connection::SetCommand {
            key,
            value,
            exptime,
            cas,
          },
          sender,
        ))
        .await
        .ok();
      TextResponseWriter { w }
        .write_set_command_response(receiver.await.unwrap())
        .await
    }

    codec::Command::Binary(BinaryCommand::Append(AppendPrependCommand { key, value })) => {
      let (sender, receiver) = oneshot::channel();
      client
        .send(connection::Command::Append(
          connection::AppendPrependCommand { key, value },
          sender,
        ))
        .await
        .ok();
      BinaryResponseWriter { w }
        .write_status_response(0x0e, receiver.await.unwrap())
        .await
    }

    codec::Command::Text(TextCommand::Append(AppendPrependCommand { key, value })) => {
      let (sender, receiver) = oneshot::channel();
      client
        .send(connection::Command::Append(
          connection::AppendPrependCommand { key, value },
          sender,
        ))
        .await
        .ok();
      TextResponseWriter { w }
        .write_set_command_response(receiver.await.unwrap())
        .await
    }

    codec::Command::Binary(BinaryCommand::Prepend(AppendPrependCommand { key, value })) => {
      let (sender, receiver) = oneshot::channel();
      client
        .send(connection::Command::Prepend(
          connection::AppendPrependCommand { key, value },
          sender,
        ))
        .await
        .ok();
      BinaryResponseWriter { w }
        .write_status_response(0x0f, receiver.await.unwrap())
        .await
    }

    codec::Command::Text(TextCommand::Prepend(AppendPrependCommand { key, value })) => {
      let (sender, receiver) = oneshot::channel();
      client
        .send(connection::Command::Prepend(
          connection::AppendPrependCommand { key, value },
          sender,
        ))
        .await
        .ok();
      TextResponseWriter { w }
        .write_set_command_response(receiver.await.unwrap())
        .await
    }

    codec::Command::Binary(BinaryCommand::Delete(KeyCommand { key })) => {
      let (sender, receiver) = oneshot::channel();
      client
        .send(connection::Command::Delete(connection::KeyCommand { key }, sender))
        .await
        .ok();
      BinaryResponseWriter { w }
        .write_status_response(0x04, receiver.await.unwrap())
        .await
    }

    codec::Command::Text(TextCommand::Delete(KeyCommand { key })) => {
      let (sender, receiver) = oneshot::channel();
      client
        .send(connection::Command::Delete(connection::KeyCommand { key }, sender))
        .await
        .ok();
      TextResponseWriter { w }
        .write_delete_command_response(receiver.await.unwrap())
        .await
    }

    codec::Command::Binary(BinaryCommand::Incr(IncrDecrCommand {
      key,
      delta,
      init,
      exptime,
    })) => {
      let (sender, receiver) = oneshot::channel();
      client
        .send(connection::Command::Increment(
          connection::IncrDecrCommand {
            key,
            delta,
            init,
            exptime,
          },
          sender,
        ))
        .await
        .ok();
      BinaryResponseWriter { w }
        .write_incr_decr_command_response(0x05, receiver.await.unwrap())
        .await
    }

    codec::Command::Text(TextCommand::Incr(TextIncrDecrCommand { key, delta })) => {
      let (sender, receiver) = oneshot::channel();
      client
        .send(connection::Command::Increment(
          connection::IncrDecrCommand {
            key,
            delta,
            init: 0,
            exptime: 0,
          },
          sender,
        ))
        .await
        .ok();
      TextResponseWriter { w }
        .write_incr_decr_command_response(receiver.await.unwrap())
        .await
    }

    codec::Command::Binary(BinaryCommand::Decr(IncrDecrCommand {
      key,
      delta,
      init,
      exptime,
    })) => {
      let (sender, receiver) = oneshot::channel();
      client
        .send(connection::Command::Increment(
          connection::IncrDecrCommand {
            key,
            delta,
            init,
            exptime,
          },
          sender,
        ))
        .await
        .ok();
      BinaryResponseWriter { w }
        .write_incr_decr_command_response(0x06, receiver.await.unwrap())
        .await
    }

    codec::Command::Text(TextCommand::Decr(TextIncrDecrCommand { key, delta })) => {
      let (sender, receiver) = oneshot::channel();
      client
        .send(connection::Command::Decrement(
          connection::IncrDecrCommand {
            key,
            delta,
            init: 0,
            exptime: 0,
          },
          sender,
        ))
        .await
        .ok();
      TextResponseWriter { w }
        .write_incr_decr_command_response(receiver.await.unwrap())
        .await
    }

    codec::Command::Binary(BinaryCommand::Touch(TouchCommand { key, exptime })) => {
      let (sender, receiver) = oneshot::channel();
      client
        .send(connection::Command::Touch(
          connection::TouchCommand { key, exptime },
          sender,
        ))
        .await
        .ok();
      BinaryResponseWriter { w }
        .write_touch_response(0x1c, receiver.await.unwrap())
        .await
    }

    codec::Command::Text(TextCommand::Touch(TouchCommand { key, exptime })) => {
      let (sender, receiver) = oneshot::channel();
      client
        .send(connection::Command::Touch(
          connection::TouchCommand { key, exptime },
          sender,
        ))
        .await
        .ok();
      TextResponseWriter { w }
        .write_touch_command_response(receiver.await.unwrap())
        .await
    }

    codec::Command::Binary(BinaryCommand::Flush) => {
      let (sender, receiver) = oneshot::channel();
      client.send(connection::Command::Flush(sender)).await.ok();
      BinaryResponseWriter { w }
        .write_status_response(0x08, receiver.await.unwrap())
        .await
    }

    codec::Command::Binary(BinaryCommand::Version) => {
      let (sender, receiver) = oneshot::channel();
      client.send(connection::Command::Version(sender)).await.ok();
      BinaryResponseWriter { w }
        .write_version_response(0x0b, receiver.await.unwrap())
        .await
    }

    codec::Command::Binary(BinaryCommand::Stats) => {
      let (sender, receiver) = oneshot::channel();
      client.send(connection::Command::Stats(sender)).await.ok();
      BinaryResponseWriter { w }
        .write_stats_response(0x10, receiver.await.unwrap())
        .await
    }

    codec::Command::Binary(BinaryCommand::Noop) => {
      let (sender, receiver) = oneshot::channel();
      client.send(connection::Command::Noop(sender)).await.ok();
      BinaryResponseWriter { w }
        .write_status_response(0x0a, receiver.await.unwrap())
        .await
    }

    codec::Command::Text(TextCommand::Flush) => {
      let (sender, receiver) = oneshot::channel();
      client.send(connection::Command::Flush(sender)).await.ok();
      TextResponseWriter { w }
        .write_ok_response(receiver.await.unwrap())
        .await
    }

    codec::Command::Text(TextCommand::Version) => {
      let (sender, receiver) = oneshot::channel();
      client.send(connection::Command::Version(sender)).await.ok();
      TextResponseWriter { w }
        .write_version_response(receiver.await.unwrap())
        .await
    }

    codec::Command::Text(TextCommand::Stats) => {
      let (sender, receiver) = oneshot::channel();
      client.send(connection::Command::Stats(sender)).await.ok();
      TextResponseWriter { w }
        .write_stats_response(receiver.await.unwrap())
        .await
    }

    codec::Command::Binary(BinaryCommand::Quit) => todo!(),
    codec::Command::Binary(BinaryCommand::SetQ(_)) => todo!(),
    codec::Command::Binary(BinaryCommand::AddQ(_)) => todo!(),
    codec::Command::Binary(BinaryCommand::ReplaceQ(_)) => todo!(),
    codec::Command::Binary(BinaryCommand::AppendQ(_)) => todo!(),
    codec::Command::Binary(BinaryCommand::GetQ(_)) => todo!(),
    codec::Command::Binary(BinaryCommand::GetKQ(_)) => todo!(),
    codec::Command::Binary(BinaryCommand::GetAndTouchQ(_)) => todo!(),
    codec::Command::Binary(BinaryCommand::PrependQ(_)) => todo!(),
    codec::Command::Binary(BinaryCommand::DeleteQ(_)) => todo!(),
    codec::Command::Binary(BinaryCommand::IncrQ(_)) => todo!(),
    codec::Command::Binary(BinaryCommand::DecrQ(_)) => todo!(),
    codec::Command::Binary(BinaryCommand::FlushQ) => todo!(),
    codec::Command::Binary(BinaryCommand::QuitQ) => todo!(),
    codec::Command::Text(TextCommand::Quit) => todo!(),
    codec::Command::Text(TextCommand::SetQ(_)) => todo!(),
    codec::Command::Text(TextCommand::AddQ(_)) => todo!(),
    codec::Command::Text(TextCommand::ReplaceQ(_)) => todo!(),
    codec::Command::Text(TextCommand::AppendQ(_)) => todo!(),
    codec::Command::Text(TextCommand::PrependQ(_)) => todo!(),
    codec::Command::Text(TextCommand::DeleteQ(_)) => todo!(),
    codec::Command::Text(TextCommand::IncrQ(_)) => todo!(),
    codec::Command::Text(TextCommand::DecrQ(_)) => todo!(),
    codec::Command::Text(TextCommand::TouchQ(_)) => todo!(),
  }
}
