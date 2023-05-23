use std::{io, time::Duration};

use bytes::Bytes;
use clap;
use kvp::{
  codec::{
    self, AppendPrependCommand, BinaryCommand, Command, IncrDecrCommand, KeyCommand, SetCommand, TextCommand,
    TouchCommand,
  },
  connection::{self, spawn_connection, ServerError},
};
use tokio::{
  io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufStream},
  net::{TcpListener, UnixListener},
  sync::{mpsc, oneshot},
};
use url::Url;

#[tokio::main]
async fn main() -> io::Result<()> {
  let cmd = clap::Command::new("kvp")
    .version("1.0")
    .author("Maxime Bedard <maxime@bedard.dev>")
    .arg(
      clap::Arg::new("bind-url")
        .short('b')
        .long("bind-url")
        .default_value("tcp://[::]:11212")
        .value_parser(Url::parse),
    )
    .arg(
      clap::Arg::new("upstream-url")
        .short('u')
        .required(true)
        .long("upstream-url")
        .action(clap::ArgAction::Append)
        .value_parser(parse_named_url),
    );

  let matches = cmd.get_matches();

  let bind_url = matches.get_one::<Url>("bind-url").unwrap();
  let upstream_urls = matches
    .get_many::<(String, Url)>("upstream-url")
    .unwrap()
    .collect::<Vec<_>>();

  let (sender, receiver) = mpsc::channel(32);

  spawn_connection(
    receiver,
    upstream_urls[0].1.clone(),
    Duration::from_secs(60 * 5),
    Duration::from_secs(60 * 30),
  );

  println!("{:?}", upstream_urls);

  // let mut router = Router::default();

  // router.insert_prefix_route("shard1:", sender.clone());
  // router.insert_prefix_route("shard2:", sender.clone());
  // router.replace_catch_all(sender.clone());

  // let router = Arc::new(router);

  let interrupt = tokio::signal::ctrl_c();
  tokio::pin!(interrupt);

  match bind_url.scheme() {
    "tcp" => {
      let host = bind_url.host_str().unwrap_or("localhost");
      let port = bind_url.port().unwrap_or(11211);
      let listener = TcpListener::bind((host, port)).await?;
      loop {
        tokio::select! {
          _ = &mut interrupt => break,
          Ok((stream, _addr)) = listener.accept() => {
            let stream = BufStream::new(stream);
            tokio::task::spawn(handle_stream(stream, sender.clone()));
          },
        }
      }
    }
    "unix" => {
      let path = bind_url.path();
      let listener = UnixListener::bind(path)?;
      loop {
        tokio::select! {
          _ = &mut interrupt => break,
          Ok((stream, _addr)) = listener.accept() => {
            let stream = BufStream::new(stream);
            tokio::task::spawn(handle_stream(stream, sender.clone()));
          },
        }
      }
    }
    _ => unimplemented!(),
  };

  Ok(())
}

fn parse_named_url(input: &str) -> Result<(String, Url), String> {
  let (name, url) = input.split_once('=').ok_or_else(|| "invalid format".to_string())?;
  let name = name.to_string();
  let url = Url::parse(url).map_err(|err| err.to_string())?;
  Ok((name, url))
}

async fn handle_stream(
  mut stream: impl AsyncBufRead + AsyncWrite + Unpin,
  mut client: mpsc::Sender<connection::Command>,
) {
  let interrupt = tokio::signal::ctrl_c();
  tokio::pin!(interrupt);

  loop {
    tokio::select! {
        _ = &mut interrupt => break,
        r = codec::read_command(&mut stream) => match r {
          Ok(command) => {
            proxy_command(&mut stream, &mut client, command).await.unwrap();
            stream.flush().await.unwrap();
          },
          Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => break,
          Err(err) => println!("{:?}", err),
        }
    }
  }

  stream.shutdown().await.unwrap();
}

struct BinaryResponseWriter<W> {
  w: W,
}

impl<W> BinaryResponseWriter<W>
where
  W: AsyncWrite + Unpin,
{
  async fn write_set_command_response(&mut self, _op: u8, r: connection::Result<()>) -> io::Result<()> {
    todo!()
  }

  async fn write_append_prepend_command_response(&mut self, _op: u8, r: connection::Result<()>) -> io::Result<()> {
    todo!()
  }

  async fn write_delete_command_response(&mut self, op: u8, r: connection::Result<()>) -> io::Result<()> {
    todo!()
  }

  async fn write_incr_decr_command_response(&mut self, op: u8, r: connection::Result<u64>) -> io::Result<()> {
    todo!()
  }

  async fn write_get_command_response(&mut self, op: u8, key: &str, r: connection::Result<Bytes>) -> io::Result<()> {
    todo!()
  }

  async fn write_touch_command_response(&mut self, op: u8, r: connection::Result<()>) -> io::Result<()> {
    todo!()
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
      Err(connection::Error::Server(ServerError::KeyExists)) => self.w.write_all(b"EXISTS\r\n").await,
      Err(connection::Error::Server(ServerError::KeyNotFound)) => self.w.write_all(b"NOT_FOUND\r\n").await,
      Err(connection::Error::Server(ServerError::Unknown(code))) => {
        self.w.write_all(format!("SERVER_ERROR {code}\r\n").as_bytes()).await
      }
    }
  }

  async fn write_append_prepend_command_response(&mut self, r: connection::Result<()>) -> io::Result<()> {
    match r {
      Ok(()) => self.w.write_all(b"STORED\r\n").await,
      Err(connection::Error::Io(err)) => Err(err),
      Err(connection::Error::Server(ServerError::KeyExists)) => self.w.write_all(b"EXISTS\r\n").await,
      Err(connection::Error::Server(ServerError::KeyNotFound)) => self.w.write_all(b"NOT_FOUND\r\n").await,
      Err(connection::Error::Server(ServerError::Unknown(code))) => {
        self.w.write_all(format!("SERVER_ERROR {code}\r\n").as_bytes()).await
      }
    }
  }

  async fn write_delete_command_response(&mut self, r: connection::Result<()>) -> io::Result<()> {
    match r {
      Ok(()) => self.w.write_all(b"STORED\r\n").await,
      Err(connection::Error::Io(err)) => Err(err),
      Err(connection::Error::Server(ServerError::KeyExists)) => self.w.write_all(b"EXISTS\r\n").await,
      Err(connection::Error::Server(ServerError::KeyNotFound)) => self.w.write_all(b"NOT_FOUND\r\n").await,
      Err(connection::Error::Server(ServerError::Unknown(code))) => {
        self.w.write_all(format!("SERVER_ERROR {code}\r\n").as_bytes()).await
      }
    }
  }

  async fn write_incr_decr_command_response(&mut self, r: connection::Result<u64>) -> io::Result<()> {
    todo!()
  }

  async fn write_get_command_response(&mut self, key: &str, r: connection::Result<Bytes>) -> io::Result<()> {
    todo!()
  }

  async fn write_touch_command_response(&mut self, r: connection::Result<()>) -> io::Result<()> {
    todo!()
  }
}

async fn proxy_command(
  w: impl AsyncWrite + Unpin,
  client: &mut mpsc::Sender<connection::Command>,
  command: codec::Command,
) -> io::Result<()> {
  match command {
    codec::Command::Binary(BinaryCommand::Get(KeyCommand { key })) => {
      let (sender, receiver) = oneshot::channel();
      client
        .send(connection::Command::Get {
          key: key.clone(),
          sender,
        })
        .await
        .ok();
      BinaryResponseWriter { w }
        .write_get_command_response(0x00, key.as_str(), receiver.await.unwrap())
        .await
    }

    codec::Command::Binary(BinaryCommand::GetK(KeyCommand { key })) => {
      let (sender, receiver) = oneshot::channel();
      client
        .send(connection::Command::Get {
          key: key.clone(),
          sender,
        })
        .await
        .ok();
      BinaryResponseWriter { w }
        .write_get_command_response(0x00, key.as_str(), receiver.await.unwrap())
        .await
    }

    codec::Command::Binary(BinaryCommand::GetAndTouch(TouchCommand { key, exptime })) => {
      let (sender, receiver) = oneshot::channel();
      client
        .send(connection::Command::GetAndTouch {
          key: key.clone(),
          exptime,
          sender,
        })
        .await
        .ok();
      BinaryResponseWriter { w }
        .write_get_command_response(0x00, key.as_str(), receiver.await.unwrap())
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
        .send(connection::Command::Set {
          key,
          value,
          exptime,
          cas,
          sender,
        })
        .await
        .ok();
      BinaryResponseWriter { w }
        .write_set_command_response(0x01, receiver.await.unwrap())
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
        .send(connection::Command::Set {
          key,
          value,
          exptime,
          cas,
          sender,
        })
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
        .send(connection::Command::Add {
          key: key.to_string(),
          value: value.to_vec(),
          exptime,
          cas,
          sender,
        })
        .await
        .ok();
      BinaryResponseWriter { w }
        .write_set_command_response(0x02, receiver.await.unwrap())
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
        .send(connection::Command::Add {
          key,
          value,
          exptime,
          cas,
          sender,
        })
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
        .send(connection::Command::Add {
          key: key.to_string(),
          value: value.to_vec(),
          exptime,
          cas,
          sender,
        })
        .await
        .ok();
      BinaryResponseWriter { w }
        .write_set_command_response(0x03, receiver.await.unwrap())
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
        .send(connection::Command::Add {
          key: key.to_string(),
          value: value.to_vec(),
          exptime,
          cas,
          sender,
        })
        .await
        .ok();
      TextResponseWriter { w }
        .write_set_command_response(receiver.await.unwrap())
        .await
    }

    codec::Command::Binary(BinaryCommand::Append(AppendPrependCommand { key, value })) => {
      let (sender, receiver) = oneshot::channel();
      client
        .send(connection::Command::Append {
          key: key.to_string(),
          value: value.to_vec(),
          sender,
        })
        .await
        .ok();
      BinaryResponseWriter { w }
        .write_append_prepend_command_response(0x04, receiver.await.unwrap())
        .await
    }

    codec::Command::Text(TextCommand::Append(AppendPrependCommand { key, value })) => {
      let (sender, receiver) = oneshot::channel();
      client
        .send(connection::Command::Append { key, value, sender })
        .await
        .ok();
      TextResponseWriter { w }
        .write_append_prepend_command_response(receiver.await.unwrap())
        .await
    }

    codec::Command::Binary(BinaryCommand::Prepend(AppendPrependCommand { key, value })) => {
      let (sender, receiver) = oneshot::channel();
      client
        .send(connection::Command::Prepend {
          key: key.to_string(),
          value: value.to_vec(),
          sender,
        })
        .await
        .ok();
      BinaryResponseWriter { w }
        .write_append_prepend_command_response(0x05, receiver.await.unwrap())
        .await
    }

    codec::Command::Text(TextCommand::Prepend(AppendPrependCommand { key, value })) => {
      let (sender, receiver) = oneshot::channel();
      client
        .send(connection::Command::Prepend { key, value, sender })
        .await
        .ok();
      TextResponseWriter { w }
        .write_append_prepend_command_response(receiver.await.unwrap())
        .await
    }

    codec::Command::Binary(BinaryCommand::Delete(KeyCommand { key })) => {
      let (sender, receiver) = oneshot::channel();
      client.send(connection::Command::Delete { key, sender }).await.ok();
      BinaryResponseWriter { w }
        .write_delete_command_response(0x05, receiver.await.unwrap())
        .await
    }

    codec::Command::Text(TextCommand::Delete(KeyCommand { key })) => {
      let (sender, receiver) = oneshot::channel();
      client.send(connection::Command::Delete { key, sender }).await.ok();
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
        .send(connection::Command::Increment {
          key: key.to_string(),
          delta,
          init,
          exptime,
          sender,
        })
        .await
        .ok();
      BinaryResponseWriter { w }
        .write_incr_decr_command_response(0x05, receiver.await.unwrap())
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
        .send(connection::Command::Increment {
          key: key.to_string(),
          delta,
          init,
          exptime,
          sender,
        })
        .await
        .ok();
      BinaryResponseWriter { w }
        .write_incr_decr_command_response(0x05, receiver.await.unwrap())
        .await
    }

    codec::Command::Binary(BinaryCommand::Touch(TouchCommand { key, exptime })) => {
      let (sender, receiver) = oneshot::channel();
      client
        .send(connection::Command::Touch {
          key: key.to_string(),
          exptime,
          sender,
        })
        .await
        .ok();
      BinaryResponseWriter { w }
        .write_touch_command_response(0x05, receiver.await.unwrap())
        .await
    }

    codec::Command::Binary(BinaryCommand::Flush) => todo!(),
    codec::Command::Binary(BinaryCommand::Version) => todo!(),
    codec::Command::Binary(BinaryCommand::Stats) => todo!(),
    codec::Command::Binary(BinaryCommand::Quit) => todo!(),
    codec::Command::Binary(BinaryCommand::Noop) => todo!(),
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
    _ => todo!(),
  }
}
