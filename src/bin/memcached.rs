use std::{io, time::Duration};

use bytes::Bytes;
use clap::{Arg, ArgAction, Command};
use kvp::{
  codec::{
    self, decode_command, AppendPrependCommandRef, CommandRef, IncrDecrCommandRef, KeyCommandRef, Protocol,
    SetCommandRef,
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
  let cmd = Command::new("kvp")
    .version("1.0")
    .author("Maxime Bedard <maxime@bedard.dev>")
    .arg(
      Arg::new("bind-url")
        .short('b')
        .long("bind-url")
        .default_value("tcp://[::]:11212")
        .value_parser(Url::parse),
    )
    .arg(
      Arg::new("upstream-url")
        .short('u')
        .required(true)
        .long("upstream-url")
        .action(ArgAction::Append)
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
        r = read_command(&mut stream) => match r {
          Ok(Some(bytes)) => proxy_command(&mut client, &mut stream, bytes).await.unwrap(),
          Ok(None) => break,
          Err(err) => println!("{:?}", err),
        }
    }
  }

  stream.shutdown().await.unwrap();
}

async fn read_command(mut r: impl AsyncBufRead + Unpin) -> io::Result<Option<Vec<u8>>> {
  r.fill_buf().await?;
  todo!()
}

struct ResponseWriter<W> {
  w: W,
}

impl<W> ResponseWriter<W>
where
  W: AsyncWrite + Unpin,
{
  async fn write_set_command_response(&mut self, _op: u8, r: connection::Result<()>) -> io::Result<()> {
    todo!()
    // match self.protocol {
    //   codec::Protocol::Text => match r {
    //     Ok(_) => self.w.write_all(b"STORED\r\n").await,
    //     Err(connection::Error::Io(err)) => Err(err),
    //     Err(connection::Error::Server(ServerError::KeyExists)) => self.w.write_all(b"EXISTS\r\n").await,
    //     Err(connection::Error::Server(ServerError::KeyNotFound)) => self.w.write_all(b"NOT_FOUND\r\n").await,
    //     Err(connection::Error::Server(ServerError::Unknown(code))) => {
    //       self.w.write_all(format!("SERVER_ERROR {code}\r\n").as_bytes()).await
    //     }
    //   },
    //   codec::Protocol::Binary => match r {
    //     Ok(_) => todo!(),
    //     Err(connection::Error::Io(err)) => Err(err),
    //     Err(connection::Error::Server(_err)) => todo!(),
    //   },
    // }
  }

  async fn write_append_prepend_command_response(&mut self, _op: u8, r: connection::Result<()>) -> io::Result<()> {
    todo!()
    // match self.protocol {
    //   codec::Protocol::Text => match r {
    //     Ok(_) => self.w.write_all(b"STORED\r\n").await,
    //     Err(connection::Error::Io(err)) => Err(err),
    //     Err(connection::Error::Server(ServerError::KeyExists)) => self.w.write_all(b"EXISTS\r\n").await,
    //     Err(connection::Error::Server(ServerError::KeyNotFound)) => self.w.write_all(b"NOT_FOUND\r\n").await,
    //     Err(connection::Error::Server(ServerError::Unknown(code))) => {
    //       self.w.write_all(format!("SERVER_ERROR {code}\r\n").as_bytes()).await
    //     }
    //   },
    //   codec::Protocol::Binary => match r {
    //     Ok(_) => todo!(),
    //     Err(connection::Error::Io(err)) => Err(err),
    //     Err(connection::Error::Server(_err)) => todo!(),
    //   },
    // }
  }

  async fn write_incr_decr_command_response(&mut self, _op: u8, r: connection::Result<()>) -> io::Result<()> {
    todo!()
  }

  async fn write_get_command_response(&mut self, op: u8, key: &str, r: connection::Result<Bytes>) -> io::Result<()> {
    todo!()
  }
}

async fn proxy_command(
  client: &mut mpsc::Sender<connection::Command>,
  w: impl AsyncWrite + Unpin,
  input: Vec<u8>,
) -> io::Result<()> {
  let (protocol, command) = decode_command(input.as_slice()).unwrap();
  let mut rw = ResponseWriter { w };
  match command {
    CommandRef::Get(KeyCommandRef { key }) => {
      let (sender, receiver) = oneshot::channel();
      client
        .send(connection::Command::Get {
          key: key.to_string(),
          sender,
        })
        .await
        .ok();
      rw.write_get_command_response(0x00, key, receiver.await.unwrap()).await
    }
    CommandRef::GetQ(_) => todo!(),
    CommandRef::GetK(KeyCommandRef { key }) => {
      let (sender, receiver) = oneshot::channel();
      client
        .send(connection::Command::Get {
          key: key.to_string(),
          sender,
        })
        .await
        .ok();
      rw.write_get_command_response(0x00, key, receiver.await.unwrap()).await
    }
    CommandRef::GetKQ(_) => todo!(),
    CommandRef::GetAndTouch(_) => todo!(),
    CommandRef::GetAndTouchQ(_) => todo!(),
    CommandRef::Set(SetCommandRef {
      key,
      value,
      flags: _,
      exptime,
      cas,
    }) => {
      let (sender, receiver) = oneshot::channel();
      client
        .send(connection::Command::Set {
          key: key.to_string(),
          value: value.to_vec(),
          exptime,
          cas,
          sender,
        })
        .await
        .ok();
      rw.write_set_command_response(0x01, receiver.await.unwrap()).await
    }
    CommandRef::SetQ(_) => todo!(),
    CommandRef::Add(SetCommandRef {
      key,
      value,
      flags: _,
      exptime,
      cas,
    }) => {
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
      rw.write_set_command_response(0x02, receiver.await.unwrap()).await
    }
    CommandRef::AddQ(_) => todo!(),
    CommandRef::Replace(SetCommandRef {
      key,
      value,
      flags: _,
      exptime,
      cas,
    }) => {
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
      rw.write_set_command_response(0x03, receiver.await.unwrap()).await
    }
    CommandRef::ReplaceQ(_) => todo!(),
    CommandRef::Append(AppendPrependCommandRef { key, value }) => {
      let (sender, receiver) = oneshot::channel();
      client
        .send(connection::Command::Append {
          key: key.to_string(),
          value: value.to_vec(),
          sender,
        })
        .await
        .ok();
      rw.write_append_prepend_command_response(0x04, receiver.await.unwrap())
        .await
    }
    CommandRef::AppendQ(_) => todo!(),
    CommandRef::Prepend(AppendPrependCommandRef { key, value }) => {
      let (sender, receiver) = oneshot::channel();
      client
        .send(connection::Command::Prepend {
          key: key.to_string(),
          value: value.to_vec(),
          sender,
        })
        .await
        .ok();
      rw.write_append_prepend_command_response(0x05, receiver.await.unwrap())
        .await
    }
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
    CommandRef::GetM { .. } => todo!(),
    CommandRef::GetAndTouchM { .. } => todo!(),
  }
}
