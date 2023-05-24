use std::{io, time::Duration};

use mcrouteur::{
  codec,
  connection::{self, spawn_connection},
  proxy,
};
use tokio::{
  io::{AsyncBufRead, AsyncWrite, AsyncWriteExt, BufStream},
  net::{TcpListener, UnixListener},
  sync::mpsc,
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
        .default_value("tcp://[::]:11211")
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
            proxy::proxy_command(&mut stream, &mut client, command).await.unwrap();
            stream.flush().await.unwrap();
          },
          Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => break,
          Err(err) => println!("{:?}", err),
        }
    }
  }

  stream.shutdown().await.unwrap();
}
