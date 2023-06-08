use std::{io, path::PathBuf};

use mcrouteur::{codec, connection, proxy, router};
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
        .default_value("tcp://localhost:11211") // TODO: ipv6
        .value_parser(Url::parse),
    )
    .arg(
      clap::Arg::new("config-file")
        .long("config-file")
        .value_name("JSON-FILE")
        .conflicts_with("config")
        .value_parser(clap::value_parser!(PathBuf)),
    )
    .arg(
      clap::Arg::new("config")
        .long("config")
        .value_name("JSON")
        .value_parser(clap::value_parser!(String)),
    );

  let mut matches = cmd.get_matches();

  let bind_url = matches.remove_one::<Url>("bind-url").unwrap();

  let mut router_configuration = None;
  if let Some(config) = matches.remove_one::<PathBuf>("config-file") {
    let bytes = tokio::fs::read(config).await?;
    router_configuration = serde_json::from_slice(bytes.as_slice()).map_err(|err| {
      io::Error::new(
        io::ErrorKind::InvalidData,
        format!("Failed to parse router configuration: {err}"),
      )
    })?;
  } else if let Some(config) = matches.remove_one::<String>("config") {
    router_configuration = serde_json::from_str(config.as_str()).map_err(|err| {
      io::Error::new(
        io::ErrorKind::InvalidData,
        format!("Failed to parse router configuration: {err}"),
      )
    })?;
  }
  if let Some(router_configuration) = &router_configuration {
    eprintln!("Starting with configuration:");
    eprintln!("{}", serde_json::to_string_pretty(router_configuration).unwrap());
  }
  eprintln!("Listening on {bind_url}...");

  let (sender, _handle) = router::spawn_router(router_configuration);

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

  eprintln!("Shutting down...");

  Ok(())
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
            match proxy::proxy_command(&mut stream, &mut client, command).await {
              Ok(()) => {},
              Err(err) => eprintln!("{:?}", err),
            }
          },
          Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => break,
          Err(err) => eprintln!("{:?}", err),
        }
    }
  }

  stream.shutdown().await.unwrap();
}
