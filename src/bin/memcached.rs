use std::{io, sync::Arc};

use clap::{Arg, ArgAction, Command};
use tokio::{
  io::{AsyncBufReadExt, BufReader, BufStream, BufWriter},
  net::{TcpListener, TcpStream, UnixListener, UnixStream},
};
use url::Url;

use kvp::{memcached, spawn_manager, ConnectionPool, ConnectionPoolBuilder, Router};

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

  println!("{:?}", upstream_urls);
  let manager = spawn_manager().await;
  let connection_pool = ConnectionPoolBuilder::new(manager, 1).build();

  let mut router = Router::default();

  router.insert_prefix_route("shard1:", connection_pool.clone());
  router.insert_prefix_route("shard2:", connection_pool.clone());
  router.replace_catch_all(connection_pool.clone());

  let router = Arc::new(router);

  let interrupt = tokio::signal::ctrl_c();
  tokio::pin!(interrupt);

  match bind_url.scheme() {
    "tcp" => {
      let host = bind_url.host_str().unwrap_or("localhost");
      let port = bind_url.port().unwrap_or(11211);
      let listener = TcpListener::bind((host, port)).await?;
      loop {
        tokio::select! {
            Ok((stream, _addr)) = listener.accept() => {
                tokio::task::spawn(handle_tcp_stream(stream, router.clone()));
            },
            Ok(_) = &mut interrupt => break,
        }
      }
    }
    "unix" => {
      let path = bind_url.path();
      let listener = UnixListener::bind(path)?;
      loop {
        tokio::select! {
            Ok((stream, _addr)) = listener.accept() => {
                tokio::task::spawn(handle_unix_stream(stream, router.clone()));
            },
            Ok(_) = &mut interrupt => break,
        }
      }
    }
    _ => unimplemented!(""),
  };

  Ok(())
}

fn parse_named_url(input: &str) -> Result<(String, Url), String> {
  let (name, url) = input.split_once("=").ok_or_else(|| "invalid format".to_string())?;
  let name = name.to_string();
  let url = Url::parse(url).map_err(|err| err.to_string())?;
  Ok((name, url))
}

async fn handle_tcp_stream(stream: TcpStream, router: Arc<Router<ConnectionPool<memcached::Connection>>>) {
  println!("connect");
  let (r, w) = stream.into_split();
  let mut r = BufReader::new(r);
  let w = BufWriter::new(w);

  let interrupt = tokio::signal::ctrl_c();
  tokio::pin!(interrupt);

  loop {
    tokio::select! {
        _ = &mut interrupt => break,
        r = memcached::read_command(&mut r) => match r {
            Ok(Some(b)) => {
                // println!("{}", l);
                let command = memcached::decode_text_command(&b[..]);
                println!("{:?}", command);
            },
            Ok(None) => break,
            Err(err) => println!("{:?}", err),
        }
    }
  }
  println!("disconnect");
}

async fn handle_unix_stream(stream: UnixStream, router: Arc<Router<ConnectionPool<memcached::Connection>>>) {}
