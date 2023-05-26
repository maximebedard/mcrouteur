use std::collections::BTreeMap;

use crc::{Crc, CRC_32_ISO_HDLC};
use tokio::{sync::mpsc, task::JoinHandle};
use url::Url;

use crate::connection::{self, spawn_connection};

#[derive(serde::Deserialize, serde::Serialize)]
pub struct RouterConfiguration {
  upstreams: BTreeMap<String, Url>,
  routes: BTreeMap<String, RouteConfiguration>,
  wildcard_route: Option<RouteConfiguration>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum RouteConfiguration {
  BroadcastSelect {
    upstreams: Vec<String>,
  },
  Random {
    upstreams: Vec<String>,
  },
  Hash {
    algorithm: HashAlgorithm,
    upstreams: Vec<String>,
  },
  Proxy {
    upstream: String,
  },
}

#[derive(Debug)]
enum Route {
  BroadcastSelect {
    senders: Vec<mpsc::Sender<connection::Command>>,
  },
  Random {
    senders: Vec<mpsc::Sender<connection::Command>>,
  },
  Hash {
    algorithm: HashAlgorithm,
    senders: Vec<mpsc::Sender<connection::Command>>,
  },
  Proxy {
    sender: mpsc::Sender<connection::Command>,
  },
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HashAlgorithm {
  Crc32,
}

#[derive(Debug, Default)]
struct Router {
  routes: BTreeMap<String, Route>,
  wildcard_route: Option<Route>,
}

pub fn spawn_router(
  config: Option<RouterConfiguration>,
  mut receiver: mpsc::Receiver<connection::Command>,
) -> JoinHandle<()> {
  let router = config
    .map(|config| {
      let upstreams = config
        .upstreams
        .into_iter()
        .map(|(name, url)| {
          let (sender, receiver) = mpsc::channel(32);
          spawn_connection(receiver, url);
          (name, sender)
        })
        .collect::<BTreeMap<_, _>>();

      let to_route = |r| match r {
        RouteConfiguration::BroadcastSelect { upstreams: names } => {
          let senders = names
            .iter()
            .filter_map(|upstream| upstreams.get(upstream).cloned())
            .collect();
          Some(Route::BroadcastSelect { senders })
        }
        RouteConfiguration::Random { upstreams: names } => {
          let senders = names
            .iter()
            .filter_map(|upstream| upstreams.get(upstream).cloned())
            .collect();
          Some(Route::Random { senders })
        }
        RouteConfiguration::Hash {
          algorithm,
          upstreams: names,
        } => {
          let senders = names
            .iter()
            .filter_map(|upstream| upstreams.get(upstream).cloned())
            .collect();
          Some(Route::Hash { algorithm, senders })
        }
        RouteConfiguration::Proxy { upstream: name } => {
          upstreams.get(&name).cloned().map(|sender| Route::Proxy { sender })
        }
      };

      let wildcard_route = config.wildcard_route.and_then(to_route);
      let routes = config
        .routes
        .into_iter()
        .filter_map(|(k, v)| to_route(v).map(|v| (k, v)))
        .collect();
      Router { routes, wildcard_route }
    })
    .unwrap_or_default();

  tokio::task::spawn(async move {
    while let Some(command) = receiver.recv().await {
      match command {
        connection::Command::Get { ref key, .. }
        | connection::Command::GetAndTouch { ref key, .. }
        | connection::Command::Touch { ref key, .. }
        | connection::Command::Set { ref key, .. }
        | connection::Command::Add { ref key, .. }
        | connection::Command::Replace { ref key, .. }
        | connection::Command::Append { ref key, .. }
        | connection::Command::Prepend { ref key, .. }
        | connection::Command::Increment { ref key, .. }
        | connection::Command::Decrement { ref key, .. }
        | connection::Command::Delete { ref key, .. } => {
          if let Some(route) = router.routes.get(key).or(router.wildcard_route.as_ref()) {
            match route {
              Route::Proxy { sender } => {
                sender.send(command).await.ok();
              }
              Route::BroadcastSelect { senders: _ } => {
                todo!();
              }
              Route::Random { senders } => {
                let i = rand::random::<usize>() % senders.len();
                if let Some(sender) = senders.get(i) {
                  sender.send(command).await.ok();
                }
              }
              Route::Hash { algorithm, senders } => {
                let checksum: usize = match algorithm {
                  HashAlgorithm::Crc32 => Crc::<u32>::new(&CRC_32_ISO_HDLC)
                    .checksum(key.as_bytes())
                    .try_into()
                    .unwrap(),
                };
                let i = checksum % senders.len();
                if let Some(sender) = senders.get(i) {
                  sender.send(command).await.ok();
                }
              }
            }
          }
        }

        connection::Command::Flush { sender } => {
          sender
            .send(Err(connection::Error::Server(connection::ServerError::Unknown(999))))
            .ok();
        }

        connection::Command::Quit { sender } => {
          sender
            .send(Err(connection::Error::Server(connection::ServerError::Unknown(999))))
            .ok();
        }

        connection::Command::Stats { sender } => {
          sender.send(Ok(BTreeMap::new())).ok();
        }

        connection::Command::Version { sender } => {
          sender.send(Ok(env!("CARGO_PKG_VERSION").to_string())).ok();
        }

        connection::Command::Noop { sender } => {
          sender.send(Ok(())).ok();
        }
      }
    }
  })
}
