use std::collections::BTreeMap;

use crc::{Crc, CRC_32_ISO_HDLC};
use tokio::{
  sync::{mpsc, oneshot},
  task::JoinHandle,
};
use url::Url;

use crate::{
  connection::{self, spawn_connection},
  trie::Trie,
};

#[derive(serde::Deserialize, serde::Serialize)]
pub struct RouterConfiguration {
  #[serde(default)]
  upstreams: BTreeMap<String, Url>,

  #[serde(default)]
  routes: BTreeMap<String, RouteConfiguration>,

  #[serde(default)]
  wildcard_route: Option<RouteConfiguration>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum RouteConfiguration {
  BroadcastSelect {
    upstreams: Vec<String>,
  },
  // BroadcastJoin {
  //   upstreams: Vec<String>,
  // },
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
  routes: Trie<Route>,
  wildcard_route: Option<Route>,
}

pub fn spawn_router(config: Option<RouterConfiguration>) -> (mpsc::Sender<connection::Command>, JoinHandle<()>) {
  let router = config
    .map(|config| {
      let upstreams = config
        .upstreams
        .into_iter()
        .map(|(name, url)| {
          let (sender, _handle) = spawn_connection(url);
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

  let (sender, mut receiver) = mpsc::channel::<connection::Command>(32);

  let handle = tokio::task::spawn(async move {
    while let Some(command) = receiver.recv().await {
      let key = command.key();
      if let Some(route) = router.routes.find_predecessor(key).or(router.wildcard_route.as_ref()) {
        match route {
          Route::Proxy { sender } => {
            let sender = sender.clone();
            tokio::task::spawn(async move {
              sender.send(command).await.ok();
            });
          }
          Route::BroadcastSelect { senders } => {
            tokio::task::spawn(broadcast(command, senders.clone()));
          }
          Route::Random { senders } => {
            let i = rand::random::<usize>() % senders.len();
            if let Some(sender) = senders.get(i) {
              let sender = sender.clone();
              tokio::task::spawn(async move {
                sender.send(command).await.ok();
              });
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
              let sender = sender.clone();
              tokio::task::spawn(async move {
                sender.send(command).await.ok();
              });
            }
          }
        }
      }
    }
  });

  (sender, handle)
}

async fn broadcast(command: connection::Command, senders: Vec<mpsc::Sender<connection::Command>>) {
  match command {
    connection::Command::Get(args, sender) => {
      let mut receiver = {
        let (sender, receiver) = mpsc::channel(32);

        for target in senders {
          let sender = sender.clone();
          let (cmd, receiver) = {
            let (sender, receiver) = oneshot::channel();
            (connection::Command::Get(args.clone(), sender), receiver)
          };

          tokio::task::spawn(async move {
            target.send(cmd).await.ok();
            if let Ok(r) = receiver.await {
              sender.send(r).await.ok();
            }
          });
        }

        receiver
      };

      if let Some(r) = receiver.recv().await {
        sender.send(r).ok();
      }
    }
    connection::Command::Delete(args, sender) => {
      let mut receiver = {
        let (sender, receiver) = mpsc::channel(32);

        for target in senders {
          let sender = sender.clone();
          let (cmd, receiver) = {
            let (sender, receiver) = oneshot::channel();
            (connection::Command::Delete(args.clone(), sender), receiver)
          };

          tokio::task::spawn(async move {
            target.send(cmd).await.ok();
            if let Ok(r) = receiver.await {
              sender.send(r).await.ok();
            }
          });
        }

        receiver
      };

      if let Some(r) = receiver.recv().await {
        sender.send(r).ok();
      }
    }
    connection::Command::GetAndTouch(args, sender) => {
      let mut receiver = {
        let (sender, receiver) = mpsc::channel(32);

        for target in senders {
          let sender = sender.clone();
          let (cmd, receiver) = {
            let (sender, receiver) = oneshot::channel();
            (connection::Command::GetAndTouch(args.clone(), sender), receiver)
          };

          tokio::task::spawn(async move {
            target.send(cmd).await.ok();
            if let Ok(r) = receiver.await {
              sender.send(r).await.ok();
            }
          });
        }

        receiver
      };

      if let Some(r) = receiver.recv().await {
        sender.send(r).ok();
      }
    }
    connection::Command::Touch(args, sender) => {
      let mut receiver = {
        let (sender, receiver) = mpsc::channel(32);

        for target in senders {
          let sender = sender.clone();
          let (cmd, receiver) = {
            let (sender, receiver) = oneshot::channel();
            (connection::Command::Touch(args.clone(), sender), receiver)
          };

          tokio::task::spawn(async move {
            target.send(cmd).await.ok();
            if let Ok(r) = receiver.await {
              sender.send(r).await.ok();
            }
          });
        }

        receiver
      };

      if let Some(r) = receiver.recv().await {
        sender.send(r).ok();
      }
    }
    connection::Command::Set(args, sender) => {
      let mut receiver = {
        let (sender, receiver) = mpsc::channel(32);

        for target in senders {
          let sender = sender.clone();
          let (cmd, receiver) = {
            let (sender, receiver) = oneshot::channel();
            (connection::Command::Set(args.clone(), sender), receiver)
          };

          tokio::task::spawn(async move {
            target.send(cmd).await.ok();
            if let Ok(r) = receiver.await {
              sender.send(r).await.ok();
            }
          });
        }

        receiver
      };

      if let Some(r) = receiver.recv().await {
        sender.send(r).ok();
      }
    }
    connection::Command::Add(args, sender) => {
      let mut receiver = {
        let (sender, receiver) = mpsc::channel(32);

        for target in senders {
          let sender = sender.clone();
          let (cmd, receiver) = {
            let (sender, receiver) = oneshot::channel();
            (connection::Command::Set(args.clone(), sender), receiver)
          };

          tokio::task::spawn(async move {
            target.send(cmd).await.ok();
            if let Ok(r) = receiver.await {
              sender.send(r).await.ok();
            }
          });
        }

        receiver
      };

      if let Some(r) = receiver.recv().await {
        sender.send(r).ok();
      }
    }
    connection::Command::Replace(args, sender) => {
      let mut receiver = {
        let (sender, receiver) = mpsc::channel(32);

        for target in senders {
          let sender = sender.clone();
          let (cmd, receiver) = {
            let (sender, receiver) = oneshot::channel();
            (connection::Command::Set(args.clone(), sender), receiver)
          };

          tokio::task::spawn(async move {
            target.send(cmd).await.ok();
            if let Ok(r) = receiver.await {
              sender.send(r).await.ok();
            }
          });
        }

        receiver
      };

      if let Some(r) = receiver.recv().await {
        sender.send(r).ok();
      }
    }
    connection::Command::Append(args, sender) => {
      let mut receiver = {
        let (sender, receiver) = mpsc::channel(32);

        for target in senders {
          let sender = sender.clone();
          let (cmd, receiver) = {
            let (sender, receiver) = oneshot::channel();
            (connection::Command::Append(args.clone(), sender), receiver)
          };

          tokio::task::spawn(async move {
            target.send(cmd).await.ok();
            if let Ok(r) = receiver.await {
              sender.send(r).await.ok();
            }
          });
        }

        receiver
      };

      if let Some(r) = receiver.recv().await {
        sender.send(r).ok();
      }
    }
    connection::Command::Prepend(args, sender) => {
      let mut receiver = {
        let (sender, receiver) = mpsc::channel(32);

        for target in senders {
          let sender = sender.clone();
          let (cmd, receiver) = {
            let (sender, receiver) = oneshot::channel();
            (connection::Command::Prepend(args.clone(), sender), receiver)
          };

          tokio::task::spawn(async move {
            target.send(cmd).await.ok();
            if let Ok(r) = receiver.await {
              sender.send(r).await.ok();
            }
          });
        }

        receiver
      };

      if let Some(r) = receiver.recv().await {
        sender.send(r).ok();
      }
    }
    connection::Command::Increment(args, sender) => {
      let mut receiver = {
        let (sender, receiver) = mpsc::channel(32);

        for target in senders {
          let sender = sender.clone();
          let (cmd, receiver) = {
            let (sender, receiver) = oneshot::channel();
            (connection::Command::Increment(args.clone(), sender), receiver)
          };

          tokio::task::spawn(async move {
            target.send(cmd).await.ok();
            if let Ok(r) = receiver.await {
              sender.send(r).await.ok();
            }
          });
        }

        receiver
      };

      if let Some(r) = receiver.recv().await {
        sender.send(r).ok();
      }
    }
    connection::Command::Decrement(args, sender) => {
      let mut receiver = {
        let (sender, receiver) = mpsc::channel(32);

        for target in senders {
          let sender = sender.clone();
          let (cmd, receiver) = {
            let (sender, receiver) = oneshot::channel();
            (connection::Command::Decrement(args.clone(), sender), receiver)
          };

          tokio::task::spawn(async move {
            target.send(cmd).await.ok();
            if let Ok(r) = receiver.await {
              sender.send(r).await.ok();
            }
          });
        }

        receiver
      };

      if let Some(r) = receiver.recv().await {
        sender.send(r).ok();
      }
    }
  }
}
