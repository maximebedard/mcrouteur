use std::{collections::BTreeMap, sync::Arc};

use tokio::sync::RwLock;

pub mod memcached;
pub mod redis;

#[derive(Debug)]
pub struct Router<C> {
    prefixes: BTreeMap<String, C>,
    catch_all: Option<C>,
}

impl<C> Default for Router<C> {
    fn default() -> Self {
        let prefixes = Default::default();
        let catch_all = Default::default();
        Self {
            prefixes,
            catch_all,
        }
    }
}

impl<C> Router<C> {
    pub fn get_connection(&self, k: &String) -> Option<&C> {
        self.prefixes.get(k).or(self.catch_all.as_ref())
    }

    pub fn insert_prefix_route(&mut self, key: impl Into<String>, value: C) -> Option<C> {
        self.prefixes.insert(key.into(), value)
    }

    pub fn replace_catch_all(&mut self, value: C) -> Option<C> {
        self.catch_all.replace(value)
    }
}

#[derive(Debug)]
pub struct ConnectionPool<C> {
    inner: Arc<RwLock<ConnectionPoolInner<C>>>,
}

impl<C> Default for ConnectionPool<C> {
    fn default() -> Self {
        let inner = Default::default();
        Self { inner }
    }
}

impl<C> Clone for ConnectionPool<C> {
    fn clone(&self) -> Self {
        let inner = self.inner.clone();
        Self { inner }
    }
}

#[derive(Debug)]
pub struct ConnectionPoolInner<C> {
    connections: Vec<C>,
}

impl<C> Default for ConnectionPoolInner<C> {
    fn default() -> Self {
        let connections = Default::default();
        Self { connections }
    }
}
