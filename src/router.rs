use std::collections::BTreeMap;

#[derive(Debug)]
pub struct Router<C> {
  prefixes: BTreeMap<String, C>,
  catch_all: Option<C>,
}

impl<C> Default for Router<C> {
  fn default() -> Self {
    let prefixes = Default::default();
    let catch_all = Default::default();
    Self { prefixes, catch_all }
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
