#[derive(Debug, PartialEq)]
pub struct Trie<V> {
  edges: Vec<TrieEdge<V>>,
}

impl<V> Default for Trie<V> {
  fn default() -> Self {
    Self {
      edges: Default::default(),
    }
  }
}

impl<V> FromIterator<(String, V)> for Trie<V> {
  fn from_iter<T: IntoIterator<Item = (String, V)>>(iter: T) -> Self {
    let mut trie = Self::default();
    for (k, v) in iter {
      trie.insert(k, v);
    }
    trie
  }
}

impl<'a, V> FromIterator<(&'a String, V)> for Trie<V> {
  fn from_iter<T: IntoIterator<Item = (&'a String, V)>>(iter: T) -> Self {
    let mut trie = Self::default();
    for (k, v) in iter {
      trie.insert(k, v);
    }
    trie
  }
}

impl<'a, V> FromIterator<(&'a str, V)> for Trie<V> {
  fn from_iter<T: IntoIterator<Item = (&'a str, V)>>(iter: T) -> Self {
    let mut trie = Self::default();
    for (k, v) in iter {
      trie.insert(k, v);
    }
    trie
  }
}

impl<V> Trie<V> {
  pub fn find_predecessor(&self, k: impl AsRef<str>) -> Option<&V> {
    fn find_predecessor_recursive<'a, V>(
      k: &str,
      edges: &'a [TrieEdge<V>],
      parent: Option<&'a TrieNode<V>>,
    ) -> Option<&'a TrieNode<V>> {
      match edges.iter().find(|e| k.starts_with(e.k.as_str())) {
        Some(TrieEdge { k: prefix, node, .. }) => {
          find_predecessor_recursive(&k[prefix.len()..], &node.edges, Some(node))
        }
        None => parent,
      }
    }
    find_predecessor_recursive(k.as_ref(), self.edges.as_slice(), None).and_then(|node| node.v.as_ref())
  }

  pub fn insert(&mut self, k: impl AsRef<str>, v: V) -> Option<V> {
    fn insert_recursive<V>(k: &str, v: V, edges: &mut Vec<TrieEdge<V>>) -> Option<V> {
      match edges
        .iter_mut()
        .map(|e| {
          let len = e.k.chars().zip(k.chars()).take_while(|(a, b)| a == b).count();
          (&k[..len], e)
        })
        .filter(|(lcp, _e)| !lcp.is_empty())
        .max_by_key(|(lcp, _e)| lcp.len())
      {
        Some((lcp, TrieEdge { k: prefix, node })) if lcp.len() == prefix.len() && lcp.len() == k.len() => {
          node.v.replace(v)
        }
        Some((lcp, TrieEdge { k: prefix, node })) if lcp.len() == prefix.len() => {
          insert_recursive(&k[prefix.len()..], v, node.edges.as_mut())
        }
        Some((lcp, prefix)) => {
          let mut suffix = std::mem::replace(
            prefix,
            TrieEdge {
              k: lcp.to_string(),
              node: TrieNode { edges: vec![], v: None },
            },
          );
          suffix.k = suffix.k[lcp.len()..].to_string();
          prefix.node.edges.push(suffix);
          insert_recursive(&k[prefix.k.len()..], v, prefix.node.edges.as_mut())
        }
        None => {
          edges.push(TrieEdge {
            k: k.to_string(),
            node: TrieNode {
              edges: vec![],
              v: Some(v),
            },
          });
          None
        }
      }
    }
    insert_recursive(k.as_ref(), v, self.edges.as_mut())
  }
}

#[derive(Debug, PartialEq)]
struct TrieEdge<V> {
  k: String,
  node: TrieNode<V>,
}

#[derive(Debug, PartialEq)]
struct TrieNode<V> {
  edges: Vec<TrieEdge<V>>,
  v: Option<V>,
}

#[cfg(test)]
mod tests {
  use crate::trie::{TrieEdge, TrieNode};

  use super::Trie;

  #[test]
  fn test_trie() {
    let trie1: Trie<&str> = Trie {
      edges: vec![TrieEdge {
        k: "te".to_string(),
        node: TrieNode {
          edges: vec![
            TrieEdge {
              k: "st".to_string(),
              node: TrieNode {
                edges: vec![TrieEdge {
                  k: "er".to_string(),
                  node: TrieNode {
                    edges: vec![],
                    v: Some("c"),
                  },
                }],
                v: Some("a"),
              },
            },
            TrieEdge {
              k: "am".to_string(),
              node: TrieNode {
                edges: vec![],
                v: Some("b"),
              },
            },
          ],
          v: None,
        },
      }],
    };

    let mut trie2 = Trie::default();
    trie2.insert("test", "a");
    trie2.insert("tester", "c");
    trie2.insert("team", "b");

    assert_eq!(trie1, trie2);

    assert_eq!(None, trie1.find_predecessor(""));
    assert_eq!(None, trie1.find_predecessor("t"));
    assert_eq!(None, trie1.find_predecessor("tes"));
    assert_eq!(Some(&"a"), trie1.find_predecessor("test"));
    assert_eq!(Some(&"a"), trie1.find_predecessor("tests"));
    assert_eq!(Some(&"b"), trie1.find_predecessor("team"));
    assert_eq!(Some(&"c"), trie1.find_predecessor("tester"));
    assert_eq!(Some(&"c"), trie1.find_predecessor("testers"));

    assert_eq!(Some("b"), trie2.insert("team", "d"));
  }
}
