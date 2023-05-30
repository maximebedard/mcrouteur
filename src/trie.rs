// https://en.wikipedia.org/wiki/Radix_tree

use std::fmt::Debug;

#[derive(Debug, Default)]
pub struct Trie<V> {
  root: Option<TrieNode<V>>,
}

impl<V> Trie<V> {
  pub fn find_predecessor(&self, k: impl AsRef<str>) -> Option<&V> {
    let mut k = k.as_ref();
    let mut current = self.root.as_ref();

    loop {
      match current {
        Some(TrieNode::Edges(edges)) => match edges.iter().find(|e| k.starts_with(e.k.as_str())) {
          Some(edge) => {
            k = &k[..edge.k.len()];
            current = Some(&edge.node);
          }
          None => break,
        },
        Some(TrieNode::Leaf(_)) => break,
        None => break,
      }
    }

    current.and_then(|v| match v {
      TrieNode::Edges(_) => None,
      TrieNode::Leaf(v) => Some(v),
    })
  }

  pub fn insert(&mut self, k: impl AsRef<str>, v: V) {
    // let mut k = k.as_ref();
    // match self.root.as_mut() {
    //   Some(TrieNode::Edges(edges)) => {
    //     let mut current = edges;

    //     loop {
    //       match current
    //         .iter_mut()
    //         .map(|e| (e.k.chars().zip(k.chars()).take_while(|(a, b)| a == b).count(), e))
    //         .filter(|(len, _e)| *len > 0)
    //         .max_by_key(|(len, _e)| *len)
    //         .map(|(_len, e)| e)
    //       {
    //         Some(TrieEdge {
    //           k: prefix,
    //           node: TrieNode::Edges(edges),
    //         }) if prefix.len() < k.len() => {}
    //         Some(TrieEdge {
    //           k: prefix,
    //           node: TrieNode::Leaf(edges),
    //         }) if prefix.len() < k.len() => {}
    //         Some(TrieEdge {
    //           k: prefix,
    //           node: TrieNode::Edges(edges),
    //         }) => {
    //           edges.push(TrieEdge { k: prefix.to_string(), node: () })
    //           break;
    //         }
    //         Some(TrieEdge {
    //           k: prefix,
    //           node: TrieNode::Leaf(_v),
    //         }) => {
    //           // replace existing value
    //           break;
    //         }
    //         None => {
    //           current.push(TrieEdge {
    //             k: k.to_string(),
    //             node: TrieNode::Leaf(v),
    //           });
    //           break;
    //         }
    //       }
    //     }
    //   }
    //   Some(TrieNode::Leaf(_)) => unreachable!(),
    //   None => {
    //     self.root = Some(TrieNode::Edges(vec![TrieEdge {
    //       k: k.to_string(),
    //       node: TrieNode::Leaf(v),
    //     }]))
    //   }
    // }
  }
}

#[derive(Debug)]
struct TrieEdge<V> {
  k: String,
  node: TrieNode<V>,
}

#[derive(Debug)]
enum TrieNode<V> {
  Edges(Vec<TrieEdge<V>>),
  Leaf(V),
}

#[cfg(test)]
mod tests {
  use super::Trie;

  #[test]
  fn test_trie() {
    let mut trie = Trie::default();
    trie.insert("test", "a");
    trie.insert("team", "b");
    // trie.insert("toast", "c");
    // trie.insert("tester", "d");

    println!("{:#?}", trie);

    assert_eq!(None, trie.find_predecessor("t"));
    assert_eq!(Some(&"a"), trie.find_predecessor("test"));
    assert_eq!(Some(&"a"), trie.find_predecessor("tests"));
  }
}
