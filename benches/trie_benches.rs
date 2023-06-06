use std::{fs::File, io::Read};

use criterion::{criterion_group, criterion_main, Criterion};
use mcrouteur::trie::Trie;

fn read_words() -> Vec<String> {
  let mut buffer = String::new();
  File::open("data/lorem.txt")
    .unwrap()
    .read_to_string(&mut buffer)
    .unwrap();
  buffer.split(char::is_whitespace).map(ToString::to_string).collect()
}

fn trie_insert(b: &mut Criterion) {
  let words = read_words();
  b.bench_function("Trie::insert", |b| {
    b.iter(|| {
      let mut trie = Trie::default();
      for w in words.iter() {
        trie.insert(w, w.len());
      }
    })
  });
}

fn trie_get(b: &mut Criterion) {
  let words = read_words();
  let mut trie = Trie::default();
  for w in words.iter() {
    trie.insert(w, w.len());
  }

  b.bench_function("Trie::find_predecessor", |b| {
    b.iter(|| {
      words
        .iter()
        .map(|w| trie.find_predecessor(w))
        .collect::<Vec<Option<&usize>>>()
    })
  });
}

criterion_group!(benches, trie_insert, trie_get);
criterion_main!(benches);
