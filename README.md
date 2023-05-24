# mcrouteur

A memcached proxy similar to mcrouter from facebook. Uses only std & tokio.

Supports:

- [ ] Prefix routing
- [ ] Replicated pools
- [ ] Consistent hashing
- [ ] Connection pooling
- [ ] Quiet operations
- [ ] Flags
- [ ] CAS
- [x] Text protocol (Downstream)
- [x] Binary protocol (Upstream & Downstream)

Implementation notes:

- GET with multiple keys are exploded into multiple GET with a single key
- GAT with multiple keys are exploded into multiple GAT with a single key

# Tests

```sh
# integration tests
cargo test --test '*' -- --test-threads=1

# unit tests
cargo test --lib
```
