# kvp

A memcached proxy similar to mcrouter from facebook. Uses only std & tokio.

Supports:

- [ ] Prefix routing
- [ ] Replicated pools
- [ ] Consistent hashing
- [ ] Connection pooling
- [ ] Quiet operations
- [x] Text protocol (Downstream)
- [x] Binary protocol (Upstream & Downstream)

Implementation notes:

- GET with multiple keys are exploded into multiple GET with a single key
