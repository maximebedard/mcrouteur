# mcrouteur

A memcached proxy similar to mcrouter from facebook. Built for fun.

Supports:

- [x] Prefix routing
- [x] Broadcast route (Select/Join)
- [x] Hash route (CRC32)
- [x] Connection pooling (simple round-robin)
- [ ] Quiet operations
  - [x] SetQ/AddQ/ReplaceQ/AppendQ/PrependQ
  - [ ] Pipelining (GetQ/GatQ/Noop)
- [x] Flags
- [ ] CAS
- [ ] Fault injection (Error/Latency)
- [ ] Rewrite (Key/Expiration)
- [x] Text protocol (Downstream)
- [x] Binary protocol (Upstream & Downstream)

Implementation notes:

- GET with multiple keys are exploded into multiple GET with a single key
- GAT with multiple keys are exploded into multiple GAT with a single key
- VERSION will return the mcrouteur version, not memcached
- STATS commands will return mcrouter internal stats
- NOOP is not supported (will eventually be used to support pipelining)
- FLUSH is not supported
- QUIT is not supported

# Tests

```sh
# integration tests
cargo test --test '*' -- --test-threads=1

# unit tests
cargo test --lib

# benchmarks
cargo bench
```

# Configuration examples

Multiple upstreams with prefixed routes

```json
{
  "upstreams": {
    "a": "tcp://[::]:11211",
    "b": "tcp://[::]:11212",
    "c": "tcp://[::]:11212"
  },
  "routes": {
    "/a/": { "type": "proxy", "upstream": "a" },
    "/b/": { "type": "proxy", "upstream": "b" },
    "/c/": { "type": "proxy", "upstream": "c" }
  },
  "wildcard_route": { "type": "proxy", "upstream": "a" }
}
```

Proxy configuration

```json
{
  "upstreams": { "primary": "tcp://[::]:11211" },
  "wildcard_route": { "type": "proxy", "upstream": "primary" }
}
```

# Types of routes

- BroadcastSelect: Broadcast to all upstreams and return the first to reply
- BroadcastJoin: Broadcast to all upstreams and return the worst reply
- Random: Sends the request to a random upstream
- Hash: Sends the request consistently based on the hash generated from the key
- Proxy: Proxy the request to a single upstream
