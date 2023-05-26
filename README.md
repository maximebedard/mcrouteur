# mcrouteur

A memcached proxy similar to mcrouter from facebook. Uses only std & tokio.

Supports:

- [ ] Prefix routing
- [ ] Broadcast route (Select/Join)
- [x] Hash route (CRC32)
- [ ] Connection pooling
- [ ] Quiet operations
- [ ] Flags
- [ ] CAS
- [ ] Fault injection (Error/Latency)
- [ ] Key rewrite
- [ ] Expiration rewrite
- [x] Text protocol (Downstream)
- [x] Binary protocol (Upstream & Downstream)

Implementation notes:

- GET with multiple keys are exploded into multiple GET with a single key
- GAT with multiple keys are exploded into multiple GAT with a single key
- STATS commands will return mcrouter internal stats
- NOOP is never sent upstream and is used to support GET/GAT with multiple keys
- FLUSH is not supported
- QUIT is not supported

# Tests

```sh
# integration tests
cargo test --test '*' -- --test-threads=1

# unit tests
cargo test --lib
```

# Sample configuration

```json
{
  "upstreams": {
    "a": "tcp://[::]:11211",
    "b": "tcp://[::]:11212",
    "c": "tcp://[::]:11212"
  },
  "routes": {
    "/a/": { "type": "<type>", .. },
    "/b/": { "type": "<type>", .. }
  },
  "wildcard_route": {"type": "<type>", ..}
}
```

Proxy configuration

```json
{
  "upstreams": { "primary": "tcp://[::]:11211" },
  "routes": {},
  "wildcard_route": { "type": "proxy", "upstream": "primary" }
}
```

# Types of routes

- BroadcastSelect: Broadcast to all upstreams and return the first to reply
- BroadcastJoin: Broadcast to all upstreams and return the worst reply
- Random: Sends the request to a random upstream
- Hash: Sends the request consistently based on the hash generated from the key
- Proxy: Proxy the request to a single upstream
