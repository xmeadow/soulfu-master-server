# SoulFu Master Server

Central coordination server for SoulFu multiplayer. Implements the ServerFu network protocol for shard discovery, join coordination, machine tracking, and IP list management.

## Building

```
make
```

Requires a C compiler (gcc, clang, etc.). No external dependencies.

## Usage

```
./master_server [port]
```

Default port is 17859. The server listens for UDP packets from game clients.

## Protocol

The server handles the following ServerFu packet types:

| Packet | Direction | Purpose |
|--------|-----------|---------|
| REQUEST_SHARD_LIST | client -> server | Discover available shards, includes version check |
| REPLY_SHARD_LIST | server -> client | Shard validity flags + map server IPs |
| REPLY_VERSION_ERROR | server -> client | Exe/data version mismatch |
| REQUEST_PLAYER_COUNT | client -> server | Query total players online |
| REPLY_PLAYER_COUNT | server -> client | Number of connected machines |
| REQUEST_JOIN | client -> server | Join a game (version + password check) |
| COMMAND_JOIN | server <-> client | Coordinates join handshake between machines |
| REPLY_ROGER | client -> server | Acknowledgment |
| REQUEST_IP_LIST | client -> server | Request machine list (64 per portion) |
| REPLY_IP_LIST | server -> client | Room positions + IPs of machines |
| REPORT_MACHINE_DOWN | client -> server | Report a disconnected machine |
| HEARTBEAT | client -> server | Keep-alive (3 min timeout) |
| REPORT_POSITION | client -> server | Update room x/y/z position |

Room-level packets (room join, character/particle sync, room orders) are peer-to-peer between game clients and do not pass through this server.

See the [ServerFu protocol spec](https://github.com/soulfu/soulfu/wiki/Development:-ServerFu) for full details.

## License

GPL-3.0 — same as SoulFu.
