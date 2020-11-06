# golang-kv-store

Distributed key value store with the following characteristics.  

### Consistency 
- Eventually consistent with the use of a shard-level gossip protocol.
- Causally consistency with the use of vector clocks.


### Shards
- Nodes evenly distributed into K shards given R replication factor.
- Shards are translated to virtual shards given a virtual shard factor.  
Virtual shards are hashed into a consistent hash ring.

### Key Partitioning
- Keys are hashed into a consistent hash ring with predecessor shard  
ownership.
