# merkledb

Binary merkle tree database, backed by leveldb.
Built to persist [ZTYP](https://github.com/protolambda/ztyp) SSZ data structures.

*This library is under development, and not quite optimized.*

The main idea is to hit the DB as few times as possible, while still structuring it as a binary tree without duplicates.
- Lookups for a specific node do not require iteration from the top-node,
  the generalized index can be used to select a range of all known nodes at a specific position.
- The DB returns virtual nodes; to lazy-load the children nodes
- Puts of nodes with children are transformed into batch-writes before writing to the db

Keys have:
- a 3-byte namespace prefix, since leveldb doesn't have tables
- a generalized index, big-endian, length prefixed. This groups adjacent/parent nodes close together, and enables smarter iteration.
- 32 bytes of the node itself

Values have:
- a 1-byte type prefix, to distinguish single-node (`0`: `Root`) and pair-node (`1`: `PairNode`) `Node` types.
- an 8-byte little-endian slot value, to track when the node was inserted
- if a pair-node: 32 byte left node key, then 32 byte right node key


## License

MIT, see [`LICENSE`](./LICENSE) file.
