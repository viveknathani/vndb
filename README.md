# vndb

vndb is a a disk based key value store.

[![Maintainability](https://api.codeclimate.com/v1/badges/3a88100c22a2b6e20df9/maintainability)](https://codeclimate.com/github/viveknathani/vndb/maintainability)

It is based on the [bitcask paper](https://riak.com/assets/bitcask-intro.pdf).

Why did I build this? Key value stores are extremely fun to build, use, and think about.

```
// below is the structure of a row in our log, along with the size it would take.
// +-----------+---------+-----------+-----------+----------+----------+
// | timestamp | keySize | valueSize | tombstone |   key    |  value   |
// +-----------+---------+-----------+-----------+----------+----------+
// | 4 bytes   | 4 bytes | 4 bytes   | 1 byte    | variable | variable |
// +-----------+---------+-----------+-----------+----------+----------+
```

## license

[MIT](./LICENSE)
