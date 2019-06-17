# Dieci
[![Build Status](https://travis-ci.com/eiri/dieci.svg?branch=master)](https://travis-ci.com/eiri/dieci)
[![Go Report Card](https://goreportcard.com/badge/github.com/eiri/dieci)](https://goreportcard.com/report/github.com/eiri/dieci)

Write-once data store.

## Summary

This is an experimental immutable binary store with _write-once_ policy infuenced by Plain 9 [Venti](https://en.wikipedia.org/wiki/Venti) storage system.

## Name

_Dieci_ is _ten_ in Italian. The name somehow maybe related to _venti_ which is _twenty_ in Italian.

## Datalog and Index format

```

Datalog: +----------+--------------------+-----------------+---
         | size (4) | score (8)          | data (...)      |
         +----------+--------------------+-----------------+---

Index:   +-----------+----------+----------+-----------+---
         | score (8) | pos (4)  | size (4) | score (8) |
         +-----------+----------+----------+-----------+---

  score (byte[8]) - Data primary key, xxHash64(data)
  position (uint32) - Position of datalog's block _starting_ from size
  size (uint32) - Size of datalog's data block _including_ its score

```

## Licence

[MIT](https://github.com/eiri/dieci/blob/master/LICENSE)
