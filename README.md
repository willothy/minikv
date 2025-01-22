# MiniKV

A distributed-ish in-memory key-value store in <300LOC.

## What is is, and what it is not

- It IS:
  - fast (in theory, no benchmarks yet)
  - very easy to use
  - entirely based on HTTP

- It is NOT:
  - fault tolerant
  - partition tolerant

- It COULD be (but is not right now):
  - persistent
  - secure
    - no auth is implemented at the moment
