# gossip-glomers

Solutions to the [Gossip Glomers](https://fly.io/dist-sys/) distributed systems
challenges.

## Running Workloads

```bash
docker build -t maelstrom .
```

### "echo":

```bash
./maelstrom.sh test -w echo --bin /usr/local/bin/echo --node-count 1 --time-limit 10
```

### "unique-ids":

```bash
./maelstrom.sh test -w unique-ids --bin /usr/local/bin/unique_ids --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
```

### "broadcast" (single-node):

```bash
./maelstrom.sh test -w broadcast --bin /usr/local/bin/broadcast --node-count 1 --time-limit 20 --rate 10
```

### "broadcast" (multi-node):

```bash
./maelstrom.sh test -w broadcast --bin /usr/local/bin/broadcast --node-count 5 --time-limit 20 --rate 10
```

### "broadcast" (fault-tolerant):

```bash
./maelstrom.sh test -w broadcast --bin /usr/local/bin/broadcast --node-count 5 --time-limit 20 --rate 10 --nemesis partition
```

### "broadcast" (efficient):

```bash
./maelstrom.sh test -w broadcast --bin /usr/local/bin/broadcast --node-count 25 --time-limit 20 --rate 100 --latency 100
```

#### 3d: Efficient Broadcast, Part I:

- Messages-per-operation is below 30
- Median latency is below 400ms
- Maximum latency is below 600ms

```go
broadcast.New(n).
    WithFanout(4).
    WithInterval(120 * time.Millisecond).
    Run()
```

#### 3e: Efficient Broadcast, Part II:

- Messages-per-operation is below 20
- Median latency is below 1 second
- Maximum latency is below 2 seconds

```go
broadcast.New(n).
    WithFanout(3).
    WithInterval(150 * time.Millisecond).
    Run()
```

### "g-counter":

```bash
./maelstrom.sh test -w g-counter --bin /usr/local/bin/g_counter --node-count 3 --rate 100 --time-limit 20 --nemesis partition
```

## License

This project is licensed under the [MIT License].

[MIT License]: https://github.com/hmunye/gossip-glomers/blob/main/LICENSE

## References

- [Gossip Glomers](https://fly.io/dist-sys/)
- [Maelstrom](https://github.com/jepsen-io/maelstrom)
