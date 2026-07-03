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

## License

This project is licensed under the [MIT License].

[MIT License]: https://github.com/hmunye/gossip-glomers/blob/main/LICENSE

## References

- [Gossip Glomers](https://fly.io/dist-sys/)
- [Maelstrom](https://github.com/jepsen-io/maelstrom)
