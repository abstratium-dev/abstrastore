# Abstrastore

A very simple library that encapsulates a database using Minio as a file store. Designed to be embedded in multiple instances
of a microservice where the concurrent versions read and write concurrently. The design is based on an append log.
It does not support ACID. The store is periodically compacted to improve read rate and cost.


## Roadmap

- caching and cache eviction
- observability
- metrics

## License

Apache 2.0 => see [LICENSE](LICENSE)

## Authors

Ant Kutschera

## Building / Releasing

```sh
eval "$(ssh-agent -s)"
ssh-add /.../abs.key
export VERS=0.0.x
git add --all && git commit -a -m'<comment>' && git tag v${VERS} && git push origin main v${VERS}
```

## TODO

- add using https://pkg.go.dev/about#adding-a-package
