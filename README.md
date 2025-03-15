# cratesync

Install:

```shell
cargo install --git https://github.com/m-ou-se/cratesync
```

Then run:

```
cratesync ~/crates-io-mirror
```

This will download all `.crate` files from crates.io and store them in the specified directory.

Run the command again later to update the mirror. It will add new files, but leave existing files alone.
