# Hypixel Api Proxy

Rust web server to proxy the hypixel api with caching and rate limiting.
Backend is almost entirely handled by [actix-web](https://crates.io/crates/actix-web), [actix-governor](https://crates.io/crates/actix-governor) (rate limiting) and [moka](https://crates.io/crates/moka) (caching).
Cache is compressed via [lz4](https://crates.io/crates/lz4_flex).

Currently only has 2 paths, full skyblock profile via `/get/<uuid>` and accross profile secrets via achievement data at `/secrets/<uuid>`

Api key should be an environment variable. Per session can be set with `export API_KEY="<apikeyhere>"`

While made for [HateCheaters](https://github.com/SubAt0m1c/HateCheaters), it should work just fine for any other projects as long as the expected paths are the same.
Its built to run on a minimum lightsail instance, with a constant value for max cache entries of 125, due to low memory.
If you intend on running this on a larger server, fork the repository and update the max cache entries to whatever you expect to use.
Large players may take ~1.2mb to store all profile data in the cache (after compression).

By default this is expected to be run through a reverse proxy with port 8000. You will need to pass client ip through.
The installation script will automatically handle installing nginx and setting this, however.

Installation and usage: (These instructions only apply to a fresh server. The script may not work as intended otherwise)

1. git clone this repo
2. move to the repo using `cd RustHypixelApi`
3. source the script by running`source install.sh` and follow its instructions
4. build the repo with `cargo build --release`
5. set api key as an environment variable with key `API_KEY`
6. run using pm2 by running `pm2 start "cargo run --release"`
7. access via set url

To update the server:

1. move to the clone using `cd RustHypixelApi`
2. run `git pull`
3. rebuild the repo using `cargo build --release`
4. run `pm2 restart 0` to restart the server
