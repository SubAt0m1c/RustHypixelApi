# Hypixel Api Proxy

Simple rust web server which proxies the hypixel api.
Implements basic caching and ip based rate limits, but that's about it.

apikey should be included in the run command, so cargo run apikeyhere (use cargo run --release apikeyhere for
optimizations)

This server is somewhat built to be run on the minimum lightsail instance, which means default values are rather low.
Increasing them may be worthwhile on larger servers.
Its worth noting if using nginx you MUST pass client address through, otherwise the rate limit will be global and not
per user.

Installation and usage:

1. git clone this repo
2. move to the repo using `cd RustHypixelApi`
3. source the script by running`source install.sh` and follow its instructions
4. build the repo with `cargo build --release`
5. run using pm2 by running `pm2 start "cargo run --release replacetextwithapikey"`
6. access via set url

To update the server:

1. move to the clone using `cd RustHypixelApi`
2. run `git pull`
3. rebuild the repo using `cargo build --release`
4. run `pm2 restart 0` to restart the server