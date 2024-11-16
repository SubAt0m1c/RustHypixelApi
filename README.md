# Hypixel Api Proxy

Simple rust web server which proxies the hypixel api.
Implements basic caching and ip based rate limits, but that's about it.

apikey should be included in the run command, so cargo run apikeyhere (use cargo run --release apikeyhere for
optimizations)

This server is somewhat built to be run on the minimum lightsail instance, which means default values are rather low.
Increasing them may be worthwhile on larger servers.
Its worth noting if using nginx you MUST pass client address through, otherwise the rate limit will be global and not
per user.