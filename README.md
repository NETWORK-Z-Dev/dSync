# dSync

dSync stands for decentralized synchronization. The goal is to have an **easy way** for listening to events and emitting events to create a decentralized network. In order for this to work you need to know and add other nodes like in the example below. The usage and syntax was inspired by socket.io which is why it is pretty similar to it.

------

## Init

Straight up dSync requires express to work. When initializing dSync using `new dSync()` you need to provide a project name. This way a single node can be part of multiple decentralized networks without clashing together.

```js
import express from "express";
import dSync from "@hackthedev/dsync";

const app = express();
let sync = new dSync("your-project-name", app)
```

------

## Adding peers

Afterwards you need to add the other nodes. In my example im adding my own localhost instance for testing. Its important that the url is reachable.

> [!IMPORTANT]
>
> Do not add yourself to the peers in production, as this was done for testing only.

```js
sync.addPeer("http://localhost:2052")
```

------

## Listening for events

To listen for events you could do something like below. This example uses `responses` and rate limit options `{ipRequestLimit: 1, requestLimit: 10}`. The numbers represent the maximum requests per minute. Everything above will be rejected with a error similar to `Rate limit exceeded`.

The `ipRequestLimit` is bound to the node emitting this event. In order to prevent further abuse and bypasses like VPNs or Proxy Servers, there is a general rate limit, `requestLimit`.

```js
sync.on("ping", { ipRequestLimit: 1, requestLimit: 10 }, (payload, response) => {
    console.log("payload:", payload)
    response({ pong: true, from: "B" })
})
```

------

## Emitting global events

You can easily emit events to all known peers with the following example:

```js
sync.emit("ping", { hello: "A and C" }, (res) => {
    console.log("Response:", res)
})
```

------

## Emitting to specific peers

To emit to specific peers you can add an array called `peers` like this: `peers: ["http://localhost:2052"]` and it will only emit to these peers. This is helpful if you target specific peers.

```js
sync.emit("ping", { 
    hello: "A and C", 
    peers: ["http://localhost:2052"]
}, (res) => {
    console.log("Response:", res)
})
```

------

## Example Result

Based on the two examples above it will result in the following output:

```js
payload:  { hello: 'A and C' } // receiver
Response: { url: 'http://localhost:2052', data: { pong: true, from: 'B' } } // receiver response on sender
```

------

## Handling Rate Limits

On default when a rate limit is reached it will automatically respond with a rate limit error and cancel further execution. If you want to catch the rate limit, you can supply a parameter called `handleRateLimit` and set it to true. This way its execution continues, and you can cache the rate limit, like `ipRateLimited` or `rateLimited`. 

You can also get the rate limited IP address using `payload.rateLimitedIP`. Optionally you can send a response back. 

The idea of this is that you could for example set high rate limits, and if someone reaches the ip rate limit you could automatically create penalties or block a server for some time. The possibilities are endless here.

```js
sync.on("ping", { ipRequestLimit: -1, requestLimit: 10, handleRateLimit: true }, (payload, response) => {
    if (payload.ipRateLimited) {
        console.log(payload.rateLimitedIP)
        response({ error: "IP Rate Limit reached!" })
        return;
    }
    if (payload.rateLimited) {
        console.log(payload.rateLimitedIP)
        response({ error: "Rate Limit reached." })
        return;
    }

    console.log("payload:", payload)
    response({ pong: true, from: "B" })
})

```
