import express from "express"
import { randomUUID } from "crypto"

export default class dSync {
    constructor({
                    prefix = null,
                    app = null,
                    host = null,
                    dSyncWeb = null
                } = {}) {
        if (!prefix || typeof prefix !== "string" || prefix.length <= 0) {
            console.error("Prefix string is required for dSync!")
            process.exit(0)
        }
        if (!app) {
            console.error("Express app is required for dSync!")
            process.exit(0)
        }

        if(!host) {
            console.error("Host domain is required for dSync!")
            process.exit(0)
        }

        this.host = host;

        this.prefix = prefix
        this.handlers = new Map()
        this.peers = new Set()
        this.seenEvents = new Map()
        this.gossipDelay = this.getDynamicDelay()
        this.dSyncWeb = dSyncWeb;

        app.use(express.json())
        app.post(`/${this.prefix}`, async (req, res) => {
            const { event, payload, eventId, source } = req.body
            const handlers = this.handlers.get(event) || []
            let responded = false
            const ip = req.ip || req.connection?.remoteAddress || "unknown"
            const interval = 60_000

            if (!eventId) {
                res.json({ error: "Missing eventId" })
                return
            }

            if (!this.seenEvents.has(eventId)) {
                this.seenEvents.set(eventId, { sources: new Set(), timer: null })
            }

            const seen = this.seenEvents.get(eventId)
            seen.sources.add(source || "unknown")

            if (!seen.timer) {
                seen.timer = setTimeout(() => this.seenEvents.delete(eventId), 60_000)
            }

            for (const h of handlers) {
                const now = Date.now()
                let ipRateLimited = false
                let globalRateLimited = false

                if (h.options.ipRequestLimit) {
                    if (!h.ipHits[ip]) h.ipHits[ip] = []

                    h.ipHits[ip] = h.ipHits[ip].filter(ts => now - ts < interval)

                    if (h.ipHits[ip].length >= h.options.ipRequestLimit) {
                        ipRateLimited = true
                    } else {
                        h.ipHits[ip].push(now)
                    }
                }

                if (h.options.requestLimit) {
                    h.globalHits = h.globalHits.filter(ts => now - ts < interval)

                    if (h.globalHits.length >= h.options.requestLimit) {
                        globalRateLimited = true
                    } else {
                        h.globalHits.push(now)
                    }
                }

                if ((ipRateLimited || globalRateLimited) && !h.options.handleRateLimit) {
                    if (!responded) {
                        res.json({
                            error: "Rate limit exceeded",
                            ipRateLimited,
                            rateLimited: globalRateLimited,
                            rateLimitedIP: ip
                        })
                        responded = true
                    }
                    continue
                }

                const rateInfo = {
                    ipRateLimited,
                    rateLimited: globalRateLimited,
                    rateLimitedIP: ip
                }

                // callback handler
                const maybeResponse = await new Promise(resolve => {
                    let done = false
                    const cb = (response) => {
                        if (!done) {
                            done = true
                            resolve(response)
                        }
                    }

                    try {
                        const r = h.handler(payload, cb, rateInfo)

                        if (r instanceof Promise) {
                            r.then(val => cb(val)).catch(err => cb({ error: err.message }))
                        }
                    } catch (err) {
                        cb({ error: err.message })
                    }
                })

                if (!responded) {
                    res.json({ payload: maybeResponse, ratelimit: rateInfo })
                    responded = true
                }

                // for dsync event viewer if applicable lol
                if(this.dSyncWeb) {
                    this.dSyncWeb.logEvent(event, source, {
                        payload,
                        response: maybeResponse
                    })
                }
            }

            if (!responded) res.json(undefined)
        })
    }

    getDynamicDelay() {
        const peers = this.peers.size || 1
        const base = 200
        return Math.min(10_000, Math.floor(base * Math.log(peers + 1)))
    }

    on(event, optionsOrHandler, maybeHandler) {
        let handler, options
        if (typeof optionsOrHandler === "function") {
            handler = optionsOrHandler
            options = {}
        } else {
            options = optionsOrHandler || {}
            handler = maybeHandler
        }

        if (!this.handlers.has(event)) this.handlers.set(event, [])
        this.handlers.get(event).push({
            handler,
            options,
            ipHits: {},
            globalHits: []
        })
    }

    addPeer(url, noAutoConvert = false) {
        url = noAutoConvert ? url : `https://${this.extractHost(url)}`

        // add it to db if web viewer exists
        if(this.dSyncWeb) {
            this.dSyncWeb.addNetworkServer(this.extractHost(url))
        }

        this.peers.add(url)
    }

    extractHost(url) {
        if (!url) return null;
        const s = String(url).trim();

        const looksLikeBareIPv6 = !s.includes('://') && !s.includes('/') && s.includes(':') && /^[0-9A-Fa-f:.]+$/.test(s);
        const withProto = looksLikeBareIPv6 ? `https://[${s}]` : (s.includes('://') ? s : `https://${s}`);

        try {
            const u = new URL(withProto);
            const host = u.hostname; // IPv6 returned without brackets
            const port = u.port;
            if (host.includes(':')) {
                return port ? `[${host}]:${port}` : host;
            }
            return port ? `${host}:${port}` : host;
        } catch (e) {
            const re = /^(?:https?:\/\/)?(?:[^@\/\n]+@)?([^:\/?#]+)(?::(\d+))?(?:[\/?#]|$)/i;
            const m = s.match(re);
            if (!m) return null;
            const hostname = m[1].replace(/^\[(.*)\]$/, '$1');
            const port = m[2];
            if (hostname.includes(':')) return port ? `[${hostname}]:${port}` : hostname;
            return port ? `${hostname}:${port}` : hostname;
        }
    }

    async emit(event, dataOrCallback, maybeCallback) {
        let payload = {}
        let callback

        // arguments like in socket.io
        if (typeof dataOrCallback === "function") {
            callback = dataOrCallback
        } else {
            payload = dataOrCallback
            callback = maybeCallback
        }

        const eventId = randomUUID()
        const targetPeers = Array.from(this.peers)

        this.seenEvents.set(eventId, {
            sources: new Set(["self"]),
            timer: setTimeout(() => this.seenEvents.delete(eventId), 60_000)
        })

        await new Promise(r => setTimeout(r, this.gossipDelay))

        const results = []
        for (const url of targetPeers) {
            const seen = this.seenEvents.get(eventId)
            if (seen && seen.sources.has(url)) continue

            try {
                const res = await fetch(`${url}/${this.prefix}`, {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ event, payload, eventId, source: this.host })
                })
                const raw = await res.json().catch(() => undefined)
                results.push({ host: url, payload: raw?.payload || null, ratelimit: raw?.ratelimit || null })
            } catch (err) {
                results.push({ host: url, error: err.message })
            }
        }

        if (typeof callback === "function") {
            callback(results)
        }

        return results
    }
}
