# MindBrain Graph SSE Example

Tiny browser client for the graph SSE stream exposed by `mindbrain-http`.

It connects to `GET /api/mindbrain/graph/subgraph`, listens for named SSE
events, and renders the graph incrementally as `seed_node`, `node`, `edge`, and
`done` events arrive.

## Run

1. Start the standalone HTTP server, for example:

```bash
MINDBRAIN_HTTP_ADDR=127.0.0.1:8091 mindbrain-http --db data/mindbrain.sqlite
```

The default bind should stay on loopback for local development. If you intentionally
need to serve other machines on a trusted network, set `MINDBRAIN_HTTP_ADDR` to a
non-loopback address such as `0.0.0.0:8091` and put the service behind your own
network controls or reverse proxy.

2. Serve this directory over HTTP, for example:

```bash
cd examples/javascript/graph
python3 -m http.server 8000
```

3. Open `http://localhost:8000` in a browser.

The page defaults to `http://localhost:8091/api/mindbrain/graph/subgraph`, but
you can change the stream URL in the UI if the server runs elsewhere.

## Query Parameters

- `seed_ids` - comma-separated entity IDs, required
- `hops` - maximum hop count, defaults to `2`
- `edge_types` - comma-separated edge types, optional

## Contract

This example expects SSE frames shaped like:

```text
event: node
data: {"depth":1,"entity":{"entity_id":123,"name":"Ada",...}}
```

The browser uses the SSE event name as the graph event kind, so the adapter can
stream the SQLite rows without extra client-side parsing.
