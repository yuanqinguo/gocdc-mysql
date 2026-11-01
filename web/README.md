# gocdc Admin Web

React 18 + TypeScript + Vite + Ant Design 5 operations UI. The SPA consumes only implemented `/api/v1` endpoints; metrics/graphs remain in Grafana.

## Development

```sh
npm ci
npm run dev
```

Vite serves on `http://127.0.0.1:5173` and proxies `/api` to `http://127.0.0.1:8060`. Override the target with `VITE_API_PROXY_TARGET` in a local `.env` file. Go service must be running with `observability.web.enabled: true`.

The login credential is kept in memory only. In an explicitly `auth_disabled` deployment, use the “continue without credentials” option.

## Checks and build

```sh
npm run typecheck
npm run lint
npm test
npm run build
```

`npm run build` writes the Vite assets to `../internal/api/dist`; the Go API embeds and serves those assets behind the same HTTP Basic auth middleware. Rebuild the frontend before building the Go binary (`make build`) or creating the Docker image. The checked-in `internal/api/dist/index.html` is only a compile-time fallback that tells developers how to build the SPA.

The small `go.mod` marks `web/` as a separate module boundary so Go commands from the repository root do not scan Go fixtures bundled inside `node_modules`.

The current API does not yet expose stock task details, nodes, per-rule runtime health/pause controls, overview metrics, or config writes. The UI intentionally does not fabricate those views.
