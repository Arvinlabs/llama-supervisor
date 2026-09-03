# llama-supervisor

Go reverse proxy that sits in front of a local llama.cpp server (`host:port` → `backend`) and turns it into an always-on, self-healing OpenAI-compatible service. A local model is one big process: it can hang, degenerate into a repeating output loop, or need a restart after being idle. The supervisor watches for that and takes care of it, while also acting as a small gateway layer for whoever calls the model. Use it for:

- **keeping the backend alive**: `probe` re-checks the backend after an idle window (and waits for it to be healthy before the next request goes through), `restart` brings it back after prolonged idleness, and `watchdog` catches an output loop (speed far above normal) — each runs a configured command (e.g. a `supervisorctl` restart) when its condition is met.
- **serving the model behind a clean API**: `request.virtualKeys` gives you OpenAI-style API key authentication (the real backend key never reaches clients), and `request.prefixCache` normalizes chat completion requests so the backend prompt cache hits more often (faster, cheaper first tokens).
- **knowing what the model costs**: `stats` records per-day token usage (input / input cache / output / total) of `/v1/chat/completions` into one JSON file per day.
- **seeing and poking what is going on**: `debug` provides a manual command endpoint and dumps inbound/outbound requests to disk.

## Build & Run

```bash
make build
./dist/llama-supervisor -config config.yaml   # default: ./config.yaml
./dist/llama-supervisor --version             # version, build time, go version, platform
```

## Release

Builds embed the git tag as the version (via `-ldflags`), also logged at startup. Cross-compilation targets, release archives and the GitHub Actions workflows (CI on push/PR, auto release on `vX.Y.Z` tags) are described in `BUILD.md`.

- releases: <https://github.com/Arvinlabs/llama-supervisor/releases>
- interactive release script: `./scripts/release.sh` (bumps a version tag, pushes, Actions publishes)
- version info: `./scripts/version.sh`

## Configuration

```bash
cp config.yaml.example config.yaml
```

### basic

| Field | Description |
|---|---|
| `host` / `port` | listen address |
| `backend` | backend address, e.g. `http://127.0.0.1:8081` |
| `apiKey` | global backend API key, sent as `Bearer <key>` on probe and `/slots` sampling requests; when `request.virtualKeys` is enabled it also re-signs accepted proxied requests, default empty |
| `startupCommand` | shell command run synchronously at startup |
| `probe` | probe config object, enabled with `enable: true`, see below |
| `restart` | restart config object, enabled with `enable: true`, see below |
| `watchdog` | watchdog config object, enabled with `enable: true`, see below |
| `request` | request policy config object, see below |
| `stats` | stats policy config object, see below |
| `debug` | debug config object, see below |

### probe

Idle health probe. Timing starts at service startup and every request extends the idle window; once idle for `probe.interval` seconds, the next request first triggers a probe (a streaming `/v1/chat/completions` call to the backend, using the server-level ctx so a user disconnect does not affect it) before proxying:

- during streaming, `reasoning_content` and `content` are checked independently; if the tail character of either repeats `probe.repeatLimit` times, the probe is aborted early and the backend is declared unhealthy (a probe failure is also unhealthy).
- if the content is normal and reaches `probe.successLimit` cumulative characters, the backend is declared healthy early, without waiting for generation to finish.
- unhealthy: run `probe.command`, then poll the backend `/health` every 0.5s until it returns 2xx before forwarding. Healthy: proxy directly.

| Field | Description |
|---|---|
| `probe.enable` | whether enabled, default `false` |
| `probe.interval` | trigger after being idle this many seconds, default `600` |
| `probe.command` | shell command run after the probe declares unhealthy |
| `probe.model` | model used by the probe request, default `default` |
| `probe.prompt` | probe prompt, default `hi` |
| `probe.maxTokens` | max generated tokens for the probe, default `64` |
| `probe.repeatLimit` | same tail character (including `reasoning_content`) repeated this many times in a row is declared unhealthy, default `10` |
| `probe.successLimit` | once normal content reaches this many cumulative characters, the backend is declared healthy early and the probe ends without waiting for generation to finish, default `20` (negative disables) |
| `probe.timeout` | probe timeout in seconds, default `5` |

### restart

Idle restart. An independent background check (once per second): timing starts after the first request, every request extends the idle window, and once idle for `restart.interval` seconds the command runs directly, without waiting for a request. After a trigger timing pauses (no requests means no timing), a new request resumes it, and it can repeat periodically.

| Field | Description |
|---|---|
| `restart.enable` | whether enabled, default `false` |
| `restart.interval` | trigger after being idle this many seconds, default `600` |
| `restart.command` | shell command run on idle timeout, e.g. restarting llama |

### watchdog

Speed watchdog. An independent background sampler polls the backend `/slots` every `watchdog.interval` seconds; if the average generation speed within a sample interval exceeds `watchdog.maxRate` t/s for `watchdog.times` consecutive samples (non-consecutive over-speed samples do not count), the backend is assumed to be stuck in an output loop (e.g. `//////`) and `watchdog.command` runs. The counter resets when the speed drops back or a sample fails; after a trigger or a `/slots` fetch failure the watchdog fully pauses for `watchdog.pause` seconds (no `/slots` fetching at all during the pause), and the first sample after the pause only rebuilds the baseline.

| Field | Description |
|---|---|
| `watchdog.enable` | whether enabled, default `false` |
| `watchdog.interval` | `/slots` sampling interval in seconds, default `2` (frequent sampling to detect early) |
| `watchdog.maxRate` | max generation speed (t/s); the average speed within a sample interval above this counts as one over-speed sample, default `300` |
| `watchdog.times` | consecutive over-speed samples required to declare unhealthy and run the command (non-consecutive over-speed samples do not count), default `2` |
| `watchdog.pause` | seconds the watchdog fully pauses (no `/slots` fetching at all) after a trigger or a `/slots` fetch failure, default `30` |
| `watchdog.verbose` | whether to log the measured speed on normal windows (a request is active and the speed is normal), default `false` |
| `watchdog.command` | shell command run after declaring unhealthy, e.g. restarting llama |

### request

Request policy with two independent sub-features (each only takes effect when `request.enable` is true):

- `virtualKeys`: when the list is non-empty, clients must present one of the keys in the OpenAI format (`Authorization: Bearer <key>`; the raw header value and the llama.cpp-style `api_key` query parameter are accepted too); missing or unknown keys are rejected with an OpenAI-format 401 error and never proxied. Accepted requests are re-signed with the global `apiKey` (no `Authorization` header is sent if `apiKey` is empty), so the virtual keys never reach the backend.
- `prefixCache`: normalizes `/v1/chat/completions` request bodies so that semantically identical requests produce identical bytes, maximizing the backend prefix cache hit rate.

| Field | Description |
|---|---|
| `request.enable` | whether the request policy is enabled, default `false`; sub-features only take effect when it is true |
| `request.virtualKeys` | virtual API key list, default empty (sub-feature off). When non-empty, proxied requests must carry one of the keys; missing or unknown keys are rejected with an OpenAI-format 401 error. Accepted requests are re-signed with the global `apiKey`, which never sees the virtual keys |
| `request.prefixCache` | prefix cache, default `false`. When enabled, `/v1/chat/completions` request bodies are normalized before proxying: only the top-level `tools` list is touched — sorted by tool name (`function.name` / `custom.name` per the OpenAI spec, canonical-byte tiebreak), and every element is re-encoded in canonical form (object keys sorted at all levels, numbers in shortest form `1.0` -> `1`, no HTML escaping), so semantically identical tools lists in any key order / number literal / whitespace yield one canonical array; every byte outside the tools array is passed through exactly as the client sent it. A tools array with invalid UTF-8 is passed through unchanged |

### stats

Per-day token usage accounting, only for `/v1/chat/completions` (all other endpoints are ignored). One JSON file per day (`YYYY-MM-DD.json`) is saved to `stats.savePath`, holding the day's cumulative counters:

```json
{
  "date": "2026-05-03",
  "requests": 2,
  "input": 37,
  "input_cache": 23,
  "output": 245,
  "total": 282
}
```

The usage comes from the backend response: non-stream responses always carry it; for streams the supervisor force-injects `stream_options.include_usage` into the outbound request (a standard, client-visible extra usage chunk at the end of the stream), so the final usage chunk is always present. The response bytes pass through to the client unmodified and unbuffered. If a stream ends abnormally before the usage chunk, that request is simply not counted. Day files older than `stats.retainDays` days are deleted at startup and at most once a day (by the first recorded request after a date change).

The supervisor also serves an embedded web page at `/stats` (its JSON data at `/stats/data`): a per-day vertical stacked bar chart (one bar per day, height = total, split into input cache / input / output) above a per-day table with tokens in `k` units (e.g. `1.32k`) and the cache hit rate (`input_cache` / `input`, 2 decimal places), plus an all-days total row. The endpoints are served by the supervisor itself and never proxied.

| Field | Description |
|---|---|
| `stats.enable` | whether the stats policy is enabled, default `false` |
| `stats.savePath` | directory the per-day stats JSON files are saved to; empty disables the policy even when `enable` is true |
| `stats.retainDays` | how many days of stats files are kept (older ones are deleted), default `7` |

### debug

Manual trigger and request dump. With `debug.enable` true, requesting `debug.path` (GET/POST) runs `debug.command` synchronously and returns the result (no authentication — keep it reachable only from trusted networks). The save paths dump proxied requests as plain text files named by the request time (`YYYYMMDD_HHMMSS.mmm.txt`, full request line + headers + body; JSON bodies stored pretty-printed, non-JSON bodies base64-encoded so the dump stays plain text).

| Field | Description |
|---|---|
| `debug.enable` | whether enabled, default `false` |
| `debug.path` | endpoint path, default `/debug/command`; GET/POST runs the command synchronously and returns the result |
| `debug.command` | shell command run on request |
| `debug.savePath` | when debug is enabled, every inbound proxied request is dumped to this directory, generated from exactly what the client sent; default empty (saving disabled) |
| `debug.outSavePath` | when debug is enabled, every outbound proxied request is dumped to this directory after the request policy has rewritten it (when `request.enable` is true), using the same plain text format as `debug.savePath`; default empty (saving disabled) |
