# llama-supervisor

Go reverse proxy with idle health probing, automatic restart, a speed watchdog, and a request policy. The idle timer keeps resetting while requests are active. `restart`, `probe`, `watchdog`, `stats`, and `debug` are independent features, each toggled by its own `enable` switch; the `request` policy is toggled by `request.enable: true` and holds sub-features (virtual keys, prefix cache), each switched on its own within the group (all disabled means the process only reverse-proxies):

- `probe.enable: true`: after being idle for `probe.interval` seconds, the next request triggers a probe of the backend llama server before proxying. If the tail characters keep repeating during streaming, the backend is considered unhealthy, `probe.command` runs, and then the request is proxied.
- `restart.enable: true`: an independent background check. Timing starts after the first request; as long as requests keep coming the window extends. Once idle for `restart.interval` seconds, `restart.command` runs directly without waiting for a request. After a trigger, timing is paused: no requests means no timing, and a new request restarts the timer.
- `watchdog.enable: true`: an independent background sampler that polls the backend `/slots` every `watchdog.interval` seconds. If the generation speed stays above `watchdog.maxRate` t/s for `watchdog.times` consecutive samples, the backend is assumed to be stuck in an output loop and `watchdog.command` runs.
- `request.enable: true` + `request.virtualKeys: [ ... ]`: virtual API keys. When the group is enabled and the list is non-empty, clients must present one of the keys in the OpenAI format (`Authorization: Bearer <key>`, the raw header value and the llama.cpp-style `api_key` query parameter are accepted too); missing or unknown keys are rejected with an OpenAI-format 401 error and never proxied. Accepted requests are re-signed with the global `apiKey`, so the virtual keys never reach the backend.
- `request.prefixCache: true`: normalize `/v1/chat/completions` request bodies before proxying to maximize the backend prefix cache hit rate — the tools list is sorted by tool name (`function.name` / `custom.name` per the OpenAI spec) and all JSON object keys (including tool parameter schemas) are re-emitted in sorted order, so semantically identical requests produce identical bytes.
- `stats.enable: true` + `stats.savePath`: per-day token usage accounting for `/v1/chat/completions` only. One JSON file per day (`YYYY-MM-DD.json`) is saved to `stats.savePath` with the day's cumulative counters (`requests` / `input` / `input_cache` / `output` / `total`); files older than `stats.retainDays` days (default 7) are deleted. Streaming requests are transparently upgraded with `stream_options.include_usage` so the usage is always available.

## Features

- Reverse proxy: `host:port` → `backend`
- Two independent idle timers (probe starts counting from service startup, restart from the first request); every request during counting extends the idle window:
  - `probe` (enabled with `enable: true`): once idle for `probe.interval`, when the next request arrives a streaming call to the backend `/v1/chat/completions` is made before proxying (the probe uses the server-level ctx, so a user request disconnect does not affect the probe result):
    - during streaming, `reasoning_content` and `content` are checked independently; if the tail character of either keeps repeating (reaching `probe.repeatLimit`), the probe is aborted early and the llama server is declared unhealthy. A probe failure is also declared unhealthy.
    - if the content is normal and the cumulative generated characters reach `probe.successLimit`, the backend is declared healthy immediately and the probe ends early, without waiting for `maxTokens` to finish.
    - unhealthy: run `probe.command`, then proxy when it completes. Healthy: proxy directly.
  - `restart` (enabled with `enable: true`): an independent background check (once per second). Timing starts from the first request after service startup (no timing before that); every request extends the idle window. Once idle for `restart.interval` (measured from the last request), `restart.command` runs without waiting for a request. After a trigger timing is paused, resumes when a new request arrives, and can repeat periodically.
- `watchdog` (enabled with `enable: true`): an independent background sampler that polls the backend `/slots` (llama.cpp) every `watchdog.interval` (default 2s). If the generation speed within a sample interval exceeds `watchdog.maxRate` t/s (default 300) for `watchdog.times` consecutive samples (default 2, non-consecutive over-speed samples do not count), the backend is assumed to be stuck in an output loop (e.g. `//////`) and `watchdog.command` runs. The counter is reset when the speed drops back or a sample fails; after a trigger or a `/slots` fetch failure the watchdog fully pauses for `watchdog.pause` seconds (default 30) — no `/slots` fetching at all during the pause — and the first sample after the pause only rebuilds the baseline.
- After a probe declares unhealthy and the command runs, the backend `/health` is polled every 0.5s until it returns 2xx before forwarding (returns immediately if the probe process exits; a restart trigger does not wait and runs its command directly).
- Optional startup command (`startupCommand`) run synchronously once at boot.
- Graceful shutdown on Ctrl+C / SIGTERM.

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

### request (virtual keys)

When `request.enable` is true and `request.virtualKeys` is non-empty, all proxied requests must carry one of the configured virtual keys (OpenAI format `Authorization: Bearer <key>`); the supervisor re-signs the outbound request with the global `apiKey` (no `Authorization` header is sent if `apiKey` is empty), and rejects other requests with an OpenAI-format 401 error.

### probe

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

| Field | Description |
|---|---|
| `restart.enable` | whether enabled, default `false` |
| `restart.interval` | trigger after being idle this many seconds, default `600` |
| `restart.command` | shell command run on idle timeout, e.g. restarting llama |

### watchdog

| Field | Description |
|---|---|
| `watchdog.enable` | whether enabled, default `false` |
| `watchdog.interval` | `/slots` sampling interval in seconds, default `2` (frequent sampling to detect early) |
| `watchdog.maxRate` | max generation speed (t/s); the average speed within a sample interval above this counts as one over-speed sample, default `300` |
| `watchdog.times` | consecutive over-speed samples required to declare unhealthy and run the command (non-consecutive over-speed samples do not count), default `2` |
| `watchdog.pause` | seconds the watchdog fully pauses (no `/slots` fetching at all) after a trigger or a `/slots` fetch failure, default `30` |
| `watchdog.verbose` | whether to log the measured speed on normal windows (a request is active and the speed is normal), default `false` |
| `watchdog.command` | shell command run after declaring unhealthy, e.g. restarting llama |

### debug

| Field | Description |
|---|---|
| `debug.enable` | whether enabled, default `false` |
| `debug.path` | endpoint path, default `/debug/command`; GET/POST runs the command synchronously and returns the result |
| `debug.command` | shell command run on request |
| `debug.savePath` | when debug is enabled, every inbound proxied request is dumped to this directory as a plain text file named by the request time (`YYYYMMDD_HHMMSS.mmm.txt`, full request line + headers + body, generated from exactly what the client sent; JSON bodies are stored pretty-printed, non-JSON bodies base64-encoded so the dump stays plain text); default empty (saving disabled) |
| `debug.outSavePath` | when debug is enabled, every outbound proxied request is dumped to this directory after the request policy has rewritten it (when `request.enable` is true), using the same plain text format as `debug.savePath`; default empty (saving disabled) |

### request

| Field | Description |
|---|---|
| `request.enable` | whether the request policy is enabled, default `false`; sub-features only take effect when it is true |
| `request.virtualKeys` | virtual API key list, default empty (sub-feature off). When non-empty, proxied requests must carry one of the keys in the OpenAI format (`Authorization: Bearer <key>`; the raw header value and the `api_key` query parameter are also accepted); missing or unknown keys are rejected with an OpenAI-format 401 error. Accepted requests are re-signed with the global `apiKey`, which never sees the virtual keys |
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

| Field | Description |
|---|---|
| `stats.enable` | whether the stats policy is enabled, default `false` |
| `stats.savePath` | directory the per-day stats JSON files are saved to; empty disables the policy even when `enable` is true |
| `stats.retainDays` | how many days of stats files are kept (older ones are deleted), default `7` |
