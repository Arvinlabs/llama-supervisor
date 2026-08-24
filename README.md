# llama-supervisor

Go reverse proxy with idle health probing, automatic restart, a speed watchdog, and a request policy. The idle timer keeps resetting while requests are active. `restart`, `probe`, and `watchdog` are three independent features, each toggled by its own `enable` switch; the `request` policy holds sub-features (like prefix cache), each toggled by its own switch (all can be disabled, in which case the process only reverse-proxies):

- `probe.enable: true`: after being idle for `probe.interval` seconds, the next request triggers a probe of the backend llama server before proxying. If the tail characters keep repeating during streaming, the backend is considered unhealthy, `probe.command` runs, and then the request is proxied.
- `restart.enable: true`: an independent background check. Timing starts after the first request; as long as requests keep coming the window extends. Once idle for `restart.interval` seconds, `restart.command` runs directly without waiting for a request. After a trigger, timing is paused: no requests means no timing, and a new request restarts the timer.
- `watchdog.enable: true`: an independent background sampler that polls the backend `/slots` every `watchdog.interval` seconds. If the generation speed stays above `watchdog.maxRate` t/s for `watchdog.times` consecutive samples, the backend is assumed to be stuck in an output loop and `watchdog.command` runs.
- `request.prefixCache: true`: normalize `/v1/chat/completions` request bodies before proxying to maximize the backend prefix cache hit rate — the tools list is sorted by tool name (`function.name` / `custom.name` per the OpenAI spec) and all JSON object keys (including tool parameter schemas) are re-emitted in sorted order, so semantically identical requests produce identical bytes.

## Features

- Reverse proxy: `host:port` → `backend`
- Two independent idle timers (probe starts counting from service startup, restart from the first request); every request during counting extends the idle window:
  - `probe` (enabled with `enable: true`): once idle for `probe.interval`, when the next request arrives a streaming call to the backend `/v1/chat/completions` is made before proxying (the probe uses the server-level ctx, so a user request disconnect does not affect the probe result):
    - during streaming, `reasoning_content` and `content` are checked independently; if the tail character of either keeps repeating (reaching `probe.repeatLimit`), the probe is aborted early and the llama server is declared unhealthy. A probe failure is also declared unhealthy.
    - if the content is normal and the cumulative generated characters reach `probe.successLimit`, the backend is declared healthy immediately and the probe ends early, without waiting for `maxTokens` to finish.
    - unhealthy: run `probe.command`, then proxy when it completes. Healthy: proxy directly.
  - `restart` (enabled with `enable: true`): an independent background check (once per second). Timing starts from the first request after service startup (no timing before that); every request extends the idle window. Once idle for `restart.interval` (measured from the last request), `restart.command` runs without waiting for a request. After a trigger timing is paused, resumes when a new request arrives, and can repeat periodically.
- `watchdog` (enabled with `enable: true`): an independent background sampler that polls the backend `/slots` (llama.cpp) every `watchdog.interval` (default 2s). If the generation speed within a sample interval exceeds `watchdog.maxRate` t/s (default 200) for `watchdog.times` consecutive samples (default 2), the backend is assumed to be stuck in an output loop (e.g. `//////`) and `watchdog.command` runs. The counter is reset when the speed drops back; after a trigger one sample is skipped before resuming.
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
| `apiKey` | global backend API key, sent as `Bearer <key>` on probe and `/slots` sampling requests (not used by normal proxying), default empty |
| `startupCommand` | shell command run synchronously at startup |
| `probe` | probe config object, enabled with `enable: true`, see below |
| `restart` | restart config object, enabled with `enable: true`, see below |
| `watchdog` | watchdog config object, enabled with `enable: true`, see below |
| `request` | request policy config object, see below |

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
| `watchdog.maxRate` | max generation speed (t/s); the average speed within a sample interval above this counts as one over-speed sample, default `200` |
| `watchdog.times` | consecutive over-speed samples required to declare unhealthy and run the command, default `2` |
| `watchdog.verbose` | whether to log the measured speed on normal windows (a request is active and the speed is normal), default `false` |
| `watchdog.command` | shell command run after declaring unhealthy, e.g. restarting llama |

### request

| Field | Description |
|---|---|
| `request.prefixCache` | prefix cache, default `false`. When enabled, `/v1/chat/completions` request bodies are normalized before proxying: the tools list is sorted by tool name (`function.name` / `custom.name` per the OpenAI spec) and every JSON object's keys are re-emitted in sorted order (including tool parameter schemas), so semantically identical requests produce identical bytes and the backend prefix cache hits |
