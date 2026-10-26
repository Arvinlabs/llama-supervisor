# llama-supervisor

Go reverse proxy that sits in front of a local llama.cpp server (`host:port` → `backend`) and turns it into an always-on, self-healing OpenAI-compatible service. A local model is one big process: it can hang, degenerate into a repeating output loop, or need a restart after being idle. The supervisor watches for that and takes care of it, while also acting as a small gateway layer for whoever calls the model. Use it for:

- **keeping the backend alive**: `probe` re-checks the backend after an idle window (and waits for it to be healthy before the next request goes through), `restart` brings it back after prolonged idleness, `watchdog` catches an output loop (speed far above normal — judged against the live generated content of streaming responses before acting — or the live content alone falling into a long repeated run) or a degenerate MTP draft acceptance rate, and `errguard` catches a backend fault that surfaces as an OpenAI-style error payload (a stream `data: {"error":{...}}` event or a non-stream `{"error":{...}}` body) whose message matches a configured pattern — each runs a configured command (e.g. a `supervisorctl` restart) when its condition is met.
- **serving the model behind a clean API**: `request.virtualKeys` gives you OpenAI-style API key authentication (the real backend key never reaches clients), `request.prefixCache` normalizes chat completion requests so the backend prompt cache hits more often (faster, cheaper first tokens), and `anthropic` exposes the same backend through the Anthropic Messages API (`/v1/messages`) for clients like Claude Code. `cloud` routes any chat completion that does not fit the local model's context window to a configured OpenAI-compatible cloud endpoint (e.g. DeepSeek), so oversized requests never truncate locally.
- **knowing what the model costs**: `stats` records per-day token usage (input / input cache / output / total) of `/v1/chat/completions` into one JSON file per day.
- **seeing and poking what is going on**: `debug` provides a manual command endpoint and dumps inbound/outbound requests to disk.

Two implementation notes that hold for every backend:

- the client's `Accept-Encoding` header is **not forwarded** to either backend. Go's transport then negotiates the encoding itself and transparently decompresses the response, so the completion tap (stats' usage counters, the watchdog's `timings` and generated content) always sees plain JSON/SSE: a compressed body is unparsable, which would silently drop the counters for clients that advertise gzip (curl and most SDKs do). The client receives a valid, uncompressed response.
- the response body is passed through byte-identical and unbuffered; a `Content-Encoding` the tap cannot decode is reported (`completion not accounted: ...`) instead of being accounted with invented numbers.

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
| `apiKey` | global backend API key, sent as `Bearer <key>` on probe, `/slots` sampling, cloud context-window detection and `/tokenize` estimation requests; when `request.virtualKeys` is enabled it also re-signs accepted proxied requests, default empty. On a key-protected backend the `cloud` sub-features (n_ctx detection, exact token counting) need it |
| `startupCommand` | shell command run synchronously at startup |
| `probe` | probe config object, enabled with `enable: true`, see below |
| `restart` | restart config object, enabled with `enable: true`, see below |
| `watchdog` | watchdog config object, enabled with `enable: true`, see below |
| `errguard` | backend error guard config object, enabled with `enable: true`, see below |
| `request` | request policy config object, see below |
| `cloud` | cloud fallback config object, enabled with `enable: true`, see below |
| `stats` | stats policy config object, see below |
| `debug` | debug config object, see below |

### probe

Idle health probe. Timing starts at service startup and every request extends the idle window; once idle for `probe.interval` seconds, the next request first triggers a probe (a streaming `/v1/chat/completions` call to the backend, using the server-level ctx so a user disconnect does not affect it) before proxying:

- during streaming, `reasoning_content` and `content` are checked independently; if the tail character of either repeats `probe.repeatLimit` times (only characters listed in `probe.repeatChars` count, any character when it is empty), the probe is aborted early and the backend is declared unhealthy (a probe failure is also unhealthy).
- if the content is normal and reaches `probe.successLimit` cumulative characters, the backend is declared healthy early, without waiting for generation to finish.
- unhealthy: run `probe.command`, then poll the backend `/health` every 0.5s until it returns 2xx before forwarding. Healthy: proxy directly. Probing happens on the first **local** request after the idle window (a cloud-routed request does not probe and does not wait for the backend).

| Field | Description |
|---|---|
| `probe.enable` | whether enabled, default `false` |
| `probe.interval` | trigger after being idle this many seconds, default `600` |
| `probe.command` | shell command run after the probe declares unhealthy |
| `probe.model` | model used by the probe request, default `default` |
| `probe.prompt` | probe prompt, default `hi` |
| `probe.maxTokens` | max generated tokens for the probe, default `64` |
| `probe.repeatLimit` | same tail character (including `reasoning_content`) repeated this many times in a row is declared unhealthy (only characters in `probe.repeatChars` count), default `10` |
| `probe.repeatChars` | whitelist of characters whose consecutive repetition counts as degenerate; a character not listed also breaks a run of one that is; empty (default) means any character |
| `probe.successLimit` | once normal content reaches this many cumulative characters, the backend is declared healthy early and the probe ends without waiting for generation to finish, default `20` (negative disables) |
| `probe.timeout` | probe timeout in seconds, default `5` |

### restart

Idle restart. An independent background check (once per second): timing starts after the first request, every request extends the idle window, and once idle for `restart.interval` seconds the command runs directly, without waiting for a request. After a trigger timing pauses (no requests means no timing), a new request resumes it, and it can repeat periodically.

Only a request that is actually **served by the local backend** counts as activity: a cloud-routed request (see the `cloud` section) leaves the local model alone, so it neither extends the idle window nor triggers/starts timing. With cloud-only traffic the local component therefore stays "idle" as far as the restart (and probe) policies are concerned.

| Field | Description |
|---|---|
| `restart.enable` | whether enabled, default `false` |
| `restart.interval` | trigger after being idle this many seconds, default `600` |
| `restart.command` | shell command run on idle timeout, e.g. restarting llama |

### watchdog

Speed watchdog. Sampling is request-driven: while at least one `/v1/chat/completions` request is in flight, the backend `/slots` is polled — the first fetch happens a fixed 1 second after the request arrives, then every `watchdog.interval` seconds until the last request ends (no fetching at all when idle). If the average generation speed within a sample interval exceeds `watchdog.maxRate` t/s for `watchdog.times` consecutive samples (non-consecutive over-speed samples do not count), the backend is assumed to be stuck in an output loop (e.g. `//////`) and `watchdog.command` runs. The counter resets when the speed drops back or a sample fails; after a trigger or a `/slots` fetch failure the watchdog fully pauses for `watchdog.pause` seconds (no `/slots` fetching at all during the pause), and the first sample after the pause only rebuilds the baseline.

When the over-speed streak is reached, the in-flight completions the supervisor is proxying are judged before triggering:

- a **non-streaming** completion has no content to judge yet — it triggers directly;
- a **streaming** completion is judged by its generated content (the supervisor taps `content` and `reasoning_content` from the stream live): it triggers only when the generated tail has degenerated into a dead loop — the same rune repeated `watchdog.repeatLimit` times in a row (e.g. `////`; only runes listed in `watchdog.repeatChars` count, any rune when it is empty); as long as all in-flight streams look healthy the trigger is held and re-judged on each further over-speed sample while the streak is still running. The degenerate mark is not sticky forever: `watchdog.repeatLimit` consecutive runes that do not extend the run clear it, so an early legitimate run (a markdown divider, `......`) cannot poison a later, unrelated over-speed judgment (a short jitter, a loop broken by a single rune, does not clear it).

Independent content-loop trigger. A streaming completion whose generated tail keeps the same rune `watchdog.hardRepeat` times in a row (only runes listed in `watchdog.repeatChars` count) triggers directly, **without any over-speed** — a model can also degenerate into a loop that decodes at a normal speed that never crosses `watchdog.maxRate`, which the speed check alone would never catch. The trigger fires once per run (at the crossing), and not while the watchdog is in its pause window.
- an over-speed with no in-flight completion visible to the tap (e.g. the request did not go through this proxy) triggers directly.

The watchdog only judges the **local** model: cloud-routed completions (see the `cloud` section) are completely invisible to it — they never start the `/slots` sampling, never enter the in-flight judgement, and never reach the draft-acceptance check.

MTP draft-acceptance watchdog. The supervisor taps every local `/v1/chat/completions` response (streaming and non-streaming) and reads the backend's `timings`, which report `draft_n` (speculative/MTP draft tokens attempted) and `draft_n_accepted` (draft tokens accepted); the acceptance ratio is `draft_n_accepted / draft_n`. When `watchdog.minDraftRate` is set (> 0) and the ratio stays below it for `watchdog.draftTimes` consecutive completions (completions without draft data are ignored, so a backend without speculative decoding never trips this), the backend is declared unhealthy and `watchdog.command` runs, and the watchdog fully pauses for `watchdog.pause` seconds. This check shares its pause window with the speed check, and enabling it is what turns the completion tap (and the `stream_options.include_usage` request injection that makes the `timings` present on streams) active. `watchdog.minDraftRate: 0` (the default) turns it off.

| Field | Description |
|---|---|
| `watchdog.enable` | whether enabled, default `false` |
| `watchdog.interval` | `/slots` sampling interval in seconds while a chat completion request is in flight (the first fetch happens a fixed 1 second after the request arrives), default `2` (frequent sampling to detect early) |
| `watchdog.maxRate` | max generation speed (t/s); the average speed within a sample interval above this counts as one over-speed sample, default `300` |
| `watchdog.times` | consecutive over-speed samples required to declare unhealthy and run the command (non-consecutive over-speed samples do not count), default `2` |
| `watchdog.minDraftRate` | min MTP draft acceptance ratio (`timings.draft_n_accepted / timings.draft_n`) sampled from each observed completion; a completion below it for `watchdog.draftTimes` in a row is declared unhealthy and runs the command, default `0` (off) |
| `watchdog.draftTimes` | consecutive low draft-acceptance completions required to declare unhealthy, default `10` |
| `watchdog.repeatLimit` | consecutive identical tail runes in a streaming completion's generated content that mark it a dead loop: when the over-speed streak is reached, a streaming in-flight completion triggers only when its content is degenerate (a non-streaming one triggers directly; only runes in `watchdog.repeatChars` count), default `10` |
| `watchdog.repeatChars` | whitelist of characters whose consecutive repetition counts as a dead loop; a character not listed also breaks a run of one that is; empty (default) means any character |
| `watchdog.hardRepeat` | independent trigger: consecutive identical tail runes in a streaming completion's generated content that declare unhealthy and run the command without any over-speed (only runes in `watchdog.repeatChars` count), default `100` (a value <= 0 disables) |
| `watchdog.pause` | seconds the watchdog fully pauses (no `/slots` fetching at all) after a trigger or a `/slots` fetch failure, default `30` |
| `watchdog.verbose` | whether to log the measured speed on normal windows (a request is active and the speed is normal), default `false` |
| `watchdog.command` | shell command run after declaring unhealthy, e.g. restarting llama |

### errguard

Backend error guard. A backend fault does not always show up as a hang or an output loop (what `probe` / `restart` / `watchdog` catch): llama.cpp can also answer a request with an OpenAI-style error object, either as a stream event

```
data: {"error":{"type":"server_error","message":"..."}}
```

or as a plain non-stream JSON body

```
{"error":{"type":"server_error","message":"..."}}
```

The supervisor taps every proxied `/v1/chat/completions` response (streaming and non-streaming, whatever the HTTP status), extracts such an error object, and when its `message` matches any regular expression in `errguard.patterns` (Go regexp; an empty list matches nothing, so the guard never triggers) it runs `errguard.command` — so a fault that surfaces as an error payload is self-healed the same way. The response bytes still reach the client verbatim; the guard only observes them.

- The match is on the error `message` (the `type` and `code` are logged but not matched). A pattern is compiled at startup; an invalid regexp fails startup rather than silently never matching.
- `errguard.cooldown` is a suppression window after a trigger: while it is active a further matching error is logged but the command is not re-run, so a burst of matching errors (many concurrent requests hitting the same backend fault) runs the command once. A value `<= 0` disables it.
- The guard reuses the completion tap, but on its own it does **not** turn on the `stream_options.include_usage` request injection (that stays tied to `stats` / `watchdog`), so enabling only `errguard` leaves the outbound stream unchanged.
- The supervisor's own injected stream-interruption event (an abnormal stream end from a killed backend / reset connection, see the stream guard) carries the same error shape, but the guard **ignores it** (it is tagged with a distinct `code`): an abnormal stream end is not a backend error message, so reacting to it would let the guard re-trigger on the very restart it ran (a dead connection is the `watchdog` / `restart` concern).

| Field | Description |
|---|---|
| `errguard.enable` | whether enabled, default `false` |
| `errguard.patterns` | list of regular expressions (Go regexp) matched against the backend error message; an empty list matches nothing (the guard never triggers), default empty |
| `errguard.command` | shell command run when an error message matches, e.g. restarting llama; empty logs the match only |
| `errguard.cooldown` | seconds during which the command is not re-run after a trigger (a burst of matching errors triggers once), default `30` (a value `<= 0` disables) |

### request

Request policy with two independent sub-features (each only takes effect when `request.enable` is true):

- `virtualKeys`: when the list is non-empty, clients must present one of the keys in the OpenAI format (`Authorization: Bearer <key>`; the raw header value and the llama.cpp-style `api_key` query parameter are accepted too); missing or unknown keys are rejected with an OpenAI-format 401 error and never proxied. Accepted requests are re-signed with the global `apiKey` (no `Authorization` header is sent if `apiKey` is empty), so the virtual keys never reach the backend.
- `prefixCache`: normalizes `/v1/chat/completions` request bodies so that semantically identical requests produce identical bytes, maximizing the backend prefix cache hit rate.

| Field | Description |
|---|---|
| `request.enable` | whether the request policy is enabled, default `false`; sub-features only take effect when it is true |
| `request.virtualKeys` | virtual API key list, default empty (sub-feature off). When non-empty, proxied requests must carry one of the keys; missing or unknown keys are rejected with an OpenAI-format 401 error. Accepted requests are re-signed with the global `apiKey`, which never sees the virtual keys |
| `request.prefixCache` | prefix cache, default `false`. When enabled, `/v1/chat/completions` request bodies are normalized before proxying: only the top-level `tools` list is touched — sorted by tool name (`function.name` / `custom.name` per the OpenAI spec, canonical-byte tiebreak), and every element is re-encoded in canonical form (object keys sorted at all levels, numbers in shortest form `1.0` -> `1`, no HTML escaping), so semantically identical tools lists in any key order / number literal / whitespace yield one canonical array; every byte outside the tools array is passed through exactly as the client sent it. A tools array with invalid UTF-8 is passed through unchanged |

### cloud

Cloud fallback: when enabled, each `/v1/chat/completions` request is size-estimated and, when it does not fit the local model's context window, routed to a configured OpenAI-compatible cloud endpoint (e.g. `https://api.deepseek.com`) instead of the local backend. Everything else — smaller requests, every non-chat path (`/completion`, `/embeddings`, `/slots`, `/health`, ...), and requests that cannot be judged (undecodable body, unknown context window) — stays local. The client sees no protocol difference: plain JSON and SSE streams are passed through unchanged from either backend.

How it works:

- The **local context window** (n_ctx) is auto-detected from the backend: `GET /props` (`n_ctx` field), falling back to parsing the `n_ctx=<n>` suffix of the model ids from `GET /v1/models`. The value is cached for 5 minutes (the supervisor itself restarts the backend, so the window may change with its startup parameters); a failing probe keeps serving the stale value, and a never-successful detection disables the routing (requests stay local). `cloud.localMaxTokens` overrides detection entirely.
- The **request size** is the local backend's own count: the chat completion body's text (message texts including multimodal text parts, tool calls and tools definitions) is sent to the backend's `POST /tokenize`, whose token list is the estimate, plus image parts (a fixed token allowance each), plus a small per-message budget for chat template markers, plus a reserved output of `max(max_tokens, cloud.outputReserve)`. There is **no client-side fallback**: when the backend cannot count the text (no `/tokenize`, a failing or timed-out call, more than 8 MiB of text) the estimation fails, the error is logged and the request stays local.
- A body larger than **32 MiB** is never judged (it already far exceeds any local context): it stays local and is forwarded **byte-identical** — the router buffers only a prefix and replays it followed by the rest of the client's body.
- The request goes to the **cloud** when `estimate > n_ctx * cloud.threshold` (default 0.9). The cloud request runs the same outbound pipeline as the local one (request policy, usage injection, response observation) plus: the `Authorization` header is replaced with `cloud.apiKey` (client keys never leave), `cloud.model` rewrites the model name, and llama.cpp-specific sampling fields (`mirostat*`, `top_k`, `repeat_penalty`, `cache_prompt`, ...) are stripped. Standard OpenAI fields (`logit_bias`, `seed`, ...) are kept.
- A **transport-level cloud failure** (connect/timeout/reset before any response) replies a 502 with an OpenAI-format error object (`{"error":{"message":"cloud unavailable","code":"cloud_unavailable"}}`), so OpenAI SDK clients parse it. A failed cloud request is **never retried** (neither on the cloud nor on the local backend, which would truncate the oversized prompt anyway): every response the cloud actually sent — status included — is passed through to the client unchanged. The cloud transport carries connection-level timeouts (dial 10s, TLS 10s, response headers 2min) so a black-holed endpoint fails instead of hanging the request, and `cloud.maxConcurrency` optionally caps the in-flight cloud requests. The access log tags every request with the backend it went to (`local` / `cloud`).
- A cloud request is **not local backend activity**: the restart / probe idle timers are only refreshed (and the probe only runs) for requests that are served by the local backend (see the `restart` / `probe` sections). It still needs the backend's `/props` + `/tokenize` to make the routing decision, so while the backend is down every request stays local until it is reachable again.
- The **watchdog never sees cloud traffic**: cloud completions are not observed by it (no draft-acceptance judgment, no in-flight over-speed/degenerate-output tracking, no `/slots` sampling — that endpoint is local anyway). Only `stats` opts in to cloud completions, accounting them in separate cloud counters. The local `errguard` is likewise never triggered by a cloud error payload.

On a key-protected backend the global `apiKey` must be set: the supervisor's own `/props` / `/v1/models` / `/tokenize` calls carry it (client keys are not used for them). Without it the context-window detection falls back to `localMaxTokens` and `/tokenize` is rejected (401), so no request is ever judged and everything stays local.

Because the Anthropic handler (`/v1/messages`) runs through the same internal pipeline, it inherits the cloud routing automatically.

| Field | Description |
|---|---|
| `cloud.enable` | whether the cloud fallback is enabled, default `false`; also requires non-empty `baseUrl` and `apiKey` |
| `cloud.baseUrl` | cloud endpoint base URL, e.g. `https://api.deepseek.com` (no trailing slash) |
| `cloud.apiKey` | cloud API key, sent to the cloud as `Bearer <key>`; client keys (virtual or real) never reach the cloud |
| `cloud.model` | model name rewritten into the request when it is routed to the cloud, default empty (the client's model name passes through) |
| `cloud.localMaxTokens` | local context window override in tokens, default `0` (auto-detect from the backend) |
| `cloud.threshold` | routing threshold: the request goes to the cloud when its estimated token size exceeds `n_ctx * threshold`, default `0.9` |
| `cloud.outputReserve` | output tokens reserved for the decision when the client did not set `max_tokens` (the decision uses `max(max_tokens, this)`), default `1024` |
| `cloud.maxConcurrency` | max in-flight cloud requests, default `0` (unlimited); further requests wait for a slot or for their client to disconnect |

### anthropic

Anthropic Messages API support: when enabled, the supervisor accepts `POST /v1/messages` on top of the OpenAI pipeline. The request is translated to the OpenAI chat completion protocol at the outer edge (via the [relaykit](https://github.com/QuantumNous/new-api/tree/main/relaykit) conversion library) and proxied to the supervisor **itself**, so the loopback request runs the full, unmodified OpenAI pipeline — auth, request policy, usage injection, stats observation, watchdog, debug dumps. The OpenAI response (plain JSON or SSE) is translated back to the Anthropic protocol before it reaches the client: non-streaming responses become a single `message` object, streams become the `message_start` / `content_block_*` / `message_delta` / `message_stop` SSE event sequence.

| Field | Description |
|---|---|
| `anthropic.enable` | whether the Anthropic protocol is enabled, default `false` |
| `anthropic.modelMap` | optional client model name → backend model name remapping, default empty; unmapped names pass through to the backend unchanged |

Notes:

- The handler does no auth of its own: the client's key (`x-api-key` header, or the `Authorization` header verbatim) is passed through to the self request, so the internal pipeline's request policy (`request.virtualKeys`) is the single authentication point for both protocols; upstream failures are mapped to Anthropic error shapes (`{"type":"error","error":{...}}`), 5xx → 500.
- The pipeline's injected stream error event (abnormal stream end) is relayed to the client as an Anthropic `event: error`, and a stream that ends without a terminal chunk is closed with a synthesized `message_delta` + `message_stop` so clients never hang.
- Tools, tool results, images (inline base64 becomes a data URL, URL sources pass through) and usage (input / cache / output tokens) are carried across; protocol-unique fields that cannot be expressed on the other side are dropped with a logged diagnostic.

### stats

Per-day token usage accounting, only for `/v1/chat/completions` (all other endpoints are ignored). One JSON file per day (`YYYY-MM-DD.json`) is saved to `stats.savePath`, holding the day's cumulative counters and the subset of them that was routed to the cloud (`cloud`, see the `cloud` section):

```json
{
  "date": "2026-05-03",
  "requests": 3,
  "input": 137,
  "input_cache": 63,
  "output": 295,
  "total": 432,
  "cloud": {
    "requests": 1,
    "input": 100,
    "input_cache": 40,
    "output": 50,
    "total": 150
  }
}
```

The usage comes from the backend response: non-stream responses always carry it; for streams the supervisor force-injects `stream_options.include_usage` into the outbound request (a standard, client-visible extra usage chunk at the end of the stream), so the final usage chunk is always present (cloud requests included, and only when stats is enabled — the watchdog alone does not observe cloud traffic). The tap parses the last SSE event even when the provider ends the stream without a trailing newline (or without `[DONE]`), so a usage chunk sent as the final, unterminated event is still counted; a body announced as JSON but carrying SSE data lines is scanned for the usage chunk too. A completion whose response carries no usage at all is logged as `not accounted` (with the response's `Content-Type` / `Content-Encoding` / stream flag) instead of being dropped silently. The prefix cache hits are read from `usage.prompt_tokens_details.cached_tokens` or, failing that, from the cloud-style `usage.prompt_cache_hit_tokens` (DeepSeek). The response bytes pass through to the client unmodified and unbuffered. If a stream ends abnormally before the usage chunk, that request is simply not counted. Day files older than `stats.retainDays` days are deleted at startup and at most once a day (by the first recorded request after a date change).

The supervisor also serves an embedded web page at `/stats` (its JSON data at `/stats/data`): a per-day vertical stacked bar chart (one bar per day, height = total, split into input cache / input / output) above a per-day table with tokens in `k` units (e.g. `1.32k`), the cloud requests / cloud tokens of the day, and the cache hit rate (`input_cache` / `input`, 2 decimal places), plus an all-days total row. The chart tooltips and the today card also show the cloud share of each bucket. The endpoints are served by the supervisor itself and never proxied.

| Field | Description |
|---|---|
| `stats.enable` | whether the stats policy is enabled, default `false` |
| `stats.savePath` | directory the per-day stats JSON files are saved to; empty disables the policy even when `enable` is true |
| `stats.retainDays` | how many days of stats files are kept (older ones are deleted), default `7` |

### debug

Manual trigger and request/response dump. With `debug.enable` true, requesting `debug.path` (GET/POST) runs `debug.command` synchronously and returns the result (no authentication — keep it reachable only from trusted networks). The request save paths dump proxied requests as plain text files named by the request time (`YYYYMMDD_HHMMSS.mmm.txt`, full request line + headers + body; JSON bodies stored pretty-printed, non-JSON bodies base64-encoded so the dump stays plain text). The response save path dumps backend responses as JSON files named by the response time (`YYYYMMDD_HHMMSS.mmm.json`, status line + headers + body): a non-streaming body is stored as the parsed JSON (base64-encoded when not valid JSON), and a streaming (SSE) chat completion is assembled into the same non-streaming structure — the per-chunk deltas (`role`/`content`/`reasoning_content`, and `tool_calls` with `name`/`arguments` fragments concatenated per call index) are merged into one `message`, the `finish_reason` comes from the terminating chunk, and `usage`/`timings` from the final usage chunk, so streaming and non-streaming responses dump in the same shape; a stream that is not a chat completion falls back to a JSON array of the data events (JSON payloads parsed, others kept as raw strings, e.g. `[DONE]`). The dump is always a single JSON document.

| Field | Description |
|---|---|
| `debug.enable` | whether enabled, default `false` |
| `debug.path` | endpoint path, default `/debug/command`; GET/POST runs the command synchronously and returns the result |
| `debug.command` | shell command run on request |
| `debug.savePath` | when debug is enabled, every inbound proxied request is dumped to this directory, generated from exactly what the client sent; default empty (saving disabled) |
| `debug.outSavePath` | when debug is enabled, every outbound proxied request is dumped to this directory after the request policy has rewritten it (when `request.enable` is true), using the same plain text format as `debug.savePath`; default empty (saving disabled) |
| `debug.respSavePath` | when debug is enabled, every backend response is dumped to this directory as a JSON file: non-streaming bodies stored as the parsed JSON (base64-encoded when not valid JSON), streaming (SSE) chat completions assembled into the non-streaming structure (deltas merged into one message, `usage`/`timings` from the final chunk); default empty (saving disabled) |
