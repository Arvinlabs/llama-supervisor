# llama-supervisor

Go 反向代理 + 空闲健康探测 + 异常重启。请求活跃期间不断重置计时，`restart` 与 `probe` 两组功能相互独立，各自通过 `enable` 开关启用或关闭（可都关闭，此时仅做反向代理）：

- `probe.enable: true`：空闲达到 `probe.interval` 秒后，下一个请求 proxy 前先探测后端 llama server，流式过程中末尾字符持续重复则判定异常，执行 `probe.command` 再 proxy
- `restart.enable: true`：独立后台检查，首次请求后开始计时，请求不断则时间窗口延展，空闲达到 `restart.interval` 秒后直接执行 `restart.command`，无需等待请求；触发重启后暂停计时，无请求不计时，再次有请求才重新计时

## 功能

- 反向代理：`host:port` → `backend`
- 两个相互独立的空闲计时器（probe 从服务启动开始计时，restart 从首次请求开始计时），计时中每收到请求都延展时间窗口：
  - `probe`（`enable: true` 时启用）：空闲达到 `probe.interval` 后，下一个请求到来时在 proxy 之前先流式调用后端 `/v1/chat/completions`（探测使用服务器级 ctx，用户请求断开不影响探测结果）：
    - 流式过程中 `reasoning_content` 与 `content` 分别独立判定，任一末尾字符持续重复（达到 `probe.repeatLimit`）即提前终止并判定 llama server 异常；探测失败同样判定异常
    - 内容正常且累计生成字符达到 `probe.successLimit` 时立即判定健康、提前结束探测，不等待 `maxTokens` 生成完成
    - 异常则执行 `probe.command`，执行完成后再 proxy；正常则直接 proxy
  - `restart`（`enable: true` 时启用）：独立后台检查（每秒一次），计时从服务启动后的第一次请求开始（此前无请求不计时），每次请求都会延展空闲时间窗口，空闲达到 `restart.interval`（距最后一次请求）即执行 `restart.command`，无需等待请求；触发后暂停计时，再次有请求时重新开始计时，可周期性重复
- probe 判定异常执行 command 后，等待后端端口可连接（每 0.5s 探测一次，进程退出则立即返回）；若配置了 `waitBackendReady`，端口就绪后再等指定秒数才转发（restart 触发时不等待，直接执行命令）
- 可选（`startupCommand`）启动时同步执行一次命令
- Ctrl+C / SIGTERM 优雅退出

## 构建与运行

```bash
make build
./llama-supervisor -config config.yaml   # 默认 ./config.yaml
```

## 配置

```bash
cp config.yaml.example config.yaml
```

**基础配置**

| 字段 | 说明 |
|---|---|
| `host` / `port` | 监听地址 |
| `backend` | 后端地址，如 `http://127.0.0.1:8081` |
| `startupCommand` | 启动时同步执行的命令(shell) |
| `waitBackendReady` | probe 执行 command 后端口就绪后再等多少秒才转发，默认 `0`（秒） |
| `restart` | 重启配置对象，`enable: true` 时启用，见下表 |
| `probe` | 后端探测配置对象，`enable: true` 时启用，见下表 |

**restart（重启）**

| 字段 | 说明 |
|---|---|
| `restart.enable` | 是否启用，默认 `false` |
| `restart.interval` | 空闲多久(秒)触发，默认 `600` |
| `restart.command` | 超时后执行的命令(shell)，如重启 llama |

**probe（后端探测）**

| 字段 | 说明 |
|---|---|
| `probe.enable` | 是否启用，默认 `false` |
| `probe.interval` | 空闲多久(秒)触发，默认 `600` |
| `probe.command` | 探测判定异常后执行的命令(shell) |
| `probe.apiKey` | 探测 api key，仅探测时携带 `Bearer <key>`，正常代理不使用 |
| `probe.model` | 探测请求使用的 model，默认 `default` |
| `probe.prompt` | 探测提示词，默认 `hi` |
| `probe.maxTokens` | 探测最大生成 token 数，默认 `64` |
| `probe.repeatLimit` | 生成内容（含 `reasoning_content`）末尾同一字符连续出现达到该长度即判定异常，默认 `10` |
| `probe.successLimit` | 生成内容累计达到该字符数且无异常时提前判定健康、立即结束探测，无需等待生成完成，默认 `20`（设为负值禁用） |
| `probe.timeout` | 探测超时(秒)，默认 `5` |
