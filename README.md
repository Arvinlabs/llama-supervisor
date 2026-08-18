# llama-supervisor

Go 反向代理 + 空闲健康探测 + 异常重启。请求活跃期间不断重置计时，`restart` 与 `probe` 两组功能相互独立，各自通过 `enable` 开关启用或关闭（可都关闭，此时仅做反向代理）：

- `probe.enable: true`：空闲达到 `probe.interval` 秒后，下一个请求 proxy 前先探测后端 llama server，流式过程中末尾字符持续重复则判定异常，执行 `probe.command` 再 proxy
- `restart.enable: true`：空闲达到 `restart.interval` 秒后，下一个请求直接执行 `restart.command` 再 proxy

## 功能

- 反向代理：`host:port` → `backend`
- 收到第一个请求开始计时；每收到请求重置；两个相互独立的空闲计时器：
  - `probe`（`enable: true` 时启用）：空闲达到 `probe.interval` 后，下一个请求到来时在 proxy 之前先流式调用后端 `/v1/chat/completions`：
    - 流式过程中末尾字符持续重复（达到 `probe.repeatLimit`）即提前终止并判定 llama server 异常；探测失败同样判定异常
    - 异常则执行 `probe.command`，执行完成后再 proxy；正常则直接 proxy
    - probe 实际执行了 command 时（后端已重启），restart 的空闲计时随之重置，且同一请求内不重复执行 restart.command
  - `restart`（`enable: true` 时启用）：空闲达到 `restart.interval` 后，下一个请求到来时执行 `restart.command`，再 proxy
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
| `probe.repeatLimit` | 生成内容末尾同一字符连续出现达到该长度即判定异常，默认 `20` |
| `probe.timeout` | 探测超时(秒)，默认 `5` |

## 配置示例

```yaml
host: "0.0.0.0"
port: 8080
backend: "http://127.0.0.1:8081"
startupCommand: "sudo /usr/bin/supervisorctl start llama"

restart:
  enable: true
  interval: 20
  command: "sudo /usr/bin/supervisorctl stop llama && sleep 3 && sudo /usr/bin/supervisorctl start llama"

probe:
  enable: true
  interval: 60
  command: "sudo /usr/bin/supervisorctl stop llama && sleep 3 && sudo /usr/bin/supervisorctl start llama"
  apiKey: ""
  model: "default"
  prompt: "hi"
  maxTokens: 64
  repeatLimit: 20
  timeout: 5
```
