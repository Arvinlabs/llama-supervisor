# llama-supervisor

Go 反向代理 + 空闲健康探测 + 异常重启。请求活跃期间不断重置计时:

- 空闲达到 `probeInterval` 秒后,下一个请求 proxy 前先探测后端 llama server,流式过程中末尾字符持续重复则判定异常,执行 `command` 再 proxy
- 空闲达到 `interval` 秒后(更长),下一个请求直接执行 `command` 再 proxy

## 功能

- 反向代理:`host:port` → `backend`
- 收到第一个请求开始计时;每收到请求重置;两个独立空闲计时器:
  - `probeInterval`:超时后下一个请求到来时,在 proxy 之前先流式调用后端 `/v1/chat/completions`:
    - 流式过程中末尾字符持续重复(达到 `probeRepeatLimit`)即提前终止并判定 llama server 异常;探测失败同样判定异常
    - 异常则执行 `command`(如重启),执行完成后再 proxy;正常则直接 proxy
  - `interval`:超时后下一个请求到来时,不探测直接执行 `command`,再 proxy
- 可选(`startupCommand`)启动时同步执行一次 `command`,失败则启动失败
- 可选(`probeApiKey`)探测 api key:仅探测时携带,正常代理不使用
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

| 字段 | 说明 |
|---|---|
| `host` / `port` | 监听地址 |
| `backend` | 后端地址(host:port) |
| `interval` | command(重启)空闲计时间隔(秒) |
| `command` | 后端异常时执行的命令(shell) |
| `startupCommand` | 启动时是否同步执行一次 `command`,失败则启动失败退出,默认 `false` |

**后端探测(probe)相关参数**

| 字段 | 说明 |
|---|---|
| `probeInterval` | 探测空闲计时间隔(秒),缺省/0 时等同于 `interval` |
| `probeApiKey` | 探测 api key,仅探测时携带 `Bearer <key>`,正常代理不使用 |
| `probeModel` | 探测请求使用的 model,默认 `default` |
| `probePrompt` | 探测提示词,默认 `hi` |
| `probeMaxTokens` | 探测最大生成 token 数,默认 `64` |
| `probeRepeatLimit` | 生成内容末尾同一字符连续出现达到该长度即判定异常,默认 `20` |
| `probeTimeout` | 探测超时(秒),默认 `5` |
