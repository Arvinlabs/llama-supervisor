# llama-supervisor

Go 反向代理 + 空闲命令执行器。请求活跃期间不断重置计时,空闲达到 `interval` 秒后执行一次 `command`。

## 功能

- 反向代理:`host:port` → `backend`
- 收到第一个请求开始计时;每收到请求重置;空闲 `interval` 秒后执行 `command`
- 执行 `command` 后停止计时,新请求到来才重新计时
- 可选(`startupCommand`)启动时同步执行一次 `command`,失败则启动失败
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
| `interval` | 空闲计时间隔(秒) |
| `command` | 空闲到期后执行的命令(shell) |
| `startupCommand` | 启动时是否同步执行一次 `command`,失败则启动失败退出,默认 `false` |
