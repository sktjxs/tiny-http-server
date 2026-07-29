# 版本演进 · tiny-http-server

| 版本 | 关键词 |
|------|--------|
| v0–v2 | socket 基础 + RAII 封装 → HTTP 协议与路由（阻塞）→ 为 epoll 打基础 |
| v3 | `epoll` + 线程池 + `EPOLLONESHOT`（单 Reactor 雏形） |
| v4 | keep-alive 长连接 |
| v5 | 连接超时 + 动态 `epoll_wait` timeout |
| v6 | main / sub 多 Reactor，fd 固定线程、无锁 |
| v7 | 异步双缓冲日志 |
| v8 | `Buffer` / `Connection` 对象化 + 优雅退出 |

---

## v0–v2 — 基础阶段
- **v0 · socket 连通性 + RAII 雏形**：先用 `socket/bind/listen/accept` 跑通一个**回显服务器**（收到什么、转大写、发回去），目的不是业务，而是**验证 socket 流程**，并落地第一个 RAII 类 `Socket`：
  - 析构自动 `close`；**禁拷贝**防 double-close；**移动语义**用 `std::exchange` 转移 fd 所有权。
  - `createServerSocket` 里 `socket()` 一成功**立刻**交给 RAII 接管，之后任何 `throw` 都会自动 close → **异常安全**。
- **v1 · 在 socket 上实现 HTTP 协议**：不再回显，而是**解析请求行**（`parse_path` 取 path）、**构造响应**（`build_response` 拼状态行+头+`Content-Length`+body）、**路由分发**（`route`：`/`、`/hello`、`/api/time`、404，分别返回 html / text / json）。顺手用上 C++20 `std::format` 和原始字符串 `R"(...)"`。
- **v2 · 工程化收尾**：把裸 fd 的散落 `close` 收敛进对象生命周期，保证每条退出路径都不泄漏。
## v3 — 单 Reactor + 线程池（EPOLLONESHOT）

核心思想：

- 主线程用 `epoll` 监听所有 fd，只负责「事件分发」，不干活。
- 就绪的 client fd 被 `EPOLLONESHOT` 自动禁用，丢给线程池处理。
- 工作线程处理完 `read` / `respond` / `close`。
- `EPOLLONESHOT` 保证：同一 fd 同一时刻只有一个线程在操作。

## v4 — keep-alive 长连接

相比 v3 的核心改动：

- 响应完不 `close`，用 `EPOLL_CTL_MOD` 重置 oneshot，等下一个请求。
- 解析 `Connection` 头：HTTP/1.1 默认 keep-alive，`close` 才断开。
- 请求头不完整时也重置 oneshot，等下次数据到达再处理。

## v5 — 连接超时

相比 v4 的核心改动：

- 每个连接有超时时间（默认 60s 无活动自动关闭）。
- `epoll_wait` 的 timeout 不再是 `-1`，而是最近过期连接的剩余毫秒。
- `epoll_wait` 返回后调用 `tick()`，清理所有超时连接。
- 客户端每次收发数据都刷新超时。

## v6 — 多 Reactor（main / sub，无锁）

架构：

- 主线程（main Reactor）：`epoll` 监听 listen fd，`accept` 后轮询分配给 sub reactor。
- 子线程（sub Reactor）×N：每个有自己的 `epoll` + `eventfd` + 定时器。
  - `eventfd` 就绪 → 从队列取新 fd，加入自己的 `epoll`。
  - client fd 就绪 → 直接 `recv` / `parse` / `send`（本线程内，无锁无竞争）。

相比 v5 的核心改进：

- 没有 task queue 锁竞争 —— 每个 fd 固定在一个子线程。
- 不需要 `EPOLLONESHOT` —— 同一 fd 只有一个线程操作。
- 定时器无锁 —— 每个子线程有自己的定时器。
- `eventfd` 跨线程唤醒 —— 主线程到子线程的 fd 分发。

## v7 — 异步双缓冲日志

相比 v6 新增：

- `AsyncLogger`：后台线程 + 双缓冲，前端 `append` 近零阻塞。
- `FixedBuffer`：4MB 固定缓冲区。
- `Logger` + 宏：`LOG_INFO` / `LOG_ERROR`，格式化时间 / 级别 / tid。

双缓冲原理：

- 前端写 `currentBuffer_`，满了 swap 到 `buffers_`，唤醒后台。
- 后台把 `buffers_` swap 出来逐个写文件，同时前端拿到新 buffer 继续写。
- 两个 buffer 交替使用，前端几乎不阻塞。

## v8 — 对象化 + 优雅退出

相比 v7 新增：

- `Buffer`：输入 / 输出缓冲区，解决分包与半发送。
- `Connection`：连接对象，封装 fd + buffer + 生命周期。
- `shared_ptr`：事件循环持有引用，防止回调执行中析构。
- `EPOLLOUT`：可写事件，保证大响应完整发送。
- 信号处理：`SIGINT` / `SIGTERM` 优雅退出，flush 残留日志。

