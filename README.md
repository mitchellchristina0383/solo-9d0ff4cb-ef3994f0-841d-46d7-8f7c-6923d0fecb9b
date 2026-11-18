# 事故诊断包断点续传服务

值班工程师接收事故诊断包的上传网页：网络断开可续传，交接时只能下载校验通过的完整文件。

## 功能

- 浏览器创建任务时提交 **文件大小 + SHA-256**，按 **1 MiB** 分块上传
- 重开页面可查看各任务 **已收/缺失分块** 并补齐（断点续传）
- 相同分块同内容重传 **幂等去重**；异内容 **拒绝并保留原块**（409）
- 提交完成时服务端核对 **分块齐全、总大小、整体 SHA-256**，通过才发布下载
- 缺块可补传；摘要不符 **拒绝发布** 并提示重新创建任务
- 多进程并发上传/提交、磁盘写失败、进程重启均不会发布残缺文件或产生两份成品
- 已发布的包重启后仍可下载
- **隔离**：发现已发布的包含敏感内容时，在网页填写原因并确认后——
  - 隔离状态先持久化到 SQLite（`synchronous=FULL`），**保存成功页面才显示成功**，保存失败状态不变；
  - **新下载一律拒绝**（403，附隔离原因）；
  - **在途下载**逐片复核隔离标记，确认后立即中止连接，不再继续发送文件字节；
  - 断开的下载连接不持有任何锁，**不妨碍隔离生效**；重启后隔离依然有效；
  - **文件保留在磁盘上**（`published/`）供内部排查，不删除；
  - 其他包的上传、发布、下载不受影响。

## 快速开始

### Docker Compose

```bash
docker compose up --build
# 打开 http://localhost:8000
```

容器内以 `--workers 2` 运行两个服务进程，共享挂载卷中的 SQLite 与数据目录。

### 本地开发

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt -r requirements-dev.txt
DATA_DIR=data .venv/bin/uvicorn app.main:app --port 8000
# 打开 http://localhost:8000
```

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/tasks` | 创建任务 `{filename, size, sha256}`，返回 `task_id` 与分块参数 |
| GET | `/api/tasks` | 任务列表（状态、已收分块数、隔离信息） |
| GET | `/api/tasks/{id}` | 任务详情：`received_chunks` / `missing_chunks` / `state` / `quarantine_reason` |
| PUT | `/api/tasks/{id}/chunks/{i}` | 上传第 i 块（1 MiB，最后一块按实际大小）。同内容重传返回 `deduplicated:true`；异内容 409 |
| POST | `/api/tasks/{id}/finalize` | 核对分块+大小+摘要，通过则发布；缺块返回 409+`missing_chunks`；摘要不符标记 `failed` |
| POST | `/api/tasks/{id}/quarantine` | 隔离已发布的包，必填 `{reason}`。持久化成功才返回 200；重复调用幂等（保留首次记录）；未发布 409 |
| GET | `/api/tasks/{id}/download` | 下载已发布的包（未发布 409；已隔离 403；在途下载在隔离生效后中止连接） |

任务状态机：`uploading` → `published` / `failed`（终态，摘要不符）；`published` 可叠加隔离标记，
页面显示为 `quarantined`（终态，下载拒绝，文件保留）。

下载默认按 **2 MiB/s** 限速（`UPLOADER_DOWNLOAD_RATE`，字节/秒，`0` 为不限速）：
限速保证流式下载不会把整包抢先灌入内核/TCP 缓冲——已入缓冲的字节在隔离生效时无法收回，
限速+逐片复核把隔离后可能继续泄漏的字节数约束在「限速 × 反应时间 + 连接在途缓冲」量级。

## 一致性设计

- **分块幂等**：`(task_id, chunk_index)` 主键。先写临时文件 → INSERT 抢主键 → 赢者原子改名落盘；撞主键则比对内容哈希，相同去重、不同 409 保留原块。
- **发布原子性**：成品先在临时目录完整拼装并 fsync，**校验通过后**才 `os.replace` 原子改名到 published 路径；任何时刻 published 路径要么不存在、要么是完整成品。
- **多进程互斥**：finalize 用 per-task `flock` 串行化；SQLite WAL + busy_timeout 处理并发写。两个进程同时提交，只会有一个完成校验-改名-置态，另一个读到已发布直接返回。
- **隔离一致性**：隔离是 `tasks` 行上的单语句 UPDATE（自动提交、`synchronous=FULL`），返回 200 前已持久化；UPDATE 失败则整体不变、任务保持原状态。隔离标记存于共享 SQLite，两个服务进程即时互相可见；下载流每 256 KiB 复核一次标记，命中即中止连接（客户端表现为下载失败而非静默截断）。下载连接不持有任何锁，客户端断开只会取消对应流，不影响隔离写入。
- **崩溃恢复**：状态全在 SQLite（synchronous=FULL）与磁盘。重启后已收分块保留；DB 有记录但文件丢失的块（INSERT 后、改名前崩溃）在 finalize 时被发现并允许补传；隔离标记重启后依然有效。
- **磁盘写失败**：改名失败即回滚 DB 记录并返回 500，任务保持 `uploading`，不会产生半成品。

## 测试

```bash
.venv/bin/python -m pytest tests/ -v
```

- `tests/test_app.py`（21 项）：完整流程、续传、幂等去重、异内容拒绝、缺块补传、摘要拒绝、磁盘失败注入（chunk 上传 / finalize 两条路径）、重启恢复、空文件、参数校验；隔离：必填原因、仅已发布可隔离、幂等保留首次记录、新下载 403、文件保留、保存失败注入（状态不变）、重启后有效、不影响其他包。
- `tests/test_live.py`（6 项）：两个 uvicorn 进程并发上传/并发提交同一任务、kill -9 后重启续传、已发布包重启后可下载、跨进程同块并发去重；隔离：跨进程在途慢速下载被中止（连接中断、未收齐全文）、客户端断开不妨碍隔离生效、kill -9 重启后隔离仍有效。

## 数据布局（`DATA_DIR`）

```
uploader.db        SQLite（WAL）
chunks/<task>/     已收分块
published/<task>   校验通过后的成品（原子改名出现）
tmp/               临时文件（启动时清理超过 1 小时的残留）
locks/             per-task flock 文件
```
