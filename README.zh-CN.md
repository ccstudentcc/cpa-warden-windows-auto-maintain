# cpa-warden-windows-auto-maintain

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue)
![uv](https://img.shields.io/badge/deps-uv-6f42c1)

[English](README.md)

`cpa-warden-windows-auto-maintain` 是一个面向 Windows 的 CPA 自动化维护项目。
它基于 [`fantasticjoe/cpa-warden`](https://github.com/fantasticjoe/cpa-warden) 二次开发，并保持对上游 `cpa_warden.py` 工作流的兼容。

## 上游归属声明

本仓库是基于 `cpa-warden` 的衍生项目。

- 上游项目：`fantasticjoe/cpa-warden`
- 本仓库衍生基线提交：`f3778f4334f443fd822c25935c1d2a1ee26c144b`
- 基线之后的提交重点在 Windows 自动化编排、调度和运行稳定性增强。

详细说明见 [NOTICE](NOTICE)。

## 项目重点

这个项目的目标不是替代 `cpa_warden.py`，而是把它在 Windows 场景中稳定编排起来：

- 监听 `auth_files`，在文件稳定后触发上传
- 维护任务按计划执行，不再被长时间上传批次阻塞
- 维护与上传使用分离的 DB/日志路径
- 支持 ZIP 入口（Bandizip + Windows 回退解压）
- 上传后清理源文件，并清理空子目录
- 支持无人值守运行，默认失败即停，重试策略显式可控

## 改进特性总览

相对衍生基线提交（`f3778f4`），当前 watcher 的关键增强包括：

1. `upload` 与 `maintain` 并发调度，维护不再被长上传批次阻塞。
   - 定时维护（`MAINTAIN_INTERVAL_SECONDS`）是全量维护。
   - 上传后维护改为按本次上传名称集合执行增量维护。
2. 上传队列支持按批次切分（`UPLOAD_BATCH_SIZE`），并通过 `--upload-names-file` 做每批精准上传。
   - 单批上传更快结束。
   - 已上传批次可更早触发增量维护。
3. 维护/上传运行状态彻底拆分：`MAINTAIN_DB_PATH` + `UPLOAD_DB_PATH`，日志也拆分为 `MAINTAIN_LOG_FILE` + `UPLOAD_LOG_FILE`。
4. 上传基线一致性修复：部分批次成功时，会与历史已上传基线合并，而不是覆盖。
5. 快照扫描增强：对扫描期间文件瞬时消失/替换等文件系统竞态更稳健。
6. 上传完成后，若检测到基线外文件，会自动排队下一批上传。
7. ZIP 变更检测升级为签名比对（路径/大小/mtime），不再只看 ZIP 数量。
8. 上传清理后会继续清理 `auth_dir` 下空目录。
9. 默认失败即停（fail-fast），并保留上传/维护独立重试策略；`--once` 语义更严格。
10. 新增 `MAINTAIN_ASSUME_YES`，便于无人值守维护。
11. 单实例锁由 Python 侧统一仲裁，降低重复 watcher 并发风险。

## 执行逻辑（Watcher）

`auto_maintain.py` 主循环行为如下：

1. 建立初始快照，若无历史基线则初始化 `last_uploaded_snapshot`。
2. 可选执行 ZIP 巡检；若解压产生 JSON 变化，会立即进入上传检查。
3. 按启动参数决定是否排队首轮维护/上传。
4. 当各自通道空闲时，维护与上传独立启动，互不阻塞。
   - 上传通道按 `UPLOAD_BATCH_SIZE` 串行消费待上传队列。
   - 每批上传通过 `--upload-names-file` 约束命令侧上传范围。
5. 独立轮询两个子进程退出状态：
   - 上传成功会更新快照/基线，并按配置删除已上传源文件；
   - 维护成功会清理维护重试状态。
6. 上传成功后可选排队“上传后维护”。
   - 上传后维护通过 `--maintain-names-file` 仅处理“刚完成这一批上传”的账号名称集合。
7. 上传与维护失败分别进入各自重试窗口，互不干扰。
8. `--once` 模式下，只有运行中和排队任务都完成才退出；失败返回非零码。

## 核心组件

- `cpa_warden.py`：兼容上游的扫描/维护/上传 CLI
- `auto_maintain.py`：面向 Windows 的调度与目录监听器
- `auto_maintain.bat`：`uv -> python` 回退启动器
- `start_auto_maintain_optimized.bat`：生产化参数模板
- `auto_maintain.config.example.json`：watcher 配置模板
- `tests/test_auto_maintain.py`：调度与文件生命周期回归测试

## 环境要求

- Windows 10/11
- Python 3.11+
- [uv](https://docs.astral.sh/uv/)（推荐）
- Bandizip（可选，ZIP 输入量大时推荐）

## 快速开始

1. 安装依赖。

```bash
uv sync
```

2. 准备配置文件。

```bash
copy config.example.json config.json
```

至少填写：

- `base_url`
- `token`

3. 准备 watcher 配置文件。

```bash
copy auto_maintain.config.example.json auto_maintain.config.json
```

4. 保持 `auth_files` 作为输入目录占位。

- 仓库只跟踪 `auth_files/.gitkeep`
- `auth_files` 下运行期 JSON/ZIP 文件均被 git 忽略

5. 启动优化配置。

```bat
start_auto_maintain_optimized.bat
```

## 运行状态与忽略规则

- `.auto_maintain_state/` 仅用于运行时状态，已被 git 忽略
- `auth_files/*` 被忽略，仅放行 `auth_files/.gitkeep`
- `auto_maintain.config.json` 是本地运行配置，已被 git 忽略
- 建议不纳入提交的运行产物：
- `.auto_maintain_state/cpa_warden_maintain.sqlite3`
- `.auto_maintain_state/cpa_warden_upload.sqlite3`
- `.auto_maintain_state/cpa_warden_maintain.log`
- `.auto_maintain_state/cpa_warden_upload.log`
- `.auto_maintain_state/maintain_names_scope.txt`
- `.auto_maintain_state/upload_names_scope.txt`
- `.auto_maintain_state/last_uploaded_snapshot.txt`
- `.auto_maintain_state/current_snapshot.txt`
- `.auto_maintain_state/stable_snapshot.txt`

## 优化启动脚本默认策略

`start_auto_maintain_optimized.bat` 现在会读取 `auto_maintain.config.json`（首次运行若不存在，会从 `auto_maintain.config.example.json` 生成）。

当前模板默认值（`auto_maintain.config.example.json`）：

- 维护周期：`2400s`
- 监听周期：`15s`
- 上传稳定等待：`5s`
- 上传批次大小：`100`
- 深度扫描间隔：`120` 次循环
- 上传完成后触发维护：开启
- 上传成功后删除源 JSON：开启
- ZIP 检测和自动解压：开启
- 单实例锁：开启
- 命令失败即停：开启

并且所有 watcher 配置都支持环境变量覆盖。

## Watcher 配置参数说明

`auto_maintain.config.json` 的参数含义如下：

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `auth_dir` | `./auth_files` | 上传监听目录，JSON/ZIP 输入都从这里读取。 |
| `config_path` | `./config.json` | `cpa_warden.py` 使用的配置文件路径。 |
| `state_dir` | `./.auto_maintain_state` | 运行状态目录（锁、快照、日志、数据库）。 |
| `maintain_db_path` | `./.auto_maintain_state/cpa_warden_maintain.sqlite3` | 维护通道 SQLite 路径。 |
| `upload_db_path` | `./.auto_maintain_state/cpa_warden_upload.sqlite3` | 上传通道 SQLite 路径。 |
| `maintain_log_file` | `./.auto_maintain_state/cpa_warden_maintain.log` | 维护通道日志路径。 |
| `upload_log_file` | `./.auto_maintain_state/cpa_warden_upload.log` | 上传通道日志路径。 |
| `maintain_interval_seconds` | `2400` | 定时全量维护周期（秒）。 |
| `watch_interval_seconds` | `15` | watcher 主循环轮询间隔（秒）。 |
| `upload_stable_wait_seconds` | `5` | 检测到变化后，上传前稳定等待时长（秒）。 |
| `upload_batch_size` | `100` | 单次 upload 命令最多处理的 JSON 数量；剩余文件进入下一批串行上传。 |
| `deep_scan_interval_loops` | `120` | 无明显变化时，每 N 轮强制做一次深度扫描。 |
| `run_maintain_on_start` | `true` | 启动时是否先排队一次维护。 |
| `run_upload_on_start` | `true` | 启动时是否先做一次上传变化检查。 |
| `run_maintain_after_upload` | `true` | 上传后是否排队维护（增量范围）。 |
| `maintain_assume_yes` | `true` | 维护命令是否自动带 `--yes`（无人值守）。 |
| `delete_uploaded_files_after_upload` | `true` | 上传成功后是否删除源 JSON 文件。 |
| `maintain_retry_count` | `1` | 维护命令失败重试次数。 |
| `upload_retry_count` | `1` | 上传命令失败重试次数。 |
| `command_retry_delay_seconds` | `20` | 命令失败后重试等待时长（秒）。 |
| `continue_on_command_failure` | `false` | `true` 时命令失败后继续循环；`false` 为失败即停。 |
| `allow_multi_instance` | `false` | `true` 允许同一状态目录多实例运行；`false` 启用单实例保护。 |
| `inspect_zip_files` | `true` | 是否启用 ZIP 检测。 |
| `auto_extract_zip_json` | `true` | 是否自动解压 ZIP 中 JSON。 |
| `delete_zip_after_extract` | `true` | 解压成功后是否删除 ZIP 原文件。 |
| `bandizip_path` | `D:\\Bandizp\\Bandizip.exe` | Bandizip 可执行文件路径。 |
| `bandizip_timeout_seconds` | `120` | 单次 Bandizip 解压超时（秒）。 |
| `use_windows_zip_fallback` | `true` | Bandizip 不可用/失败时是否使用 Windows 内置解压回退。 |

说明：

- `maintain_interval_seconds` 只控制定时全量维护。
- 上传后维护按“每个已完成上传批次”的名称集合执行增量维护。
- 路径参数支持相对路径（相对仓库根目录解析）。

## 常用环境变量

`auto_maintain.py` 主要读取：

- `WATCH_CONFIG_PATH`
- `AUTH_DIR`、`CONFIG_PATH`、`STATE_DIR`
- `MAINTAIN_DB_PATH`、`UPLOAD_DB_PATH`
- `MAINTAIN_LOG_FILE`、`UPLOAD_LOG_FILE`
- `MAINTAIN_INTERVAL_SECONDS`、`WATCH_INTERVAL_SECONDS`
- `UPLOAD_STABLE_WAIT_SECONDS`、`UPLOAD_BATCH_SIZE`、`DEEP_SCAN_INTERVAL_LOOPS`
- `RUN_MAINTAIN_ON_START`、`RUN_UPLOAD_ON_START`、`RUN_MAINTAIN_AFTER_UPLOAD`
- `MAINTAIN_ASSUME_YES`
- `MAINTAIN_RETRY_COUNT`、`UPLOAD_RETRY_COUNT`、`COMMAND_RETRY_DELAY_SECONDS`
- `CONTINUE_ON_COMMAND_FAILURE`、`ALLOW_MULTI_INSTANCE`
- `INSPECT_ZIP_FILES`、`AUTO_EXTRACT_ZIP_JSON`、`DELETE_ZIP_AFTER_EXTRACT`
- `BANDIZIP_PATH`、`BANDIZIP_TIMEOUT_SECONDS`、`USE_WINDOWS_ZIP_FALLBACK`

优先级：

- 环境变量
- `--watch-config` / `WATCH_CONFIG_PATH` 指定的 JSON 文件
- 内置默认值

单轮自检运行：

```bash
uv run python auto_maintain.py --watch-config ./auto_maintain.config.json --once
```

## 核心 CLI 兼容性

`cpa_warden.py` 行为保持与上游兼容，常用命令示例：

```bash
uv run python cpa_warden.py --mode scan
uv run python cpa_warden.py --mode maintain --yes
uv run python cpa_warden.py --mode upload --upload-dir ./auth_files --upload-recursive
uv run python cpa_warden.py --mode maintain-refill --min-valid-accounts 200 --upload-dir ./auth_files
```

## 验证命令

```bash
uv run python -m py_compile cpa_warden.py auto_maintain.py clean_codex_accounts.py
uv run python cpa_warden.py --help
uv run python auto_maintain.py --help
uv run python -m unittest -v tests/test_auto_maintain.py
```

## 参与贡献

见 [CONTRIBUTING.md](CONTRIBUTING.md)。

## 更新记录

见 [CHANGELOG.md](CHANGELOG.md)。

## 安全说明

见 [SECURITY.md](SECURITY.md)。

## 许可证

MIT，详见 [LICENSE](LICENSE)。
