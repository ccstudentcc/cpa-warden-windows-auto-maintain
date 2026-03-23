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
2. 维护/上传运行状态彻底拆分：`MAINTAIN_DB_PATH` + `UPLOAD_DB_PATH`，日志也拆分为 `MAINTAIN_LOG_FILE` + `UPLOAD_LOG_FILE`。
3. 上传基线一致性修复：上传进行中新出现的文件，不会被误判为“已上传”。
4. 快照扫描增强：对扫描期间文件瞬时消失/替换等文件系统竞态更稳健。
5. 上传完成后，若检测到基线外文件，会自动排队下一批上传。
6. ZIP 变更检测升级为签名比对（路径/大小/mtime），不再只看 ZIP 数量。
7. 上传清理后会继续清理 `auth_dir` 下空目录。
8. 默认失败即停（fail-fast），并保留上传/维护独立重试策略；`--once` 语义更严格。
9. 新增 `MAINTAIN_ASSUME_YES`，便于无人值守维护。
10. 单实例锁由 Python 侧统一仲裁，降低重复 watcher 并发风险。

## 执行逻辑（Watcher）

`auto_maintain.py` 主循环行为如下：

1. 建立初始快照，若无历史基线则初始化 `last_uploaded_snapshot`。
2. 可选执行 ZIP 巡检；若解压产生 JSON 变化，会立即进入上传检查。
3. 按启动参数决定是否排队首轮维护/上传。
4. 当各自通道空闲时，维护与上传独立启动，互不阻塞。
5. 独立轮询两个子进程退出状态：
   - 上传成功会更新快照/基线，并按配置删除已上传源文件；
   - 维护成功会清理维护重试状态。
6. 上传成功后可选排队“上传后维护”。
7. 上传与维护失败分别进入各自重试窗口，互不干扰。
8. `--once` 模式下，只有运行中和排队任务都完成才退出；失败返回非零码。

## 核心组件

- `cpa_warden.py`：兼容上游的扫描/维护/上传 CLI
- `auto_maintain.py`：面向 Windows 的调度与目录监听器
- `auto_maintain.bat`：`uv -> python` 回退启动器
- `start_auto_maintain_optimized.bat`：生产化参数模板
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

3. 保持 `auth_files` 作为输入目录占位。

- 仓库只跟踪 `auth_files/.gitkeep`
- `auth_files` 下运行期 JSON/ZIP 文件均被 git 忽略

4. 启动优化配置。

```bat
start_auto_maintain_optimized.bat
```

## 运行状态与忽略规则

- `.auto_maintain_state/` 仅用于运行时状态，已被 git 忽略
- `auth_files/*` 被忽略，仅放行 `auth_files/.gitkeep`
- 建议不纳入提交的运行产物：
- `.auto_maintain_state/cpa_warden_maintain.sqlite3`
- `.auto_maintain_state/cpa_warden_upload.sqlite3`
- `.auto_maintain_state/cpa_warden_maintain.log`
- `.auto_maintain_state/cpa_warden_upload.log`
- `.auto_maintain_state/last_uploaded_snapshot.txt`
- `.auto_maintain_state/current_snapshot.txt`
- `.auto_maintain_state/stable_snapshot.txt`

## 优化启动脚本默认策略

`start_auto_maintain_optimized.bat` 当前默认：

- 维护周期：`2400s`
- 监听周期：`30s`
- 上传稳定等待：`10s`
- 深度扫描间隔：`120` 次循环
- 上传完成后触发维护：开启
- 上传成功后删除源 JSON：开启
- ZIP 检测和自动解压：开启
- 单实例锁：开启
- 命令失败即停：开启

并且优先使用环境变量 `BANDIZIP_PATH`，仅在未设置时回退到 `D:\Bandizp\Bandizip.exe`。

## 常用环境变量

`auto_maintain.py` 主要读取：

- `AUTH_DIR`、`CONFIG_PATH`、`STATE_DIR`
- `MAINTAIN_DB_PATH`、`UPLOAD_DB_PATH`
- `MAINTAIN_LOG_FILE`、`UPLOAD_LOG_FILE`
- `MAINTAIN_INTERVAL_SECONDS`、`WATCH_INTERVAL_SECONDS`
- `UPLOAD_STABLE_WAIT_SECONDS`、`DEEP_SCAN_INTERVAL_LOOPS`
- `RUN_MAINTAIN_ON_START`、`RUN_UPLOAD_ON_START`、`RUN_MAINTAIN_AFTER_UPLOAD`
- `MAINTAIN_ASSUME_YES`
- `MAINTAIN_RETRY_COUNT`、`UPLOAD_RETRY_COUNT`、`COMMAND_RETRY_DELAY_SECONDS`
- `CONTINUE_ON_COMMAND_FAILURE`、`ALLOW_MULTI_INSTANCE`
- `INSPECT_ZIP_FILES`、`AUTO_EXTRACT_ZIP_JSON`、`DELETE_ZIP_AFTER_EXTRACT`
- `BANDIZIP_PATH`、`BANDIZIP_TIMEOUT_SECONDS`、`USE_WINDOWS_ZIP_FALLBACK`

单轮自检运行：

```bash
uv run python auto_maintain.py --once
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
