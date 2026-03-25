# cpa-warden-windows-auto-maintain

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue)
![uv](https://img.shields.io/badge/deps-uv-6f42c1)

[English](README.md)

`cpa-warden-windows-auto-maintain` 是一个面向 Windows 的 CPA 长运行自动化维护项目。
它基于 [`fantasticjoe/cpa-warden`](https://github.com/fantasticjoe/cpa-warden) 二次开发，并保持对上游 `cpa_warden.py` 工作流的兼容。

## 上游归属声明

本仓库是基于 `cpa-warden` 的衍生项目。

- 上游项目：`fantasticjoe/cpa-warden`
- 本仓库衍生基线提交：`f3778f4334f443fd822c25935c1d2a1ee26c144b`
- 基线之后的提交重点在 Windows 自动化编排、通道调度安全性与运行稳定性增强

详细说明见 [NOTICE](NOTICE)。

## 项目最新状态（2026-03-25）

- `cwma/auto` 已按能力分组：`orchestration`、`channel`、`state`、`infra`、`ui`，并通过 `runtime` 适配层装配
- Stage-2.6 的能力拆分测试映射已收敛为：
  - `tests/test_auto_modules_process_channel.py`
  - `tests/test_auto_modules_state.py`
  - `tests/test_auto_modules_ui.py`
- 旧的 `cwma/auto/*.py` 顶层兼容包装模块已移除，当前以子包路径为准
- maintain 队列已升级为分阶段作业模型；全量与增量任务共享显式的步骤级状态转换
- maintain 服务已通过步骤引擎与流水线运行策略执行有序步骤（`scan -> delete_401 -> quota -> reenable -> finalize`）
- 上传稳定等待已采用“冻结当前候选批次”策略；窗口内新增/更新项延后到下一轮入队，且按路径合并待上传项（`last-writer-wins`）
- 智能调度批次决策已改为共享“总积压”信号（上传待处理 + 增量维护待处理 + 全量维护等价积压）；增量 defer 语义已收敛为仅 `batch_too_small_waiting_fill`
- 仪表盘面板已支持 maintain 步骤队列可观测（`steps_qr` / `steps_retry`）、full/incremental 作业计数，以及通道/流水线并行状态提示
- Stage 7 加固已完成：通过工作区临时目录沙箱修复 Python 3.14 Windows 下全量 unittest 稳定性问题，并补齐了 in-process 执行灰度/回滚文档

## 文档架构（避免冗余）

为降低文档漂移，每份文档只承担一个主职责：

| 文档 | 主职责 | 何时更新 |
| --- | --- | --- |
| `README.md` | 英文操作入口、运行行为摘要、核心命令 | 启动流程、运行行为、用户命令发生变化 |
| `README.zh-CN.md` | 中文镜像（语义与 `README.md` 对齐） | `README.md` 语义发生变化 |
| `ARCHITECTURE.md` | 当前模块边界、依赖规则、数据/并发/失败模型 | 包结构、状态模型、编排契约变化 |
| `cwma/auto/BOUNDARY_MAP.md` | `cwma/auto` 与 `cwma/auto/runtime` 的能力归属图 | 归属边界或依赖方向规则变化 |
| `CONTRIBUTING.md` | 贡献流程、验证要求、文档同步规则 | 协作流程、CI 验证、文档政策变化 |
| `CHANGELOG.md` | 版本层面的变更历史 | 有用户可见或维护价值明显的变更落地 |

## 这个项目解决什么问题

项目目标不是替换 `cpa_warden.py`，而是在 Windows 下把它稳定地“长期跑起来”：

- 监听 `auth_files`，在文件稳定后排队上传
- `upload` 与 `maintain` 两个通道独立调度
- 支持定时全量维护 + 上传后增量维护
- 上传/增量维护调度可根据积压自动在“实时并行”与“吞吐清队列”模式间切换
- 上传/增量维护批次不再只看单队列局部压力，而是由统一总积压驱动
- maintain 任务按显式分阶段流水线执行，步骤顺序可预测，并对 action 阶段应用账号级锁冲突规避
- 维护/上传运行数据库与日志分离
- 上传稳定等待采用冻结当前批次策略，避免等待窗口内变更无限重置计时
- 增量维护仅在“当前批次过小且存在上传侧补充来源”时才 defer（`batch_too_small_waiting_fill`）
- 终端面板直接展示 maintain pipeline 的 queue/running/retry/defer 状态，并区分 full/incremental 作业与并行状态
- 支持归档入口（`.zip/.7z/.rar`，Bandizip 优先，`.zip` 可回退 Windows 内置解压）
- 上传后清理文件并清理空目录
- 双层单实例保护（启动器锁 + Python 运行时锁）
- 默认失败即停，重试策略显式可控

## 当前组件布局

- `cwma/apps/cpa_warden.py`：兼容上游的 CPA 扫描/维护/上传 CLI 宿主
- `cwma/auto/app.py`：watcher 宿主与主编排入口
- `cwma/auto/orchestration/*`：启动与 watch 循环编排
- `cwma/auto/channel/*`：维护/上传通道命令与生命周期策略
- `cwma/auto/state/*`：队列/快照/状态转换与纯决策
- `cwma/auto/infra/*`：进程/归档/锁/配置/清理等副作用边界
- `cwma/auto/ui/*`：进度解析与终端仪表盘渲染
- `cwma/auto/runtime/*`：宿主适配层，装配 orchestration/state/infra/ui
- `cwma/warden/*`：CLI/config/services/api/db/models/exports 的领域拆分模块
- `cwma/scheduler/smart_scheduler.py`：自适应调度策略
- `cwma/common/config_parsing.py`：共享严格配置解析与路径处理
- `auto_maintain.py` / `cpa_warden.py` / `smart_scheduler.py`：仓库根目录兼容入口

更深层的模块归属和依赖规则，见 [ARCHITECTURE.md](ARCHITECTURE.md) 与 [`cwma/auto/BOUNDARY_MAP.md`](cwma/auto/BOUNDARY_MAP.md)。

## Watcher 执行流程

`auto_maintain.py` 主循环：

1. 加载配置（`环境变量 > --watch-config JSON > 默认值`）并初始化运行时状态。
2. 构建快照与已上传基线。
3. 可选巡检归档（`.zip/.7z/.rar`），并把解压出的 JSON 变化并入同一上传管线。
4. 按配置排队首轮维护/上传检查。
5. 在各自通道空闲时独立启动 `upload` 与 `maintain`。
6. 子进程运行期间执行主动探测，并按通道独立轮询/重试，同时推进 maintain 步骤引擎周期。
7. 上传稳定等待阶段冻结当前候选批次；窗口内新增/更新项延后到下一轮队列入队。
8. 上传成功后更新基线、执行清理，并按配置排队上传后增量维护。
9. `--once` 模式仅在运行中与排队任务都收敛后退出；未恢复失败返回非零。

## 环境要求

- Windows 10/11
- Python 3.11+
- [uv](https://docs.astral.sh/uv/)（推荐）
- Bandizip（可选，归档输入量大时建议安装）

## 快速开始

1. 安装依赖。

```bash
uv sync
```

2. 准备 CPA 配置文件。

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

4. 保留 `auth_files` 作为输入目录占位。

- 仓库只跟踪 `auth_files/.gitkeep`
- `auth_files` 下运行期 JSON/归档文件均被 git 忽略

5. 启动优化配置。

```bat
start_auto_maintain_optimized.bat
```

## 运行状态与忽略规则

- `.auto_maintain_state/` 仅用于运行时状态，已被 git 忽略
- `auth_files/*` 被忽略，仅放行 `auth_files/.gitkeep`
- `auto_maintain.config.json` 是本地运行配置，已被 git 忽略
- `*.sqlite3`、`*.log`、导出 JSON 等运行产物不应提交

## 默认配置（来自 `auto_maintain.config.example.json`）

- 维护周期：`2400s`
- 监听周期：`15s`
- 上传稳定等待：`5s`
- 上传批次大小：`100`
- 智能调度与自适应批处理：开启
- 上传后维护：开启
- 上传成功后删除源 JSON：开启
- 归档检测与自动解压：开启
- 单实例锁：开启
- 命令失败即停：开启

## 关键 Watcher 配置项

完整配置请以 `auto_maintain.config.example.json` 为准。常用项：

- 路径：`auth_dir`、`config_path`、`state_dir`、`maintain_db_path`、`upload_db_path`
- 轮询节奏：`maintain_interval_seconds`、`watch_interval_seconds`、`upload_stable_wait_seconds`
- 上传调度：`upload_batch_size`、`adaptive_upload_batching`、`upload_high_backlog_*`
- 维护调度：`adaptive_maintain_batching`、`incremental_maintain_*`、`maintain_high_backlog_*`
  - 调度模式切换规则：低积压偏小批次，提升上传/维护交错实时性；高积压偏大批次，加速队列清空
  - 总积压规则：上传/增量维护批次选择共享同一积压估算（上传待处理 + 增量待处理 + 全量维护等价积压）
  - 增量 defer 规则：仅用于“小批补充等待”（`batch_too_small_waiting_fill`），不再使用 cooldown/full-guard 旧语义
- 运行行为：`run_maintain_on_start`、`run_upload_on_start`、`run_maintain_after_upload`
- 执行后端开关：`inprocess_execution_enabled`（`false` 使用 legacy 子进程，`true` 使用进程内通道执行）
- 失败策略：`maintain_retry_count`、`upload_retry_count`、`command_retry_delay_seconds`、`continue_on_command_failure`
- 安全策略：`allow_multi_instance`、`maintain_assume_yes`
- 归档入口：`inspect_zip_files`、`auto_extract_zip_json`、`delete_zip_after_extract`、`archive_extensions`、`bandizip_*`、`use_windows_zip_fallback`

## 环境变量

`auto_maintain.py` 主要读取：

- `WATCH_CONFIG_PATH`
- `AUTH_DIR`、`CONFIG_PATH`、`STATE_DIR`
- `MAINTAIN_DB_PATH`、`UPLOAD_DB_PATH`
- `MAINTAIN_LOG_FILE`、`UPLOAD_LOG_FILE`
- 调度和节奏控制（`*_INTERVAL_*`、`*_BATCH_*`、`*_BACKLOG_*`）
- 行为开关（`RUN_*`、`ALLOW_MULTI_INSTANCE`、`CONTINUE_ON_COMMAND_FAILURE`、`MAINTAIN_ASSUME_YES`、`INPROCESS_EXECUTION_ENABLED`）
- 归档控制（`INSPECT_ZIP_FILES`、`AUTO_EXTRACT_ZIP_JSON`、`ARCHIVE_EXTENSIONS`、`BANDIZIP_*`、`USE_WINDOWS_ZIP_FALLBACK`）
- 面板开关（`AUTO_MAINTAIN_FIXED_PANEL`、`AUTO_MAINTAIN_PANEL_COLOR`）

优先级：

1. 环境变量
2. `--watch-config` / `WATCH_CONFIG_PATH` JSON
3. 内置默认值

## Stage 7 灰度与回滚

- 默认仍保持可回滚安全：`inprocess_execution_enabled=false`（子进程后端）。
- 灰度上线建议：
  1. 在 `auto_maintain.config.json` 打开 `inprocess_execution_enabled=true`（或设置 `INPROCESS_EXECUTION_ENABLED=1`）
  2. 先跑短窗口 `--once` 检查，再进入常规 watcher 窗口观察
  3. 灰度期间保持 fail-fast（`continue_on_command_failure=false`）
- 快速回滚：
  - 将 `inprocess_execution_enabled=false`（或 `INPROCESS_EXECUTION_ENABLED=0`）并重启 watcher
  - 行为会回到 legacy 子进程生命周期，不影响根入口命令

## 核心 CLI 兼容性

`cpa_warden.py` 行为保持与上游兼容。常用命令：

```bash
uv run python cpa_warden.py --mode scan
uv run python cpa_warden.py --mode maintain --yes
uv run python cpa_warden.py --mode upload --upload-dir ./auth_files --upload-recursive
uv run python cpa_warden.py --mode maintain-refill --min-valid-accounts 200 --upload-dir ./auth_files
```

单轮自检：

```bash
uv run python auto_maintain.py --watch-config ./auto_maintain.config.json --once
```

## 验证

CI 基线命令：

```bash
uv run python -m py_compile cpa_warden.py auto_maintain.py clean_codex_accounts.py
uv run python cpa_warden.py --help
uv run python auto_maintain.py --help
uv run python -m unittest -v tests/test_auto_maintain.py
```

本地可选全量测试：

```bash
uv run python -m unittest discover -s tests -p "test_*.py"
```

说明：测试套件已通过 `tests/temp_sandbox.py` 将 `tempfile.TemporaryDirectory()` 适配到仓库工作区安全目录，避免受限 Windows/Python 3.14 环境下的权限波动导致全量回归不稳定。

## 参与贡献

见 [CONTRIBUTING.md](CONTRIBUTING.md)。

## 更新记录

见 [CHANGELOG.md](CHANGELOG.md)。

## 安全说明

见 [SECURITY.md](SECURITY.md)。

## 许可证

MIT，详见 [LICENSE](LICENSE)。
