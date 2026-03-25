# 常用命令速查

## 1) 初始化与配置

```bash
uv sync
copy config.example.json config.json
copy auto_maintain.config.example.json auto_maintain.config.json
```

## 2) 核心 CLI（cpa_warden.py）

### 扫描

```bash
uv run python cpa_warden.py --mode scan
```

### 维护（全量/按范围）

```bash
uv run python cpa_warden.py --mode maintain --yes
uv run python cpa_warden.py --mode maintain --maintain-names-file ./.auto_maintain_state/maintain_names_scope.txt --yes
# 按步骤运行（示例：只执行 scan + quota + finalize）
uv run python cpa_warden.py --mode maintain --maintain-steps scan,quota,finalize --yes
```

### 上传（全目录/按范围）

```bash
uv run python cpa_warden.py --mode upload --upload-dir ./auth_files --upload-recursive
uv run python cpa_warden.py --mode upload --upload-dir ./auth_files --upload-recursive --upload-names-file ./.auto_maintain_state/upload_names_scope.txt
```

### 维护补量

```bash
uv run python cpa_warden.py --mode maintain-refill --min-valid-accounts 200 --upload-dir ./auth_files
```

### 限额账号激进策略（直接删除）

```bash
uv run python cpa_warden.py --mode maintain --quota-action delete
```

## 3) Watcher（auto_maintain.py）

### 单轮自检

```bash
uv run python auto_maintain.py --watch-config ./auto_maintain.config.json --once
```

### 启动优化配置（Windows）

```bat
start_auto_maintain_optimized.bat
```

## 4) 帮助与基础校验

```bash
uv run python cpa_warden.py --help
uv run python auto_maintain.py --help
uv run python -m py_compile cpa_warden.py auto_maintain.py clean_codex_accounts.py
```

## 5) Stage-2.6 能力拆分回归（建议串行）

```bash
uv run python -m unittest -q tests.test_auto_modules_process_channel
uv run python -m unittest -q tests.test_auto_modules_state
uv run python -m unittest -q tests.test_auto_modules_ui
uv run python -m unittest -q tests.test_auto_modules_process_channel tests.test_auto_modules_state tests.test_auto_modules_ui
uv run python -m py_compile tests/test_auto_modules_process_channel.py tests/test_auto_modules_state.py tests/test_auto_modules_ui.py
```

## 6) 测试中断说明

- 在受限运行环境中，如出现 `KeyboardInterrupt` 或退出码 `130/137`，按“外部中断”处理，不视为稳定结果。
- 需要将对应命令串行重跑，直到回归命令稳定完成。

## 7) StageX 对比报告（质量闸门 G3）

```bash
uv run python tools/stage0_json_replay_benchmark.py --output results/stageX_json_replay_candidate.csv
uv run python tools/stage_comparison_report.py --candidate results/stageX_json_replay_candidate.csv --output results/stageX_vs_stage0_report.md --stage-label "Stage X" --commit-ref <commit-sha>
```

非性能改动（例如可靠性/约束控制）可用 N/A 模式仅产出报告不触发 Gate-B 严格判定：

```bash
uv run python tools/stage_comparison_report.py --candidate results/stageX_json_replay_candidate.csv --output results/stageX_vs_stage0_report.md --stage-label "Stage X" --commit-ref <commit-sha> --gate-b-mode na
```

## 8) G1/G2 一键质量闸门

```bash
uv run python tools/quality_gate_runner.py --output results/quality_gate_report.md --strict
```

说明：该命令会执行 `BASE`（compile + CLI help）以及 `G1/G2` 测试闸门。

仅跑 G1（跳过 G2 集成套件）：

```bash
uv run python tools/quality_gate_runner.py --g1-only --output results/quality_gate_report.md --strict
```
