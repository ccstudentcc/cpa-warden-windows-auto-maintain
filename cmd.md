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
