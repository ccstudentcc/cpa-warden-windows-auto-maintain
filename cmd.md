uv run python cpa_warden.py --mode maintain --yes
uv run python cpa_warden.py --mode maintain --maintain-names-file ./.auto_maintain_state/maintain_names_scope.txt --yes

uv run python cpa_warden.py --mode upload --upload-dir ./auth_files --upload-recursive
uv run python cpa_warden.py --mode upload --upload-dir ./auth_files --upload-recursive --upload-names-file ./.auto_maintain_state/upload_names_scope.txt

对限额账号使用更激进策略（直接删除）：
uv run python cpa_warden.py --mode maintain --quota-action delete

uv run python auto_maintain.py --watch-config ./auto_maintain.config.json --once

stage-2.6 capability regression (run serially):
uv run python -m unittest -q tests.test_auto_modules_process_channel
uv run python -m unittest -q tests.test_auto_modules_state
uv run python -m unittest -q tests.test_auto_modules_ui
uv run python -m unittest -q tests.test_auto_modules_process_channel tests.test_auto_modules_state tests.test_auto_modules_ui
uv run python -m py_compile tests/test_auto_modules_process_channel.py tests/test_auto_modules_state.py tests/test_auto_modules_ui.py

说明：
- 在受限运行环境中，若出现 KeyboardInterrupt 或退出码 130/137，按“外部中断”处理，不视为稳定结果。
- 需按上面命令串行重跑，直到回归命令稳定完成。

copy auto_maintain.config.example.json auto_maintain.config.json

start_auto_maintain_optimized.bat
