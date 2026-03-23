uv run python cpa_warden.py --mode maintain --yes
uv run python cpa_warden.py --mode maintain --maintain-names-file ./.auto_maintain_state/maintain_names_scope.txt --yes

uv run python cpa_warden.py --mode upload --upload-dir ./auth_files --upload-recursive

uv run python auto_maintain.py --once

start_auto_maintain_optimized.bat
