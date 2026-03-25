# Contributing

## Scope

Contributions are welcome for:

- Windows automation reliability
- watcher/scheduler correctness
- operational safety and observability
- documentation and onboarding quality

This repository is a derivative project based on `cpa-warden`. Please keep upstream attribution intact and do not remove [NOTICE](NOTICE).

## Local Setup

```bash
uv sync
uv run python -m py_compile cpa_warden.py auto_maintain.py clean_codex_accounts.py
uv run python cpa_warden.py --help
uv run python auto_maintain.py --help
uv run python -m unittest -v tests/test_auto_maintain.py
uv run python tools/quality_gate_runner.py --output results/quality_gate_report.md --strict
# non-performance stage-comparison evidence (Gate B marked as N/A)
uv run python tools/stage_comparison_report.py --candidate results/stageX_json_replay_candidate.csv --output results/stageX_vs_stage0_report.md --stage-label "Stage X" --commit-ref <commit-sha> --gate-b-mode na
```

## Development Guidelines

- Keep changes small and incremental.
- Do not hardcode secrets. Use environment variables and local ignored config files.
- If behavior changes, update related docs and `CHANGELOG.md` in the same PR.
- Keep production output concise and keep deep detail in log files.
- Prefer explicit error handling. Avoid silent exception swallowing.
- Avoid broad refactors unless required for the concrete change.

## Runtime Artifact Policy

Do not commit runtime artifacts:

- `config.json`
- `auto_maintain.config.json`
- `.auto_maintain_state/*`
- `auth_files/*` (except `auth_files/.gitkeep`)
- local SQLite/log/export files

If you share logs for debugging, sanitize tokens, account identifiers, and sensitive operational details.

## Pull Request Expectations

- Describe why the change is needed.
- Summarize behavior impact and risk.
- List validation commands that were run.
- Update docs and changelog when user-facing behavior changes.
- Keep examples and screenshots sanitized.
- CI automatically uploads `quality_gate_report_ci.md` as an artifact per Python version; use it as the primary gate evidence in PR discussion.
- CI job summary now includes `Overall` + Gate Summary table extracted from `quality_gate_report_ci.md` for quick triage.

## Test Runner Interruptions (Known Behavior In Constrained Environments)

When tests are executed in constrained runner environments (for example sandboxed agent sessions), commands may be interrupted by an external signal even if assertions themselves are healthy.

Typical symptoms:

- `KeyboardInterrupt` in test output
- non-zero process exit like `130` / `137`
- partial output that may still include passing test lines before interruption

Validation rule for this repository:

1. Treat `KeyboardInterrupt` / `130` / `137` as **interrupted run**, not as a reliable pass/fail signal.
2. Re-run the same validation commands **serially** (one command at a time).
3. Only accept the validation result when the final rerun exits cleanly and covers the intended test set.
4. Avoid running multiple `uv run ... unittest ...` commands in parallel during final PR validation.

## Documentation Consistency

Keep docs single-purpose and avoid copy-paste drift:

- `README.md`: operator-facing usage and command examples
- `README.zh-CN.md`: Chinese mirror of `README.md` (same meaning)
- `ARCHITECTURE.md`: module boundaries, runtime flow, state/concurrency/failure contracts
- `cwma/auto/BOUNDARY_MAP.md`: `cwma/auto` capability ownership and dependency directions
- `CONTRIBUTING.md`: workflow and validation expectations

Update rules:

1. Operator-visible behavior change: update `README.md`, `README.zh-CN.md`, and `CHANGELOG.md`.
2. Boundary/state/failure model change: update `ARCHITECTURE.md` (and `cwma/auto/BOUNDARY_MAP.md` when ownership/directions change).
3. Keep Chinese and English README aligned in meaning (not necessarily sentence-by-sentence literal translation).

## Security

For vulnerabilities, do not open a public issue first. Follow [SECURITY.md](SECURITY.md).
