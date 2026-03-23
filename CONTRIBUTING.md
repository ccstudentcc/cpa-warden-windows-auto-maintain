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
```

## Development Guidelines

- Keep changes small and incremental.
- Do not hardcode secrets. Use environment variables and local ignored config files.
- If behavior changes, update `README.md`, `README.zh-CN.md`, and `CHANGELOG.md` in the same PR.
- Keep production output concise and keep deep detail in log files.
- Prefer explicit error handling. Avoid silent exception swallowing.
- Avoid broad refactors unless required for the concrete change.

## Runtime Artifact Policy

Do not commit runtime artifacts:

- `config.json`
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

## Documentation Consistency

`README.md` and `README.zh-CN.md` should remain aligned in meaning.

When scheduler/watcher behavior changes, update these sections explicitly:

- "Improvement Highlights / 改进特性总览"
- "Execution Logic / 执行逻辑（Watcher）"
- `ARCHITECTURE.md` concurrency/snapshot/failure model sections

## Security

For vulnerabilities, do not open a public issue first. Follow [SECURITY.md](SECURITY.md).
