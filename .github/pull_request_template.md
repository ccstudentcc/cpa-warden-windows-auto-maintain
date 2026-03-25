## Summary

Describe the purpose of this pull request.

## Changes

- 

## Validation

- [ ] `uv run python tools/quality_gate_runner.py --output results/quality_gate_report.md --strict`
- [ ] `uv run python tools/stage_comparison_report.py --candidate results/stageX_json_replay_candidate.csv --output results/stageX_vs_stage0_report.md --stage-label "Stage X" --commit-ref <commit-sha>`（若涉及性能闸门）
- [ ] 非性能改动时：`uv run python tools/stage_comparison_report.py --candidate results/stageX_json_replay_candidate.csv --output results/stageX_vs_stage0_report.md --stage-label "Stage X" --commit-ref <commit-sha> --gate-b-mode na`
- [ ] Runtime behavior tested locally if applicable

## Notes

Include any config, logging, or output-format changes here.

## Security checklist

- [ ] No real tokens or secrets were committed
- [ ] No sensitive account identifiers were added without sanitization
