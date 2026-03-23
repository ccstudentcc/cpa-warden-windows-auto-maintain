# Security Policy

## Supported Versions

This project is currently in active development. Security fixes are prioritized for:

- the latest `main` branch
- the latest tagged release line

## Reporting A Vulnerability

Do not disclose suspected vulnerabilities in public issues.

Preferred path:

1. Use GitHub private vulnerability reporting if available.
2. Otherwise contact the repository maintainer privately before public disclosure.

Please include:

- affected version or commit
- reproduction steps
- impact summary
- sanitized evidence (logs/screenshots)

## Sensitive Data Rules

Never include real secrets in reports:

- CPA tokens
- raw auth JSON files
- unsanitized account identifiers
- `.auto_maintain_state` SQLite/log files from production

Sanitize all operational artifacts before sharing.
