from __future__ import annotations

import shutil


def fit_panel_line(text: str) -> str:
    width = shutil.get_terminal_size((160, 20)).columns
    if width <= 0:
        return text
    if len(text) <= width:
        return text
    if width <= 3:
        return text[:width]
    return text[: width - 3] + "..."


def panel_border_line(char: str = "=") -> str:
    width = max(40, shutil.get_terminal_size((120, 20)).columns)
    # Keep a small margin so borders do not hit the edge in narrow terminals.
    inner = max(10, width - 4)
    return f"+{char * inner}+"


def color_text(text: str, code: str, *, enabled: bool) -> str:
    if not enabled:
        return text
    return f"\x1b[{code}m{text}\x1b[0m"


def state_color_code(state: str) -> str:
    mapping = {
        "running": "32",  # green
        "pending": "33",  # yellow
        "idle": "37",  # gray/white
        "failed": "31",  # red
    }
    return mapping.get(state, "36")


def apply_panel_colors(
    lines: list[str],
    *,
    enabled: bool,
    upload_state: str,
    maintain_state: str,
    upload_stage: str,
    maintain_stage: str,
) -> list[str]:
    if not enabled:
        return lines

    colored = list(lines)
    if len(colored) >= 1:
        colored[0] = color_text(colored[0], "90", enabled=enabled)
    if len(colored) >= 2:
        colored[1] = color_text(colored[1], "1;36", enabled=enabled)
    if len(colored) >= 3:
        line = colored[2]
        line = line.replace("UPLOAD", color_text("UPLOAD", "1;34", enabled=enabled), 1)
        line = line.replace(
            f"state={upload_state}",
            f"state={color_text(upload_state, state_color_code(upload_state), enabled=enabled)}",
            1,
        )
        line = line.replace(
            f"stage={upload_stage}",
            f"stage={color_text(upload_stage, '36', enabled=enabled)}",
            1,
        )
        colored[2] = line
    if len(colored) >= 4:
        colored[3] = color_text(colored[3], "90", enabled=enabled)
    if len(colored) >= 5:
        colored[4] = color_text(colored[4], "90", enabled=enabled)
    if len(colored) >= 6:
        line = colored[5]
        line = line.replace("MAINTAIN", color_text("MAINTAIN", "1;35", enabled=enabled), 1)
        line = line.replace(
            f"state={maintain_state}",
            f"state={color_text(maintain_state, state_color_code(maintain_state), enabled=enabled)}",
            1,
        )
        line = line.replace(
            f"stage={maintain_stage}",
            f"stage={color_text(maintain_stage, '36', enabled=enabled)}",
            1,
        )
        colored[5] = line
    if len(colored) >= 7:
        colored[6] = color_text(colored[6], "90", enabled=enabled)
    if len(colored) >= 8:
        colored[7] = color_text(colored[7], "90", enabled=enabled)
    return colored
