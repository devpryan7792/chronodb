#!/usr/bin/env python3
"""Render `assets/bench.svg` (README chart) from a recorded benchmark result.

The input is the JSON produced by a `chronodb readbench` +
`bench/compare_read.py` run — see `bench/results/`. Reads the newest result
file by default, so the chart stays in sync with what was actually measured.

Usage:

    python3 bench/render_charts.py [--results PATH] [--out PATH]
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
RESULTS_DIR = Path(__file__).resolve().parent / "results"

WIDTH = 1100
LABEL_X = 24
BAR_X = 250
BAR_W = WIDTH - BAR_X - 170
BAR_H = 11
BAR_GAP = 3
ROW_H = 32
PANEL_TOP = 26
HEADER_H = 96
FOOTER_H = 52

AXIS_MIN = 0.1
AXIS_MAX = 10000.0
TICKS = [0.1, 1.0, 10.0, 100.0, 1000.0, 10000.0]
TICK_LABELS = ["0.1 µs", "1 µs", "10 µs", "100 µs", "1 ms", "10 ms"]

BG = "#0b0f19"
PANEL_BG = "#0f172a"
GRID = "#1e293b"
TEXT = "#e5e7eb"
MUTED = "#94a3b8"
FAINT = "#64748b"

COLORS = {
    "chronodb": "#2dd4bf",
    "sqlite": "#60a5fa",
    "sqlite-mmap": "#818cf8",
    "duckdb": "#f59e0b",
}

PANELS = [
    (
        "asof",
        "as-of point query",
        "50 000 queries · random entity @ exact record timestamp",
    ),
    ("latest", "latest lookup", "20 000 lookups · newest version per entity"),
    (
        "entity_scan",
        "entity history scan",
        "2 000 scans · 200 records each, payloads materialised",
    ),
]


def esc(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def log_x(value: float) -> float:
    span = math.log10(AXIS_MAX) - math.log10(AXIS_MIN)
    return BAR_X + (math.log10(value) - math.log10(AXIS_MIN)) / span * BAR_W


def fmt(value: float) -> str:
    if value >= 100:
        return f"{value:.0f}"
    if value >= 10:
        return f"{value:.1f}"
    return f"{value:.2f}"


def render(results: dict) -> str:
    engines = results["engines"]
    order = ["chronodb", "sqlite", "sqlite-mmap", "duckdb"]
    config = results["config"]
    panel_h = PANEL_TOP + 30 + len(order) * ROW_H + 18
    height = HEADER_H + len(PANELS) * panel_h + FOOTER_H

    out: list[str] = []
    out.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {WIDTH} {height}" '
        f'width="{WIDTH}" height="{height}" role="img" '
        f'aria-label="ChronoDB read latency compared to SQLite and DuckDB">'
    )
    out.append(
        "<style>"
        'text{font-family:ui-sans-serif,system-ui,-apple-system,"Segoe UI",sans-serif}'
        ".mono{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}"
        "</style>"
    )
    out.append(f'<rect width="{WIDTH}" height="{height}" fill="{BG}"/>')

    # ---- header ----
    out.append(
        f'<text x="{LABEL_X}" y="40" font-size="23" font-weight="700" fill="{TEXT}">'
        f"ChronoDB read latency vs SQLite &amp; DuckDB</text>"
    )
    out.append(
        f'<text x="{LABEL_X}" y="64" font-size="13.5" fill="{MUTED}">'
        f"{config['records']:,} records × {config['payload_bytes']} B over "
        f"{config['entities']:,} entities · release build · warm cache · "
        f"per-call latency, lower is better (log scale)</text>"
    )
    # legend
    lx = WIDTH - 250
    for i, (name, color) in enumerate(
        [("p50", COLORS["chronodb"]), ("p99", COLORS["chronodb"])]
    ):
        opacity = 1.0 if name == "p50" else 0.5
        out.append(
            f'<rect x="{lx + i * 92}" y="30" width="14" height="10" rx="2" '
            f'fill="{color}" fill-opacity="{opacity}"/>'
        )
        out.append(
            f'<text x="{lx + 20 + i * 92}" y="39" font-size="12.5" fill="{MUTED}">{name}</text>'
        )

    # ---- panels ----
    y = HEADER_H
    for key, title, subtitle in PANELS:
        out.append(
            f'<rect x="{LABEL_X - 8}" y="{y - 8}" width="{WIDTH - 2 * (LABEL_X - 8)}" '
            f'height="{panel_h - 12}" rx="10" fill="{PANEL_BG}"/>'
        )
        out.append(
            f'<text x="{LABEL_X + 8}" y="{y + 16}" font-size="15" font-weight="600" '
            f'fill="{TEXT}">{esc(title)}</text>'
        )
        out.append(
            f'<text x="{LABEL_X + 8 + 200}" y="{y + 16}" font-size="12" fill="{FAINT}">'
            f"{esc(subtitle)}</text>"
        )

        rows_y = y + 40
        for tick, label in zip(TICKS, TICK_LABELS):
            gx = log_x(tick)
            out.append(
                f'<line x1="{gx:.1f}" y1="{rows_y - 8}" x2="{gx:.1f}" '
                f'y2="{rows_y + len(order) * ROW_H - 6}" stroke="{GRID}" stroke-width="1"/>'
            )
            out.append(
                f'<text x="{gx:.1f}" y="{rows_y + len(order) * ROW_H + 6}" font-size="10.5" '
                f'fill="{FAINT}" text-anchor="middle">{label}</text>'
            )

        base = engines["chronodb"][key]["p50"]
        for i, engine in enumerate(order):
            block = engines[engine]
            color = COLORS[engine]
            row_y = rows_y + i * ROW_H
            prefix = "▸ " if engine == "chronodb" else ""
            weight = "700" if engine == "chronodb" else "400"
            out.append(
                f'<text x="{LABEL_X + 8}" y="{row_y + 9}" font-size="12.5" fill="{color}" '
                f'font-weight="{weight}">{prefix}{esc(block["label"])}</text>'
            )
            rel = block[key].get("rel_p50", block[key]["p50"] / base)
            out.append(
                f'<text x="{LABEL_X + 8}" y="{row_y + 25}" font-size="10.5" fill="{FAINT}" '
                f'class="mono">{rel:.2f}×</text>'
            )
            for j, (stat, opacity) in enumerate([("p50", 1.0), ("p99", 0.5)]):
                value = block[key][stat]
                bx = log_x(value)
                by = row_y + j * (BAR_H + BAR_GAP)
                out.append(
                    f'<rect x="{BAR_X}" y="{by}" width="{max(bx - BAR_X, 1.2):.1f}" height="{BAR_H}" '
                    f'rx="2" fill="{color}" fill-opacity="{opacity}"/>'
                )
                out.append(
                    f'<text x="{bx + 6:.1f}" y="{by + BAR_H - 1.5}" font-size="10.5" '
                    f'fill="{MUTED}" class="mono">{fmt(value)}</text>'
                )
        y += panel_h

    # ---- footer ----
    fy = height - FOOTER_H + 24
    out.append(
        f'<text x="{LABEL_X}" y="{fy}" font-size="11.5" fill="{FAINT}">'
        f"μs per call · p50 / p99 shown · source: bench/results/"
        f"{esc(results.get('_source', ''))}</text>"
    )
    out.append(
        f'<text x="{LABEL_X}" y="{fy + 17}" font-size="11.5" fill="{FAINT}">'
        f"ChronoDB is timed in-process (Rust); SQLite and DuckDB include the "
        f"Python client round-trip — the comparison is conservative for them.</text>"
    )
    out.append(
        f'<text x="{WIDTH - LABEL_X}" y="{fy + 17}" font-size="11.5" fill="{FAINT}" '
        f'text-anchor="end">github.com/devpryan7792/chronodb</text>'
    )
    out.append("</svg>")
    return "\n".join(out) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path, default=None)
    parser.add_argument("--out", type=Path, default=REPO / "assets" / "bench.svg")
    args = parser.parse_args()

    results_path = args.results
    if results_path is None:
        candidates = sorted(RESULTS_DIR.glob("*.json"))
        if not candidates:
            print(f"error: no result files in {RESULTS_DIR}")
            return 1
        results_path = candidates[-1]

    results = json.loads(results_path.read_text())
    results["_source"] = results_path.name
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(render(results))
    print(f"wrote {args.out} from {results_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
