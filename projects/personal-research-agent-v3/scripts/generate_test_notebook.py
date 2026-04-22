#!/usr/bin/env python3
"""Generate a Jupyter notebook analysis from Personal Research Agent debug artifacts."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any


try:
    import nbformat as nbf
except ModuleNotFoundError:
    nbf = None


def load_json(path: Path) -> tuple[dict[str, Any] | None, str | None]:
    try:
        return json.loads(path.read_text(encoding="utf-8")), None
    except FileNotFoundError:
        return None, f"Missing file: {path.name}"
    except json.JSONDecodeError as exc:
        return None, f"Invalid JSON in {path.name}: {exc}"


def load_debug_data(debug_dir: Path) -> tuple[dict[str, dict[str, Any]], list[str]]:
    files: dict[str, dict[str, Any]] = {}
    warnings: list[str] = []
    for path in sorted(debug_dir.glob("*.json")):
        data, warning = load_json(path)
        if warning:
            warnings.append(warning)
        elif data is not None:
            files[path.name] = data

    expected = ["01_input.json", "02_validator.json", "02_output.json", "final_output.json"]
    for name in expected:
        if name not in files:
            warnings.append(f"Expected artifact not found: {name}")
    return files, warnings


def payload(data: dict[str, Any] | None) -> dict[str, Any]:
    if not data:
        return {}
    inner = data.get("payload", data)
    return inner if isinstance(inner, dict) else {}


def first_present(*values: Any, default: Any = None) -> Any:
    for value in values:
        if value not in (None, "", [], {}):
            return value
    return default


def summarize(debug_dir: Path, files: dict[str, dict[str, Any]], warnings: list[str]) -> dict[str, Any]:
    input_payload = payload(files.get("01_input.json"))
    validator_payload = payload(files.get("02_validator.json"))
    output_payload = payload(files.get("02_output.json"))
    final_payload = payload(files.get("final_output.json"))
    trace_payload = final_payload.get("trace_payload", {}) if isinstance(final_payload.get("trace_payload"), dict) else {}

    reason_counts = first_present(
        validator_payload.get("reason_counts"),
        trace_payload.get("reason_counts"),
        default={},
    )
    selected_counts = first_present(
        output_payload.get("selected_counts"),
        trace_payload.get("selected_counts"),
        default={},
    )
    quality_gate = first_present(output_payload.get("quality_gate_status"), trace_payload.get("quality_gate_status"), default={})

    context = files.get("final_output.json", {}).get("context", {})
    run_id = first_present(context.get("run_id"), trace_payload.get("run_id"), debug_dir.name, default=debug_dir.name)

    return {
        "run_id": run_id,
        "debug_dir": str(debug_dir),
        "user_request": input_payload.get("user_request", "No input request artifact was found."),
        "candidate_count": int(validator_payload.get("candidate_count", 0) or 0),
        "valid_count": int(validator_payload.get("valid_count", 0) or 0),
        "rejected_count": int(validator_payload.get("rejected_count", 0) or 0),
        "reason_counts": reason_counts,
        "selected_counts": selected_counts,
        "quality_gate": quality_gate,
        "validated_preview": validator_payload.get("validated_preview", []),
        "rejected_preview": validator_payload.get("rejected_preview", []),
        "report_len": output_payload.get("report_len", len(final_payload.get("final_report", "") or "")),
        "newsletter_len": output_payload.get("newsletter_len", len(final_payload.get("final_newsletter", "") or "")),
        "warnings": warnings,
    }


def md_cell(text: str) -> dict[str, Any]:
    if nbf is not None:
        return nbf.v4.new_markdown_cell(text)
    return {"cell_type": "markdown", "metadata": {}, "source": text}


def code_cell(text: str) -> dict[str, Any]:
    if nbf is not None:
        return nbf.v4.new_code_cell(text)
    return {"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [], "source": text}


def new_notebook(cells: list[dict[str, Any]]) -> dict[str, Any]:
    if nbf is not None:
        return nbf.v4.new_notebook(cells=cells)
    return {
        "cells": cells,
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python", "pygments_lexer": "ipython3"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def write_notebook(notebook: dict[str, Any], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    if nbf is not None:
        with output.open("w", encoding="utf-8") as handle:
            nbf.write(notebook, handle)
        return
    output.write_text(json.dumps(notebook, indent=2), encoding="utf-8")


def markdown_table(mapping: dict[str, Any], empty_label: str) -> str:
    if not mapping:
        return empty_label
    lines = ["| Item | Count |", "| --- | ---: |"]
    for key, value in sorted(mapping.items(), key=lambda item: str(item[0])):
        lines.append(f"| {key} | {value} |")
    return "\n".join(lines)


def build_notebook(summary: dict[str, Any], style_text: str, top_n: int) -> dict[str, Any]:
    warnings = summary["warnings"]
    warning_text = "\n".join(f"- {warning}" for warning in warnings) if warnings else "- No missing or invalid debug artifacts detected."
    reason_table = markdown_table(summary["reason_counts"], "No rejection reasons were recorded.")
    selected_table = markdown_table(summary["selected_counts"], "No selected category counts were recorded.")
    quality_status = summary["quality_gate"].get("status", "unknown") if isinstance(summary["quality_gate"], dict) else "unknown"

    cells = [
        md_cell(f"# Personal Research Agent Test Analysis: {summary['run_id']}"),
        md_cell(
            "## Introduction\n\n"
            "This notebook summarizes a Personal Research Agent debug run. "
            "It focuses on deterministic evidence: input request, validation counts, rejection reasons, selected categories, and output lengths."
        ),
        md_cell(
            "## Setup\n\n"
            f"- Debug directory: `{summary['debug_dir']}`\n"
            "- Regenerate this notebook with `python3 scripts/generate_test_notebook.py`.\n"
            "- Install project requirements before running the analysis code cells."
        ),
        md_cell(
            "## Data Overview\n\n"
            f"Input request: {summary['user_request']}\n\n"
            f"- Candidate count: {summary['candidate_count']}\n"
            f"- Valid count: {summary['valid_count']}\n"
            f"- Rejected count: {summary['rejected_count']}\n"
            f"- Quality gate status: `{quality_status}`\n"
            f"- Report length: {summary['report_len']}\n"
            f"- Newsletter length: {summary['newsletter_len']}\n\n"
            "### Selected Counts\n\n"
            f"{selected_table}\n\n"
            "### Rejection Reasons\n\n"
            f"{reason_table}"
        ),
        md_cell("## Artifact Warnings\n\n" + warning_text),
        code_cell(
            "import json\n"
            "from pathlib import Path\n\n"
            f"DEBUG_DIR = Path({summary['debug_dir']!r})\n"
            "artifacts = {}\n"
            "for path in sorted(DEBUG_DIR.glob('*.json')):\n"
            "    with path.open('r', encoding='utf-8') as handle:\n"
            "        artifacts[path.name] = json.load(handle)\n"
            "sorted(artifacts)\n"
        ),
        md_cell(
            "## Analysis\n\n"
            "The next cells convert validation previews into tables and plot compact distributions. "
            "Large candidate lists should be summarized rather than printed in full."
        ),
        code_cell(
            "import pandas as pd\n\n"
            f"validated_preview = {json.dumps(summary['validated_preview'][:top_n], indent=2)}\n"
            f"rejected_preview = {json.dumps(summary['rejected_preview'][:top_n], indent=2)}\n"
            "validated_df = pd.DataFrame(validated_preview)\n"
            "rejected_df = pd.DataFrame(rejected_preview)\n"
            "display(validated_df.head())\n"
            "display(rejected_df.head())\n"
        ),
        code_cell(
            "import matplotlib.pyplot as plt\n"
            "import pandas as pd\n\n"
            f"reason_counts = {json.dumps(summary['reason_counts'], indent=2, sort_keys=True)}\n"
            f"selected_counts = {json.dumps(summary['selected_counts'], indent=2, sort_keys=True)}\n"
            "fig, axes = plt.subplots(1, 2, figsize=(12, 4))\n"
            "pd.Series(reason_counts).sort_values().plot(kind='barh', ax=axes[0], title='Rejection reasons')\n"
            "pd.Series(selected_counts).sort_values().plot(kind='barh', ax=axes[1], title='Selected counts')\n"
            "axes[0].set_xlabel('Count')\n"
            "axes[1].set_xlabel('Count')\n"
            "plt.tight_layout()\n"
        ),
        md_cell(
            "## Conclusion\n\n"
            f"The run processed {summary['candidate_count']} candidates and selected "
            f"{sum(Counter(summary['selected_counts']).values()) if isinstance(summary['selected_counts'], dict) else 0} final items. "
            f"The quality gate status was `{quality_status}`. Review warning and rejection reason sections before using this run as a demo sample."
        ),
        md_cell(
            "## Future Work\n\n"
            "- Compare this run with the next v3 run after Telegram-triggered execution.\n"
            "- Track repeated rejection reasons in the database feedback tables.\n"
            "- Add richer plots only when they answer a concrete validation question."
        ),
        md_cell("## Style Reference\n\n" + style_text[:1500]),
    ]
    return new_notebook(cells)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--debug-dir", required=True, help="Directory containing debug JSON artifacts.")
    parser.add_argument("--style-file", default="docs/notebook_style.md", help="Notebook style guide path.")
    parser.add_argument("--output", required=True, help="Output .ipynb path.")
    parser.add_argument("--top-n", type=int, default=10, help="Preview row limit for notebook code cells.")
    args = parser.parse_args()

    debug_dir = Path(args.debug_dir)
    style_path = Path(args.style_file)
    style_text = style_path.read_text(encoding="utf-8") if style_path.exists() else "No style guide found."

    files, warnings = load_debug_data(debug_dir)
    summary = summarize(debug_dir, files, warnings)
    notebook = build_notebook(summary, style_text, args.top_n)
    write_notebook(notebook, Path(args.output))
    print(f"notebook_generated={args.output}")
    print(f"warnings={len(warnings)}")


if __name__ == "__main__":
    main()

