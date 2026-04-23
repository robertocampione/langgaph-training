# Notebook Style Guide

Generated Personal Research Agent v4 notebooks should read like a concise test report.

## Structure

1. Title with the run ID or debug folder name.
2. Introduction explaining the purpose of the run.
3. Setup with the project path, debug directory, and reproducibility notes.
4. Data overview with input, candidate counts, validation counts, and selected counts.
5. Execution showing how the debug artifacts can be loaded.
6. Analysis with tables and simple charts for rejection reasons and selected categories.
7. Conclusion with pass/fail interpretation and notable anomalies.
8. Future work with concrete next checks or directives.

## Tone

Use brief factual English. Prefer short paragraphs, explicit counts, and neutral observations. Avoid conversational filler inside generated notebooks.

## Code Cells

Code cells should be runnable after installing project requirements. Keep them small and transparent. Use pandas for tables and Matplotlib for simple bar charts.

