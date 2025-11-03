# Repository Guidelines

## Project Structure & Module Organization
- Core scripts live at the repo root (e.g., `generate_flattened_cusum_html.py`, `compare_k_parameters.py`, `generate_pcrai_from_db.py`).
- Shared helpers are in `utils/` (`database.py`, `algorithms.py`, `visualization.py`). Reuse these instead of duplicating logic.
- Data and artifacts: `input_data/` (sources), `output_data/` (HTML/PCRAI), `readings.db` and `qst_discreps.db` (SQLite). These are git-ignored.
- Additional materials: `flatten/`, `archive/`, and docs (`README.md`, `PRD.md`).

## Build, Test, and Development Commands
- Create a virtualenv (recommended): `python3 -m venv .venv && source .venv/bin/activate`.
- Quick visual check (example dataset): `python3 generate_flattened_cusum_html.py --example-dataset --k 0.2 --limit 20`.
- Compare k-values: `python3 compare_k_parameters.py --default-k 0.0 --test-k 0.3 --cusum-limit -80 --example-dataset`.
- Create flattened DB: `python3 create_flattened_database_fast.py --threshold -100`.
- Export PCRAI: `python3 generate_pcrai_from_db.py --all`.

## Coding Style & Naming Conventions
- Python 3.x, 4-space indentation, PEP 8 naming (`snake_case` for modules/functions/variables, `CapWords` for classes).
- Prefer small, pure functions; add docstrings similar to existing `utils/*`.
- Avoid new dependencies; use standard library, NumPy, SQLite, and tqdm only as needed.
- Place shared logic in `utils/`; keep scripts focused on CLI, orchestration, and reporting.

## Testing Guidelines
- No formal test suite. Validate changes with:
  - `--limit` and `--example-dataset` flags for fast runs.
  - Spot-checking specific IDs via `--ids`.
  - Reviewing generated HTML in `output_data/` and DB diffs where applicable.
- Do not modify large databases in place without a backup; prefer a copy for experiments.

## Commit & Pull Request Guidelines
- History is mixed; use concise, imperative subjects (optionally with scope), e.g., `feat(html): add LOB sanity check toggle`.
- Include a brief body listing affected scripts and flags.
- For PRs: include purpose, sample commands, screenshots of HTML where relevant, and any DB migration notes. Link issues when available.

## Security & Configuration Tips
- Do not commit databases or generated artifacts (`.gitignore` already excludes `output_data/`, `input_data/`, `readings.db`).
- Keep secrets and external DB paths out of code; pass paths via CLI flags.
- When you generate outputdata for testing, put in /output_data folder for ease of finding and git management.

