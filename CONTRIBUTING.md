# Contributing to PipelineX

## Quickstart for development

```bash
git clone <this-repo>
cd pipelinex
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# Get the datasets (~2.2 GB, one-time)
bash scripts/download_datasets.sh

# Run the full test suite
pytest

# Lint and type-check
ruff check src/ tests/
mypy src/
```

The CI matrix runs Python 3.11 and 3.12 against `ruff check`, `mypy
--strict`, and the unit + property test suites. Integration tests
against PostgreSQL only run when `POSTGRES_DSN` is set in the
environment, so local laptop tests are always fast.

## Where to find what

- `src/pipelinex/core/` — interfaces, models, pipeline executor, config
  loader, builder. **No concrete implementations live here.**
- `src/pipelinex/stages/` — concrete stages (parsers, validators,
  enrichers, sessionizer, decorators). Group is by *kind*, not by use case.
- `src/pipelinex/detectors/` — point + sequence detectors and the
  shared factory.
- `src/pipelinex/persistence/` — repository implementations.
- `src/pipelinex/api/` — FastAPI app and routes.
- `src/pipelinex/events/` — EventBus + event types + listeners.
- `tests/unit/` — fast tests (run on every push).
- `tests/integration/` — tests that need docker/postgres.
- `tests/property/` — Hypothesis-based robustness tests.
- `scripts/` — operational scripts (download, train, eval, benchmark).

## How to add things

See `docs/extending-pipelinex.md` for parser, validator, detector, and
listener walkthroughs. The short version: each kind has an interface in
`core/interfaces.py` and a factory in its module's `factory.py`. Add a
class, register it in the factory, and you're done — no core changes.

## Code style

- We use **mypy strict** mode. New code must type-check.
- We use **ruff** for linting and formatting.
- Public functions get one-line docstrings explaining contract; details
  go in the module docstring.
- Tests get full names (`test_outlier_above_fences_fires`, not
  `test_outlier_1`). Future-you reading a failure log will thank you.

## Commit hygiene

- One topic per commit. "Add CUSUMDetector + tests" is fine; "Add CUSUM,
  fix typo in README, refactor builder" is not.
- The first line is the summary; body explains *why*, not *what* (the
  diff is the *what*).
- Fixes for hook failures should be a new commit, not an `--amend`.

## Tests required for a PR

- New core classes: at least one happy-path + one failure-mode unit test.
- New parsers: oracle test against a real sample if available, plus
  `test_can_parse_never_raises` property test.
- New detectors: warmup-period test, one happy-path firing, one
  no-fire test on inliers, `reset()` clears state.
- Anything touching the executor: a property-test sanity check that the
  records-conservation invariant still holds.
