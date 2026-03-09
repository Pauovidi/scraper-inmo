# AGENTS Instructions

## Working Mode
- Work with full autonomy by default.
- Do not request confirmation for file edits, command runs, or local validations.
- Ask for user input only when a hard blocker requires external data that cannot be inferred from the repository.

## Engineering Priorities
- Preserve useful existing behavior.
- Keep changes small, reversible, and non-destructive.
- Prioritize robustness, traceability, and simplicity over speculative architecture.
- Avoid destructive changes that are not strictly necessary.

## Baseline and Evolution Path
- Treat the current legacy scraper as the functional baseline.
- Do not rewrite the baseline in one step.
- Evolve incrementally toward a modular layout:
  - `collectors/`
  - `archiver/`
  - `parsers/`
  - `exporters/`

## Documentation Discipline
- Keep `README.md` aligned with actual implemented behavior.
- Keep setup and execution commands updated as the project evolves.
- Explicitly document what is implemented vs planned.
