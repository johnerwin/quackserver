# Contributing to quackserver

## Before You Start

Read the scope boundary in the README. It is not aspirational — it is load-bearing. The constraints that make quackserver operable for a solo developer are the same constraints that make it possible to reason about its failure modes. A contribution that relaxes them needs a strong justification, not just a use case.

The whiteboard test: if the change makes the system harder to explain in 5 minutes, it needs a very strong justification. If you can't explain it to someone who has never seen the codebase in that time, it's probably too complex.

## What Gets Merged

- Bug fixes with a reproducing test
- Performance improvements with a benchmark that justifies the change
- New endpoints or storage methods that fit the operating envelope
- Improvements to error messages, structured logging, or observability
- Documentation corrections

## What Does Not Get Merged

- Arbitrary SQL execution endpoints
- User-managed transactions
- Background compaction
- Realtime subscriptions
- Distributed anything
- Features that require relaxing a resource limit constant without workload evidence

If you're unsure, open an issue before writing code.

## Development Setup

```bash
git clone https://github.com/johnerwin/quackserver
cd quackserver
pip install -e ".[dev]"
pytest
```

Python 3.11+ required. All 260 tests should pass before you touch anything.

## Making Changes

**One concern per PR.** A PR that fixes a bug and refactors a module is two PRs.

**Tests are required.** The test suite covers durability contracts, projection idempotency, the runtime state machine, and chaos scenarios. New behavior needs a test that would fail without the change. New failure modes need a test that exercises them.

**The chaos suite is the bar.** `tests/test_chaos.py` verifies no data loss and no duplicates across failure and restart cycles. If your change touches the write path, the log, the dedup store, or the worker, run the chaos suite explicitly and confirm it passes.

**Do not add resource limit constants without justification.** Every constant in `config.py` is named and annotated with its rationale. New constants need the same treatment.

**No comments that explain what the code does.** The codebase comments explain *why* — hidden constraints, non-obvious invariants, spec cross-references. Don't add comments that restate the code.

## Commit Messages

Short subject line (imperative mood). Body only when the why isn't obvious from the diff. Reference the spec section if the change implements or deviates from a spec requirement.

## The Spec

`DuckDB_AppServer_Spec.docx` is the source of truth for design decisions. When a PR conflicts with the spec, the spec wins unless there is a documented reason it should not.

## Opening Issues

Use the issue for a concrete problem statement — what you observed, what you expected, what workload produced it. Vague feature requests will be closed. Issues #1 (backpressure metrics), #2 (DuckDB projection), and #3 (read endpoints) are the current development priorities.
