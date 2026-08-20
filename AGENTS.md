# Project Engineering Instructions

## Mandatory Development Workflow

For every non-trivial feature, architectural change, behavior change,
refactoring, or bug fix, follow this workflow.

### Phase 1: Design

Use the Superpowers `brainstorming` skill.

Before writing production code:

1. Inspect the existing codebase.
2. Understand the current architecture.
3. Understand requirements and constraints.
4. Identify affected components.
5. Consider edge cases and failure modes.
6. Consider 2-3 possible approaches when appropriate.
7. Explain trade-offs.
8. Recommend one approach.
9. Produce a technical design.

Do NOT modify production code during this phase.

Save the design under:

`docs/superpowers/specs/YYYY-MM-DD-<feature>-design.md`

STOP after the design.

Wait for explicit user approval before creating the implementation plan.


### Phase 2: Implementation Plan

After the design has been approved, use the Superpowers
`writing-plans` skill.

Produce a detailed implementation plan.

The plan must contain:

- exact files to create
- exact files to modify
- classes/functions/interfaces to add
- database changes
- migrations
- tests
- integration points
- error handling
- logging/observability changes
- verification commands

Break work into small independently verifiable tasks.

For each behavior change use:

1. write failing test
2. run test and verify failure
3. implement minimal code
4. run test and verify success
5. refactor
6. run tests again

Save implementation plans under:

`docs/superpowers/plans/YYYY-MM-DD-<feature>.md`

Do NOT implement the plan yet.

STOP after producing the plan.

Wait for explicit user approval before implementation.


### Phase 3: Implementation

Only after explicit approval of the implementation plan may production code
be changed.

Prefer the Superpowers `subagent-driven-development` skill.

If it is not appropriate or unavailable, use `executing-plans`.

Implement the approved plan task-by-task.

Do not silently change architecture or requirements.

If implementation reveals that the approved design is incorrect,
stop implementation and explain the problem before changing the design.


## Test-Driven Development

Use the Superpowers `test-driven-development` skill.

Strictly follow:

RED -> GREEN -> REFACTOR

For every new behavior:

### RED

Write the smallest meaningful failing test first.

Run it.

Confirm that it fails for the expected reason.

### GREEN

Write only enough production code to make the test pass.

Run the test.

Confirm it passes.

### REFACTOR

Improve code structure without changing behavior.

Run relevant tests again.

Never write production behavior before its failing test.

Bug fixes must start with a regression test reproducing the bug.


## Debugging

For non-trivial bugs use the Superpowers `systematic-debugging` skill.

Do not randomly modify code trying possible fixes.

First:

1. reproduce the problem
2. collect evidence
3. identify the root cause
4. form a hypothesis
5. test the hypothesis
6. create a regression test
7. implement the smallest correct fix
8. verify the fix


## Verification

Before claiming that work is finished, use the Superpowers
`verification-before-completion` skill.

Never claim success based only on reasoning.

Run fresh verification.

At minimum verify:

- relevant unit tests
- integration tests
- full test suite when practical
- type checking if configured
- linting if configured
- formatting if configured
- migrations if changed
- application startup when relevant

Report the actual commands executed and their results.


## Git

Do not implement non-trivial changes directly on `main` or `master`.

Use a feature branch or the Superpowers `using-git-worktrees` workflow.

Keep commits focused.

Do not combine unrelated refactoring with feature implementation.


# Engineering Principles

Prefer simple and explicit solutions.

Apply:

- Clean Code
- SOLID where it provides concrete value
- dependency inversion at infrastructure boundaries
- composition over inheritance
- clear interfaces between components
- explicit dependencies
- small cohesive functions and classes
- separation of concerns
- testable architecture
- deterministic behavior
- idempotency where applicable

Avoid:

- unnecessary abstractions
- speculative extensibility
- premature optimization
- god classes
- hidden global state
- circular dependencies
- large unrelated refactors
- clever code when simple code works

Follow YAGNI.

Follow DRY when duplication represents the same concept.

Do not create abstractions merely to remove a few repeated lines.


# Python

Use the project's existing virtual environment:

`venv/`

Do not install project dependencies globally.

Prefer commands from the virtual environment, for example:

`venv/bin/python`
`venv/bin/pytest`
`venv/bin/alembic`

Follow the existing Python version and dependencies defined by the project.

Use type annotations for public interfaces and non-trivial internal APIs.

Prefer explicit domain types over dictionaries when the structure has
meaningful behavior.


# Architecture

Keep business logic independent from infrastructure when practical.

Prefer boundaries such as:

- API / transport
- application services
- domain logic
- repositories
- infrastructure
- persistence

External systems such as PostgreSQL, HTTP clients, filesystem access,
and third-party APIs should be behind clear interfaces where doing so
improves testability or replacement cost.

Do not introduce abstractions without a concrete reason.


# Database

Use PostgreSQL as the persistence implementation.

Use Alembic for schema migrations.

Schema changes must include:

- migration
- model changes
- repository changes where necessary
- tests

Never rely on application startup to silently mutate the database schema.


# Observability

Important operations must be observable.

Prefer structured logging.

Logs should provide enough context to understand:

- what happened
- where it happened
- which operation/request/job was affected
- why it failed

Do not log secrets or sensitive credentials.

Exceptions should preserve useful context and stack traces.


# Final Completion Gate

A feature is complete only when:

1. approved design exists
2. approved implementation plan exists
3. implementation matches the plan
4. tests cover new behavior
5. tests have been executed
6. verification has been executed
7. no known critical issue remains

If any of these conditions is false, do not claim the feature is complete.
