# task-orchestrator

This project is a **learning exercise**. The user is building this to learn, not to ship
production software as fast as possible.

## Working guidelines

- Do not overwrite or rewrite files the user has authored unless explicitly told to do so.
  Prefer explaining what's wrong and how to fix it over jumping straight to an edit.
- Default to analysis first: when you spot an issue (bug, broken import, incomplete stub,
  design problem), explain the root cause and the options to resolve it, and wait for
  direction before changing code — unless the user has directly asked you to fix it.
- Skeleton/incomplete code (e.g. stub functions, TODO-like fragments) is expected and often
  intentional work-in-progress — don't "complete" it on your own initiative.

## Commits

- **Do not add `Co-Authored-By:` trailers or any other AI attribution signature to commit
  messages.** Write the message as a plain description of the change.

## Worktrees

When executing a plan, do the work in a git worktree rather than on `main`:

```sh
git worktree add .worktree/<branch-name> -b <branch-name> main
```

- Create every worktree inside **`.worktree/`** at the repo root. It is gitignored, so the
  checkouts never show up as untracked files.
- Name the branch after the plan being executed (e.g. `error-handling-and-logging`) and
  give the worktree directory the same name.
- **Clean up after merging to `main`** — a finished worktree left behind is a stale
  checkout that will drift:

  ```sh
  git worktree remove .worktree/<branch-name>
  git branch -d <branch-name>
  ```

  Use `git worktree list` to check for leftovers.

## Documentation

All project documentation lives in [`docs/`](docs/). **Start at
[`docs/index.md`](docs/index.md)** — it is the index and describes what every other
document is for.

The layout:

- `docs/index.md` — the index. Read this first; keep it current when adding a doc.
- `docs/*.md` — conventions and plans that describe how *this* codebase works today.
- `docs/reference/` — forward-looking reference material about where the project
  could go next. These are **not** specs for current code and describe systems that
  do not exist yet; don't implement from them unless asked.

Before writing new code, check `docs/` for a convention that already covers it — in
particular [`docs/error-handling-and-logging.md`](docs/error-handling-and-logging.md),
which every package is expected to follow. When you add a document, add a line for it
to `docs/index.md`.

`CLAUDE.md` is a symlink to this file, so both names read the same content — edit
`AGENTS.md`.
