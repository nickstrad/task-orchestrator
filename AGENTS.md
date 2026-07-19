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

## Reporting changes

When I ask you to change code or fix a bug, report the result as a list of findings, each
one a vertically stacked before/after snippet with a concise explanation:

````markdown
## 1. Short title naming the defect

```go
// before
copy = slices.Delete(copy, idx, idx)

// after
m.WorkerTaskMap[worker] = slices.Delete(assigned, idx, idx+1)
```

`Delete(s, i, j)` removes the half-open range `s[i:j]`, so `i == j` was a silent no-op —
the task stayed in the worker's list forever.
````

- **Stack before over after**, never side by side. Long lines wrap badly in a terminal.
- **Snippets are excerpts, not whole functions.** Show the few lines that changed plus
  just enough context to place them.
- **One numbered finding per defect**, ordered most to least severe, each with a title
  that names the defect rather than the fix.
- **Explain the consequence, not the edit.** The diff already shows what changed; say
  what went wrong at runtime and why. Point out when a fix is untestable, unreachable,
  or silent.
- **Flag behavior changes separately** and say which one to push back on if I disagree.
- Group trivia (typos, constants, renames) into one final block rather than numbering
  each one.

## Commits

- **Never commit unless I explicitly ask you to.** Leave finished work in the working
  tree and tell me what changed; I decide when it gets committed.
- **Never merge to `main` unless I explicitly ask you to.** This includes fast-forwards
  and merges of a plan's worktree branch.
- Asking once does not stand as permission for later commits — wait for it each time.
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
