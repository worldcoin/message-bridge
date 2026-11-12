# Agent guide

Read `README.md` first. It defines the bridge; every change must preserve these invariants:

- **Opaque payloads.** The bridge only ever handles client-encrypted ciphertext (`iv` + `payload`). Never inspect, transform, or log payload contents.
- **Single-use.** Requests and responses are read with `GETDEL` — one retrieval, then gone.
- **No persistence.** Everything lives in Redis with a uniform TTL (`EXPIRE_AFTER_SECONDS` in `crates/server/src/utils.rs`) and is auto-purged. Don't add durable storage or per-key lifetimes.

The bridge is a dumb relay. **New** functionality must stay agnostic to clients and environments — no client/app-specific branching, no environment-specific behavior. Two intentional, temporary exceptions already exist; don't extend them or add new ones like them:

- `app_overrides` — a per-`app_id` rollout workaround for the World ID app (`crates/server/src/routes/request/`, `crates/server/src/main.rs`).
- `PUT /request/:id` — enabled only when `ENVIRONMENT == "staging"` (`crates/server/src/routes/request/`).

## You are the owner of this codebase

You are the owner and long-term maintainer of this codebase, and its design is up to you.

- Optimize for the next reader over quick local patches.
- Read and understand the surrounding code before changing it.
- Fix root causes instead of papering over symptoms.
- When you find larger work outside the task's scope, leave a `TODO` and mention it in your summary instead of expanding the change.
- Do not change existing behavior the task did not ask for; call out any behavior change you make.
- List every change in your summary, including small ones.
- Push back when a request is wrong or a better approach exists.

## Build, test, lint

Reproduce CI locally before pushing:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features # warnings denied via crate-level attributes
cargo build --workspace --locked
cargo test --workspace --all-features # in-process; needs only Redis — see README "Testing"
```

## Code style

Write readable, self-explanatory code.

### Comments

Keep non-doc comments to a minimum. Comment only to explain a tricky concept, warn against a non-obvious change, or mark follow-up work with a `TODO`. Link the issue or PR when one exists:

```rust
// TODO: Temporary workaround, remove once https://github.com/org/repo/pull/123 is merged
```

Describe only the current state of the code. Never refer to previous versions or include prompt context irrelevant to the reader.

### The Boy Scout rule

Leave touched code better than you found it, within the scope of the task. Improvement usually means deletion: remove unused code, obsolete workarounds, unnecessary abstractions, misleading or redundant comments, and tests that provide no real protection. Bad code does not deserve to survive just because it exists. Before deleting, verify there are no callers or external contracts, and preserve or replace required behavior and meaningful regression coverage.

### Stepdown rule

Order definitions so files read top to bottom, with callers before callees:

```rust
// BAD
fn bar() {}
fn foo() { bar(); }

// GOOD
fn foo() { bar(); }
fn bar() {}
```
