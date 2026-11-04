# Agent guide

Read `README.md` first. It defines the bridge; every change must preserve these invariants:

- **Opaque payloads.** The bridge only ever handles client-encrypted ciphertext (`iv` + `payload`). Never inspect, transform, or log payload contents.
- **Single-use.** Requests and responses are read with `GETDEL` — one retrieval, then gone.
- **No persistence.** Redis data auto-expires: request/response/status use `EXPIRE_AFTER_SECONDS` (`utils.rs`); session reservation/metadata use `SESSION_TTL_SECONDS` (`session.rs`). Normal operations preserve the reservation deadline; staging reset may extend it. Don't add durable storage or other per-key lifetimes.

The bridge is a dumb relay. **New** functionality must stay agnostic to clients and environments — no client/app-specific branching, no environment-specific behavior. Two intentional, temporary exceptions already exist; don't extend them or add new ones like them:

- `app_overrides` — a per-`app_id` rollout workaround for the World ID app (`crates/server/src/routes/request/`, `crates/server/src/main.rs`).
- `PUT /request/:id` — enabled only when `ENVIRONMENT == "staging"` (`crates/server/src/routes/request/`).

## Build, test, lint

Reproduce CI locally before pushing:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features # warnings denied via crate-level attributes
cargo build --workspace --locked
cargo test --workspace --all-features # in-process; needs only Redis — see README "Testing"
```
