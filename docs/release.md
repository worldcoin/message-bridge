# Release Rust crates

The workspace version is shared by `message-bridge` and `message-bridge-client`.
The server crate includes the production router, executable and optional
`test-util` module; the client has no server dependency outside its tests.

`.github/workflows/release-crates.yml` runs release-plz on pushes to `main`:

1. The release-PR job proposes a version bump and a combined root `CHANGELOG.md`.
2. A maintainer reviews and merges the release PR.
3. The publish job publishes both crates through crates.io trusted publishing and
   creates the `vX.Y.Z` GitHub release and tag. `release_always = false` limits
   publishing to merged release PRs.

The server owns the shared changelog and tag; the client does not create a second
release. The existing `.github/workflows/release.yml` continues to build and push
Docker images on `main` and version tags. The bot token allows release PRs and
version tags to trigger CI and Docker workflows.

## One-time setup

A repository/crate owner configures the following before enabling releases:

- Create the GitHub `production` environment and its desired deployment protections.
- Set `MESSAGE_BRIDGE_BOT_TOKEN` to a GitHub bot token with repository contents and
  pull-request write permissions. This is GitHub authentication, not a crates.io token.
  The default `GITHUB_TOKEN` would suppress CI on bot-created release PRs and the
  Docker workflow on bot-created tags.
- Publish the first versions manually: `message-bridge` first, followed by
  `message-bridge-client` (its integration-test dev-dependency uses the server).
  Both start at the version in `[workspace.package]`. Trusted publishing cannot
  create a new crate.
- In the settings of **each** crate on crates.io, add a GitHub trusted publisher:
  owner `worldcoin`, repository `message-bridge`, workflow filename
  `release-crates.yml`, environment `production`.

There is no `CARGO_REGISTRY_TOKEN` secret or separate crates.io auth action. The
publish job grants `id-token: write`; release-plz exchanges its OIDC identity for
short-lived registry credentials. No crates are published by pull-request CI.

For a failed publish, rerun the original failed workflow run at the release merge
commit. Manual runs on `main` run only the publish job and require its tip to still
belong to a release PR; otherwise release-plz skips publishing. Already published
versions are skipped. A published version is immutable: yank a bad version and
prepare a new release rather than replacing it.

The workflow follows the release-plz setups in
[world-id-protocol](https://github.com/worldcoin/world-id-protocol/blob/0db4150f1dbd85cb208dd06304fd023c56b58d41/.github/workflows/release-crates.yml)
and [WalletKit](https://github.com/worldcoin/walletkit/blob/aac0e59c286e85b3e4a9706f2a4e5653c769f9b6/.github/workflows/release.yml).
See the [release-plz trusted-publishing guide](https://release-plz.dev/docs/github/quickstart)
and [crates.io trusted publishing](https://crates.io/docs/trusted-publishing) for
publisher configuration.
