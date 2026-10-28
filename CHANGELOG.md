# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0](https://github.com/worldcoin/message-bridge/releases/tag/v0.1.0) - 2026-10-28

### Added

- add the message-bridge-client crate ([#137](https://github.com/worldcoin/message-bridge/pull/137))
- expose a reusable test server ([#136](https://github.com/worldcoin/message-bridge/pull/136))
- make analytics callbacks optional ([#129](https://github.com/worldcoin/message-bridge/pull/129))
- report anonymized proof response fetches ([#117](https://github.com/worldcoin/message-bridge/pull/117))
- update documentation & start rename to message-bridge ([#106](https://github.com/worldcoin/message-bridge/pull/106))
- server-driven per-app URL overrides on POST /request ([#98](https://github.com/worldcoin/message-bridge/pull/98))

### Fixed

- answer CORS preflight on /request/{id} when the staging PUT route is enabled ([#143](https://github.com/worldcoin/message-bridge/pull/143))
- gate app_overrides behind opt-in flag on POST /request ([#99](https://github.com/worldcoin/message-bridge/pull/99))
- rustls needs an explicit provider ([#61](https://github.com/worldcoin/message-bridge/pull/61))
- expiring incorrect key ([#6](https://github.com/worldcoin/message-bridge/pull/6))

### Other

- publish Rust crates with release-plz and trusted publishing ([#138](https://github.com/worldcoin/message-bridge/pull/138))
- update axum 0.8, aide 0.15 & related ([#132](https://github.com/worldcoin/message-bridge/pull/132))
- organize server in a shared Cargo workspace ([#135](https://github.com/worldcoin/message-bridge/pull/135))
- Add integration testing infrastructure ([#83](https://github.com/worldcoin/message-bridge/pull/83))
- Add standalone response flow ([#81](https://github.com/worldcoin/message-bridge/pull/81))
- Update README.md ([#45](https://github.com/worldcoin/message-bridge/pull/45))
- Status handling improvements ([#3](https://github.com/worldcoin/message-bridge/pull/3))
- Implement Bridge Spec (v1)
- Initial commit
