# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added
- Standard Noteable open source patterns
  - Contributing / Code of Conduct files
  - Issue templates
  - CI/CD files and noxfile syntax
- `WebsocketManager` Backend
- New extra install `-E websockets`, additionally a convenience `-E all` option
- `WebsocketManager` saves response headers on connect

### Changed
- Use `managed_service_fixtures` for Redis tests
- `WebsocketManager` backend uses vanilla `logging` instead of `structlog`, remove need for `structlog` dependency once `managed-service-fixtures` also drops it

## [0.2.2] - 2022-07-28
### Changed
- Debug logs now contain the qualified name of callbacks

### Fixed
- System events are now opt-in only for callbacks through the `on_system_event` kwarg

## [0.2.1] - 2022-07-26
### Fixed
- Callback decorator supports system events

## [0.2.0] - 2022-07-26
### Added
- New backend for Jupyter kernels
- Shorthand for callback predicates on message topic
- Shorthand for callback predicates on system events
- Added the ability to publish system events to callbacks that opt-in to it

### Changed
- Created a separate method to get a DetachedPubSubSession
- Callbacks are concurrently delegated to all at once, instead of in batches

### Removed
- Dependency on `prometheus-client`

### Quality
- Set up nox

## [0.1.1] - 2021-10-15
### Added
- The ability to create isolated pubsub sessions that do not receive manager-level subscription messages

### Fixed
- Pubsub sessions no longer receive messages subscribed to in other sessions

## [0.1.0] - 2021-09-10
### Added
- Initial project scaffolding
