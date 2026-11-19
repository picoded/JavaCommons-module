# Changelog

All notable changes to `JavaCommons-module` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [4.1.14] - 2026-10-21

### Fixed
- Optimized `AccountTableCore.queryCount` performance by delegating directly to `accountDataObjectMap.queryCount` to run native database count queries instead of loading record IDs into memory.
