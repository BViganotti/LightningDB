# Contributing to Lightning

Thank you for your interest in contributing to Lightning. This document
outlines the process for contributing code, reporting issues, and proposing
improvements.

## Getting Started

### Prerequisites

- [Rust](https://rustup.rs) (stable toolchain, 1.75 or later)
- [Git](https://git-scm.com/)
- [Maturin](https://www.maturin.rs/) (optional, for Python bindings)

### Setup

```bash
git clone https://github.com/lightning-db/lightning.git
cd lightning
cargo build --workspace
```

### Running Tests

```bash
cargo test --workspace
```

Some tests require additional setup. If a test fails, check the test
documentation for any prerequisites.

### Linting

```bash
cargo clippy --workspace -- -D warnings
cargo fmt --check
```

Install clippy and rustfmt via rustup if not already present:

```bash
rustup component add clippy rustfmt
```

## Code Style

- Follow standard Rust idioms and conventions as enforced by `cargo fmt`.
- All public items must have doc comments (`///`).
- Use `thiserror` for library error types; use `anyhow` for application-level
  error handling.
- Keep functions focused and under ~80 lines where possible.
- Prefer `parking_lot` synchronization primitives over `std::sync` equivalents
  for performance-sensitive paths.
- Avoid `unsafe` unless absolutely necessary. If used, document the safety
  invariants thoroughly.
- Write tests for all new functionality. Use `tempfile` for file system
  fixtures where applicable.
- Follow the [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/).

## Pull Request Process

1. Fork the repository and create a feature branch from `main`.
2. Ensure your changes pass all checks:
   ```bash
   cargo build --workspace
   cargo test --workspace
   cargo clippy --workspace -- -D warnings
   cargo fmt --check
   ```
3. Update or add tests to cover your changes.
4. Update documentation if your changes affect the public API.
5. If your change is user-facing, add an entry to `CHANGELOG.md` under the
   `[Unreleased]` section.
6. Submit a pull request against the `main` branch.
7. Ensure CI passes. Address any review feedback.

### Commit Messages

Follow the [Conventional Commits](https://www.conventionalcommits.org/)
specification:

- `feat:` — new feature
- `fix:` — bug fix
- `docs:` — documentation only
- `refactor:` — code change that neither fixes a bug nor adds a feature
- `test:` — adding or updating tests
- `chore:` — build process or tooling changes
- `perf:` — performance improvement

### Code of Conduct

Be respectful, constructive, and inclusive. Harassment of any kind will not
be tolerated.

## Reporting Issues

Use the [GitHub Issues](https://github.com/lightning-db/lightning/issues)
tracker. Include:

- A clear description of the problem.
- Steps to reproduce.
- Expected vs. actual behavior.
- Rust version (`rustc --version`) and OS details.
- A minimal reproducible example if possible.

## Security Issues

Please do not report security vulnerabilities via public issues. See
[SECURITY.md](SECURITY.md) for the responsible disclosure process.
