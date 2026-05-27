.PHONY: build test lint fmt audit python-build clean

build:
	cargo build --workspace --release

test:
	cargo test --workspace

lint:
	cargo clippy --workspace -- -D warnings
	cargo fmt --check

fmt:
	cargo fmt

audit:
	cargo audit
	cargo deny check bans licenses sources

python-build:
	maturin build --release
	@echo "Python package built in target/wheels/"

clean:
	cargo clean
