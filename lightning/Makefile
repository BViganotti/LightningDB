.PHONY: build test lint fmt python-build clean

build:
	cargo build --release

test:
	cargo test

lint:
	cargo clippy -- -D warnings
	cargo fmt --check

fmt:
	cargo fmt

python-build:
	maturin build --release
	@echo "Python package built in target/wheels/"

clean:
	cargo clean
