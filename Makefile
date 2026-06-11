.PHONY: build test lint fmt clean

build:
	cargo build --release

test:
	cargo test

lint:
	cargo clippy -- -D warnings
	cargo fmt --check

fmt:
	cargo fmt

clean:
	cargo clean
