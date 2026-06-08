#!/usr/bin/env bash
set -euo pipefail

# MIRI (MIR Interpreter) verification for LightningDB.
# This script runs MIRI on the test suite to detect Undefined Behavior.
#
# Usage:
#   ./scripts/miri_test.sh               # Run the comprehensive test
#   ./scripts/miri_test.sh --quick        # Run only the lib tests (faster)
#   ./scripts/miri_test.sh --test <name>  # Run a specific test

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CARGO_MANIFEST="$SCRIPT_DIR/../crates/lightning-core"

run_miri() {
    local test_filter="$1"
    shift

    MIRIFLAGS="-Zmiri-disable-isolation" \
        cargo +nightly miri test \
        --manifest-path "$CARGO_MANIFEST/Cargo.toml" \
        "$test_filter" \
        "$@"
}

case "${1:-}" in
    --quick)
        echo "=== MIRI: running lib tests (quick) ==="
        run_miri "" --lib
        ;;
    --test)
        shift
        echo "=== MIRI: running test '$1' ==="
        run_miri "$1"
        ;;
    *)
        echo "=== MIRI: running comprehensive test ==="
        run_miri "comprehensive_test"
        ;;
esac

echo "=== MIRI completed with no UB detected ==="
