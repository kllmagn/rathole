#!/bin/sh
set -eu

if [ "$(uname -s)" != Linux ]; then
    printf '%s\n' 'Run this regression script on Linux, not the disk-constrained local Mac.' >&2
    exit 2
fi

source_root=$(CDPATH= cd "$(dirname "$0")/.." && pwd)
red_dir=$(mktemp -d "${TMPDIR:-/tmp}/rathole-review-red.XXXXXX")
trap 'rm -rf "$red_dir"' EXIT HUP INT TERM

tar -C "$source_root" --exclude='./target' --exclude='./.git' -cf - . |
    tar -C "$red_dir" -xf -
cd "$red_dir"
git apply --check docs/tcp-pool-review-red.patch
git apply docs/tcp-pool-review-red.patch

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$source_root/target}/review-red"
export CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
export CARGO_TERM_COLOR=never

run_red() {
    filter=$1
    log="$red_dir/$filter.log"
    if cargo test -j2 --locked --no-default-features --features embedded --lib "$filter" -- --nocapture > "$log" 2>&1; then
        cat "$log"
        printf 'Expected a behavioral failure for %s, but tests passed.\n' "$filter" >&2
        exit 1
    fi
    cat "$log"
    if ! grep -q '^test result: FAILED\.' "$log"; then
        printf 'No behavioral RED result for %s; compilation/environment failure is not RED.\n' "$filter" >&2
        exit 1
    fi
}

run_red pending_creation_commands_stay_bounded_across_expiry_cycles
run_red control_write_tests
run_red stalled_backend_connect_has_its_own_deadline
printf '%s\n' 'All review regressions failed behaviorally on the restored faulty paths. Run GREEN on the unchanged source tree.'
