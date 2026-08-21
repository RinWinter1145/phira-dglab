#!/usr/bin/env bash
# Build Phira for desktop Linux — development / ad-hoc testing helper.
#
# Usage:
#   ./scripts/build-linux.sh                    debug build (fast, default)
#   ./scripts/build-linux.sh -r|--release       release build (opt-level 2, stripped)
#   ./scripts/build-linux.sh --check            type-check only (quickest)
#   ./scripts/build-linux.sh --run              build, then launch the game
#   ./scripts/build-linux.sh --target <triple>  build for a specific target triple
#   ./scripts/build-linux.sh -h|--help          show this help
#
# Env:
#   CARGO_HOME    cargo cache dir (default: <repo>/.cargo-home if present,
#                 else ~/.cargo; falls back to a freshly created .cargo-home)
#   CARGO_EXTRA   extra arguments appended to every cargo invocation
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ---------- options ----------
PROFILE="dev"
MODE="build"        # build | check
RUN_AFTER=false
TARGET=""
CARGO_ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        -r|--release) PROFILE="release" ;;
        --check) MODE="check" ;;
        --run) RUN_AFTER=true ;;
        --target) TARGET="${2:?--target needs an argument}"; shift ;;
        -h|--help)
            sed -n '2,20p' "$0"
            exit 0
            ;;
        *)
            echo "unknown option: $1" >&2
            sed -n '2,20p' "$0" >&2
            exit 2
            ;;
    esac
    shift
done

# ---------- cargo home: a writable cache is mandatory ----------
# Priority: project-local cache (warm on this machine) ->
#           $CARGO_HOME / ~/.cargo -> freshly created .cargo-home fallback.
if [[ -d "$ROOT/.cargo-home" ]]; then
    export CARGO_HOME="$ROOT/.cargo-home"
elif [[ -z "${CARGO_HOME:-}" ]]; then
    if mkdir -p "$HOME/.cargo/git" "$HOME/.cargo/registry" 2>/dev/null; then
        export CARGO_HOME="$HOME/.cargo"
    else
        export CARGO_HOME="$ROOT/.cargo-home"
        mkdir -p "$CARGO_HOME"
        echo "[build-linux] $HOME/.cargo is not writable; using CARGO_HOME=$CARGO_HOME"
    fi
fi

cd "$ROOT"

extras=()
[[ -n "$TARGET" ]] && extras+=(--target "$TARGET")
# shellcheck disable=SC2206
extras+=(${CARGO_EXTRA:-})
extras+=("${CARGO_ARGS[@]}")

case "$MODE" in
    check)
        echo "[build-linux] cargo check --bin phira-main (profile: $PROFILE)"
        cargo check --bin phira-main --profile "$PROFILE" "${extras[@]}"
        echo "[build-linux] check passed"
        exit 0
        ;;
    build)
        echo "[build-linux] cargo build --bin phira-main (profile: $PROFILE)"
        cargo build --bin phira-main --profile "$PROFILE" "${extras[@]}"
        ;;
esac

# ---------- resolve the produced binary ----------
case "$PROFILE" in
    dev) BIN_DIR="debug" ;;
    release) BIN_DIR="release" ;;
    *) BIN_DIR="$PROFILE" ;;
esac
if [[ -n "$TARGET" ]]; then
    bin="$ROOT/target/$TARGET/$BIN_DIR/phira-main"
else
    bin="$ROOT/target/$BIN_DIR/phira-main"
fi

if [[ ! -x "$bin" ]]; then
    echo "[build-linux] ERROR: expected binary not found: $bin" >&2
    exit 1
fi
echo "[build-linux] binary: $bin"

if $RUN_AFTER; then
    echo "[build-linux] launching (from $ROOT so assets/data resolve)..."
    cd "$ROOT"
    exec env RUST_BACKTRACE=1 "$bin"
fi