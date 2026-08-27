#!/usr/bin/env bash
# run_tests.sh — run the workshop test suite
#
# Usage:
#   ./run_tests.sh           # unit tests only (no external services needed)
#   ./run_tests.sh --all     # unit + integration tests (requires live Kafka)
#   ./run_tests.sh --lint    # lint only
#   ./run_tests.sh --full    # lint + unit + integration

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colours
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

mode="${1:-}"

run_lint() {
    echo -e "${YELLOW}==> Lint${NC}"
    uv run ruff check producer/ mcp_ingestion_server/ tests/
    echo -e "${GREEN}    Lint passed${NC}"
}

run_unit() {
    echo -e "${YELLOW}==> Unit tests${NC}"
    uv run pytest -m "not integration" -v
}

run_integration() {
    echo -e "${YELLOW}==> Integration tests (requires live Kafka)${NC}"
    # Warn if broker env var looks like the default placeholder
    bootstrap="${KAFKA_BOOTSTRAP_SERVERS:-}"
    if [[ -z "$bootstrap" || "$bootstrap" == "localhost:9092" ]]; then
        echo -e "${RED}    WARNING: KAFKA_BOOTSTRAP_SERVERS not set or is localhost:9092"
        echo -e "    Make sure mcp_ingestion_server/.env has real broker credentials.${NC}"
    fi
    uv run pytest -m "integration" -v
}

case "$mode" in
    --all)
        run_unit
        run_integration
        ;;
    --lint)
        run_lint
        ;;
    --full)
        run_lint
        run_unit
        run_integration
        ;;
    "")
        run_unit
        ;;
    *)
        echo "Usage: $0 [--all | --lint | --full]"
        exit 1
        ;;
esac

echo -e "${GREEN}==> Done${NC}"
