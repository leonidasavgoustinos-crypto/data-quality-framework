#!/usr/bin/env bash
# =============================================================================
# deploy.sh <target> [extra --var flags...]
#
# One-command full deploy: validate -> deploy -> run dq_setup.
# Run from the bundle root (bundles/dq_dashboard/).
#
# Examples:
#   ./scripts/deploy.sh dev
#   ./scripts/deploy.sh test --var notification_email=team@company.com
#   DATABRICKS_HOST=https://... DATABRICKS_TOKEN=... ./scripts/deploy.sh prod
# =============================================================================
set -euo pipefail

TARGET="${1:-dev}"
shift || true

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUNDLE_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$BUNDLE_ROOT"

echo "===  Validate  ==="
databricks bundle validate --target "$TARGET" "$@"

echo "===  Deploy  ==="
databricks bundle deploy --target "$TARGET" "$@"

echo "===  Run dq_setup  ==="
databricks bundle run dq_setup --target "$TARGET"

echo "===  Done  ==="
