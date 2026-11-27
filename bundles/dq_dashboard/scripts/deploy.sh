#!/usr/bin/env bash
# =============================================================================
# deploy.sh <target> [extra --var flags...]
#
# One-command full deploy: render dashboard -> validate -> deploy -> run dq_setup.
# Run from anywhere; the script moves to the bundle root (bundles/dq_dashboard/).
#
# The dashboard JSON holds the token __DASHBOARD_URL__ for its own links. This
# script writes a rendered copy to build/ with the URL of THIS deploy's
# dashboard. On the first deploy of a target the dashboard does not exist yet,
# so the script deploys, reads the new id, renders again and deploys once more.
#
# VALIDATE_ONLY=1 ./scripts/deploy.sh test   -> render + validate only (CI on feature branches)
#
# Examples:
#   ./scripts/deploy.sh dev
#   ./scripts/deploy.sh test --var notification_email=team@company.com
# =============================================================================
set -euo pipefail

TARGET="${1:-dev}"
shift || true

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUNDLE_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$BUNDLE_ROOT"

command -v jq >/dev/null || { echo "jq is required (https://jqlang.github.io/jq/)"; exit 1; }

SRC="src/dashboards/data_quality.lvdash.json"
OUT="build/data_quality.lvdash.json"
mkdir -p build

# Print the URL of this target's deployed dashboard, or nothing if it does not exist yet.
dashboard_url() {
  local json host id
  json="$(databricks bundle summary --target "$TARGET" "$@" -o json 2>/dev/null || true)"
  [ -n "$json" ] || return 0
  host="$(jq -r '.workspace.host // empty' <<<"$json")"
  id="$(jq -r '.resources.dashboards.data_quality.id // empty' <<<"$json")"
  if [ -n "$host" ] && [ -n "$id" ]; then
    echo "${host%/}/sql/dashboardsv3/${id}"
  fi
}

# Write build/ with the given URL ("#" until the dashboard exists).
render() {
  local url="${1:-#}"
  sed "s|__DASHBOARD_URL__|${url}|g" "$SRC" > "$OUT"
}

URL="$(dashboard_url "$@")"
render "$URL"

echo "===  Validate  ==="
databricks bundle validate --target "$TARGET" "$@"
if [ "${VALIDATE_ONLY:-0}" = "1" ]; then
  echo "===  Validate only: done  ==="
  exit 0
fi

echo "===  Deploy  ==="
databricks bundle deploy --target "$TARGET" "$@"

if [ -z "$URL" ]; then
  echo "===  First deploy of '$TARGET': setting the dashboard links  ==="
  URL="$(dashboard_url "$@")"
  if [ -z "$URL" ]; then
    echo "Could not read the dashboard id from 'bundle summary'. Links stay '#'." >&2
  else
    render "$URL"
    databricks bundle deploy --target "$TARGET" "$@"
  fi
fi

echo "===  Run dq_setup  ==="
databricks bundle run dq_setup --target "$TARGET" "$@"

echo "===  Done  ==="
