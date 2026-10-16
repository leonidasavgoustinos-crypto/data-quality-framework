#!/usr/bin/env bash
# Deploy guard: stop unless the workspace the CLI is connected to is the target's expected_host.
#
#   bash scripts/check_host.sh <cli profile> <expected host>
#
# Called by the bundle artifact build (bundle/databricks.yml) with ${workspace.profile} and ${var.expected_host}
# (from variable-overrides.json). The host of that profile is read with `databricks auth describe`. The build runs
# before anything is uploaded, so `bundle deploy` (and `bundle plan`) stop here on the wrong workspace.
set -euo pipefail
norm() { printf '%s' "$1" | tr '[:upper:]' '[:lower:]' | sed -e 's#^http://#https://#' -e 's#/*$##'; }
profile="${1:-}"
[ -n "$profile" ] || { echo "ERROR: host check needs the CLI profile name" >&2; exit 1; }
host="$(databricks auth describe -p "$profile" -o json | python3 -I -c 'import json,sys; print(json.load(sys.stdin)["details"]["host"])')" \
  || { echo "ERROR: cannot read the host of CLI profile '$profile'" >&2; exit 1; }
actual="$(norm "$host")"; expected="$(norm "${2:-}")"
if [ -z "$actual" ] || [ -z "$expected" ] || [[ "$expected" == *"<"* ]] || [[ "$expected" == *'${'* ]]; then
  echo "ERROR: host check needs a connected host and a configured expected_host (got '$host' / '${2:-}')" >&2; exit 1
fi
if [ "$actual" != "$expected" ]; then
  echo "ERROR: connected workspace '$actual' != expected_host '$expected'. Nothing was deployed." >&2; exit 1
fi
echo "host check OK: $actual"
