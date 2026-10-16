#!/usr/bin/env bash
# Build the patched framework wheel from a read-only copy of the repo (tracked files are never touched).
#
#   bash dq_dashboard/scripts/build_patched.sh                 # default patch set, default base commit
#   PATCHES="01 02 03 04" bash dq_dashboard/scripts/build_patched.sh
#   BASE_COMMIT=9c671ac bash dq_dashboard/scripts/build_patched.sh
#
# BASE_COMMIT: the repo commit the patches are applied to. Default = source_commit of the current
# bundle/src/driver/build_manifest.json, so the build does not depend on later commits on the branch.
# Base files are read READ-ONLY with `git show $BASE_COMMIT:<path>` (no checkout, no stash: the working tree
# and the index are never touched). The build fails if the commit or any required path does not exist.
#
# Steps: check repo clean (outside dq_dashboard/) -> fresh COPY of framework/, setup.py and the files patched by 05-08
# from BASE_COMMIT into dq_dashboard/.build/src (plus an unpatched reference copy in dq_dashboard/.build/base) ->
# apply the selected patches in order with `patch -p1`
# (fails on the first patch that does not apply) -> run the local patch tests -> build the wheel with a PEP 440
# local version that names the patches -> copy it into the bundle (bundle/dist) -> write
# bundle/src/driver/build_manifest.json (version, sha256, patches, source commit) -> check repo still clean.
# Build outputs: dq_dashboard/.build/ (scratch) and dq_dashboard/bundle/dist/*.whl; both are git-ignored.
# 05 and 06 patch setup/ and configuration/ only (no runtime effect here), so they are not in the default set.
set -euo pipefail

PATCHES="${PATCHES:-01 02 03 04 07 08}"

SCRIPTS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT="$(cd "$SCRIPTS/.." && pwd)"                # dq_dashboard/
REPO="$(git -C "$SCRIPTS" rev-parse --show-toplevel)"
PROJECT_REL="${PROJECT#"$REPO"/}"
BUILD="$PROJECT/.build"
SRC="$BUILD/src"
DIST="$BUILD/dist"
BUNDLE="$PROJECT/bundle"
PATCH_DIR="$PROJECT/patches"

# The repo outside dq_dashboard/ must be clean (dq_dashboard/ itself is where this build writes its outputs).
repo_clean() {
  local st; st="$(git -C "$REPO" status --porcelain -- . ":(exclude)$PROJECT_REL")"
  if [ -n "$st" ]; then echo "ERROR: repo working tree is not clean:" >&2; echo "$st" >&2; exit 1; fi
}

die() { echo "ERROR: $*" >&2; exit 1; }

repo_clean

MANIFEST="$BUNDLE/src/driver/build_manifest.json"
if [ -z "${BASE_COMMIT:-}" ]; then
  [ -f "$MANIFEST" ] || die "BASE_COMMIT not set and $MANIFEST not found"
  BASE_COMMIT="$(python3 -I -c 'import json,sys; print(json.load(open(sys.argv[1]))["source_commit"])' "$MANIFEST")" \
    || die "cannot read source_commit from $MANIFEST"
fi
git -C "$REPO" cat-file -e "${BASE_COMMIT}^{commit}" 2>/dev/null || die "BASE_COMMIT '$BASE_COMMIT' is not a commit in $REPO"
BASE_SHORT="$(git -C "$REPO" rev-parse --short "${BASE_COMMIT}^{commit}")"
echo "base commit: $BASE_SHORT (read-only via git show; working tree and index untouched)"

REQUIRED=(setup.py setup/01_ddl.py setup/03_reporting_views.py configuration/dataset_creation.ipynb)
FRAMEWORK_FILES=()
while IFS= read -r f; do FRAMEWORK_FILES+=("$f"); done < <(git -C "$REPO" ls-tree -r --name-only "$BASE_SHORT" -- framework/)
[ "${#FRAMEWORK_FILES[@]}" -gt 0 ] || die "framework/ is missing in $BASE_SHORT"
for f in "${REQUIRED[@]}"; do
  git -C "$REPO" cat-file -e "$BASE_SHORT:$f" 2>/dev/null || die "required path '$f' is missing in $BASE_SHORT"
done

BASE="$BUILD/base"                                   # unpatched reference copy (used by the tests)
rm -rf "$SRC" "$DIST" "$BASE"
mkdir -p "$DIST" "$BUNDLE/dist"
for dest in "$SRC" "$BASE"; do
  for f in "${FRAMEWORK_FILES[@]}" "${REQUIRED[@]}"; do
    mkdir -p "$dest/$(dirname "$f")"
    git -C "$REPO" show "$BASE_SHORT:$f" > "$dest/$f" || die "git show $BASE_SHORT:$f failed"
  done
done

applied=()
for n in $PATCHES; do
  matches=("$PATCH_DIR"/${n}_*.diff)
  if [ ! -f "${matches[0]}" ] || [ "${#matches[@]}" -ne 1 ]; then echo "ERROR: need exactly one patch file for $n" >&2; exit 1; fi
  echo "applying $(basename "${matches[0]}")"
  patch -p1 --batch --forward --fuzz=0 --quiet -d "$SRC" < "${matches[0]}"
  applied+=("$(basename "${matches[0]}" .diff)")
done

# local tests on the patched copy (fail the build on any deviation)
cd "$PATCH_DIR/tests"
fw=("01" "02" "03" "04"); stage="base"
for s in "${fw[@]}"; do [[ " $PATCHES " == *" $s "* ]] && stage="$s" || break; done
python3 -I test_framework_patches.py --framework "$SRC" --stage "$stage" > "$BUILD/test_framework.log" || { cat "$BUILD/test_framework.log"; exit 1; }
other="$(echo "$PATCHES" | tr ' ' '\n' | grep -E '^0[5-8]$' | paste -sd, - || true)"
python3 -I test_other_patches.py --root "$SRC" --original "$BASE" --applied "$other" > "$BUILD/test_other.log" || { cat "$BUILD/test_other.log"; exit 1; }
echo "tests: framework stage $stage OK, other patches [$other] OK"

# version + build info inside the package
base_version="$(python3 -I -c "import re,sys; print(re.search(r'version=\"([^\"]+)\"', open(sys.argv[1]).read()).group(1))" "$SRC/setup.py")"
local_label="dqd.p$(echo "$PATCHES" | tr ' ' '\n' | paste -sd. - | sed 's/\./.p/g')"
version="${base_version}+${local_label}"
commit="$BASE_SHORT"
python3 -I - "$SRC/setup.py" "$version" <<'PY'
import re, sys
p, v = sys.argv[1], sys.argv[2]
s = open(p).read()
s2 = re.sub(r'version="[^"]+"', f'version="{v}"', s, count=1)
assert s2 != s
open(p, "w").write(s2)
PY
python3 -I - "$SRC/framework/_build_info.py" "$version" "$commit" "${applied[@]}" <<'PY'
import sys, json, datetime
path, version, commit, patches = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4:]
info = {"version": version, "source_commit": commit, "patches": patches}   # no timestamp: keeps the wheel reproducible
open(path, "w").write("# build info written by dq_dashboard/scripts/build_patched.sh, not part of the repo\nBUILD_INFO = " + json.dumps(info, indent=1) + "\n")
PY

# wheel (isolated build env provides setuptools; nothing is installed system-wide).
# SOURCE_DATE_EPOCH = BASE_COMMIT commit time unless already set -> identical inputs give an identical wheel sha256
# and an identical build_manifest.json (built_at is this timestamp, not the wall clock).
export SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-$(git -C "$REPO" log -1 --format=%ct "$BASE_SHORT")}"
find "$SRC" -exec touch -h -d "@$SOURCE_DATE_EPOCH" {} + 2>/dev/null || find "$SRC" -exec touch -t "$(date -r "$SOURCE_DATE_EPOCH" +%Y%m%d%H%M.%S)" {} +
python3 -m pip wheel --no-deps --no-cache-dir --quiet -w "$DIST" "$SRC"
rm -rf "$SRC/build" "$SRC"/*.egg-info
whl="$(ls "$DIST"/*.whl)"
sha="$(shasum -a 256 "$whl" | cut -d' ' -f1)"

rm -f "$BUNDLE/dist/"*.whl
cp "$whl" "$BUNDLE/dist/"
python3 -I - "$BUNDLE/src/driver/build_manifest.json" "$(basename "$whl")" "$version" "$sha" "$commit" "$SOURCE_DATE_EPOCH" "${applied[@]}" <<'PY'
import sys, json, datetime
path, wheel, version, sha, commit, epoch, patches = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], int(sys.argv[6]), sys.argv[7:]
json.dump({"wheel": wheel, "version": version, "sha256": sha, "source_commit": commit, "patches": patches,
           "built_at": datetime.datetime.fromtimestamp(epoch, datetime.timezone.utc).isoformat(timespec="seconds")},
          open(path, "w"), indent=1)
PY

repo_clean
echo "wheel:   $(basename "$whl")"
echo "version: $version"
echo "sha256:  $sha"
echo "patches: ${applied[*]}"
