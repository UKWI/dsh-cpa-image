#!/usr/bin/env bash
# Install dsh-cpa-image into a DSH profile as a bundle.
#
# What it does (idempotent, no network):
#   1. copies this package to $DEST (default $HOME/diy-plugins/dsh-cpa-image)
#   2. adds it to the profile's package.json dependencies (link:) and to
#      dsh.profile.bundles
#   3. symlinks it into the profile's node_modules
#
# It does NOT restart the harness. Restart the web process afterwards so the
# new bundle mounts (the running process keeps its in-memory plugin tree).
#
# Usage:
#   ./install.sh                 # install into the web profile
#   PROFILE=~/.dsh/profiles/web ./install.sh
#   ./install.sh --dry-run       # show what would change
#   ./install.sh --uninstall     # remove the bundle entry and the copy

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEST="${DEST:-$HOME/diy-plugins/dsh-cpa-image}"
PROFILE="${PROFILE:-${DSH_HOME:-$HOME/.dsh}/profiles/web}"
PACKAGE_NAME="dsh-cpa-image"
MODE="install"

for argument in "$@"; do
  case "$argument" in
    --dry-run) MODE="dry-run" ;;
    --uninstall) MODE="uninstall" ;;
    -h|--help) sed -n '2,20p' "$0"; exit 0 ;;
    *) echo "unknown argument: $argument" >&2; exit 2 ;;
  esac
done

if [ ! -f "$PROFILE/package.json" ]; then
  echo "not a DSH profile: $PROFILE (no package.json)" >&2
  exit 1
fi

echo "package : $HERE"
echo "dest    : $DEST"
echo "profile : $PROFILE"
echo "mode    : $MODE"
echo

edit_package_json() {
  # $1 = "install" | "uninstall"
  node --input-type=module -e '
    import { readFileSync, writeFileSync } from "node:fs"
    const [file, name, target, mode] = process.argv.slice(1)
    const manifest = JSON.parse(readFileSync(file, "utf8"))
    manifest.dependencies ??= {}
    manifest.dsh ??= {}
    manifest.dsh.profile ??= {}
    manifest.dsh.profile.bundles ??= []
    const bundles = manifest.dsh.profile.bundles

    if (mode === "install") {
      manifest.dependencies[name] = `link:${target}`
      if (!bundles.includes(name)) bundles.push(name)
    } else {
      delete manifest.dependencies[name]
      manifest.dsh.profile.bundles = bundles.filter(entry => entry !== name)
    }
    writeFileSync(file, `${JSON.stringify(manifest, null, 2)}\n`)
    console.log(`  package.json: ${mode === "install" ? "added" : "removed"} ${name}`)
  ' "$PROFILE/package.json" "$PACKAGE_NAME" "$DEST" "$1"
}

if [ "$MODE" = "uninstall" ]; then
  edit_package_json uninstall
  rm -f "$PROFILE/node_modules/$PACKAGE_NAME"
  if [ "$DEST" != "$HERE" ]; then rm -rf "$DEST"; fi
  echo
  echo "Removed. Restart the harness to unmount it."
  exit 0
fi

if [ "$MODE" = "install" ]; then
  if [ "$DEST" != "$HERE" ]; then
    mkdir -p "$DEST"
    cp -a "$HERE/package.json" "$HERE/cordis.patch.yml" "$HERE/README.md" "$HERE/.gitignore" "$HERE/src" "$HERE/test" "$HERE/install.sh" "$DEST/"
    echo "  copied package -> $DEST"
  else
    echo "  package already in place -> $DEST"
  fi
fi

if [ "$MODE" = "install" ]; then
  edit_package_json install
  mkdir -p "$PROFILE/node_modules"
  ln -sfn "$DEST" "$PROFILE/node_modules/$PACKAGE_NAME"
  echo "  symlinked node_modules/$PACKAGE_NAME"
else
  echo "  (dry run: package.json and node_modules untouched)"
fi

echo
echo "Next: restart the harness web process, then reload the browser."
echo "No provider was added or replaced: every route keeps its own adapter and settings,"
echo "and this bundle only observes the model calls they make."
