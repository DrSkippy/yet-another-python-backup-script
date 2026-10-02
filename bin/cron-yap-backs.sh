#!/usr/bin/env bash
# Cron entrypoint for yap-backs: cron has no direnv and a minimal PATH, so
# load .envrc and resolve poetry explicitly before running a real backup.
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BACKUP_MOUNT="/home/scott/Scott-Ubuntu/backup"
POETRY="/home/scott/.local/bin/poetry"
LOCK_FILE="/tmp/yap-backs.lock"

# pyenv shims are needed so poetry resolves Python 3.11 and finds the existing
# virtualenv; with system python it would create a new, empty in-project .venv.
export PATH="/home/scott/.pyenv/shims:/home/scott/.pyenv/bin:/home/scott/.local/bin:/usr/local/bin:/usr/bin:/bin"

cd "$PROJECT_DIR"

# shellcheck source=/dev/null
source "$PROJECT_DIR/.envrc"

# If the NFS share isn't mounted, the backups would silently fill the local disk.
if ! mountpoint -q "$BACKUP_MOUNT"; then
    echo "$(date '+%F %T') - ERROR - $BACKUP_MOUNT is not mounted; skipping backup" >&2
    exit 1
fi

# Skip this run if the previous one is still going.
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
    echo "$(date '+%F %T') - ERROR - previous yap-backs run still in progress; skipping" >&2
    exit 1
fi

exec "$POETRY" run python bin/yap-backs.py --execute
