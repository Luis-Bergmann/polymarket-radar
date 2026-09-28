#!/usr/bin/env bash
# Commit the track/ checkout (track-record branch) if it changed, and push.
# The radar and the backtest both write here, so rebase and retry on a race.
set -euo pipefail
cd track
git config user.name "radar-bot"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
git add -A
if git diff --cached --quiet; then
  echo "track record unchanged"
  exit 0
fi
git commit -qm "$1"
for i in 1 2 3 4 5; do
  if git push -q origin HEAD:track-record; then
    echo "track record pushed"
    exit 0
  fi
  sleep $((i * 3))
  git pull -q --rebase origin track-record
done
echo "could not push track record" >&2
exit 1
