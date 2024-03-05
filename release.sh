#!/usr/bin/env bash

SCOPE="$1"

if [ -z "$SCOPE" ]; then
  SCOPE="patch"
fi

echo "Using scope $SCOPE"

bump2version "$SCOPE"
git push origin --follow-tags
