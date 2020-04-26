#!/bin/bash

set -e -u
CURDIR="$(cd "$(dirname "$0")"; pwd)"
cd "$CURDIR"
OUTPUT_DIR="$CURDIR/output"
[ -d "$OUTPUT_DIR" ] && rm -r "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"
go build -o "$OUTPUT_DIR/konsen"
go run "$CURDIR/configutils/main.go" \
  --cluster_config_path "$CURDIR/conf/cluster.yml" \
  --binary_path "$OUTPUT_DIR/konsen" \
  --output_dir "$OUTPUT_DIR"
rm "$OUTPUT_DIR/konsen"