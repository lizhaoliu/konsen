#!/bin/bash

set -e

CURDIR="$(cd "$(dirname "$0")"; pwd)"
exec "$CURDIR/konsen" --cluster_config_path "$CURDIR/cluster.yml" --db_file_path "$CURDIR/logs.db"