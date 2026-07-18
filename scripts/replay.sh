#!/usr/bin/env bash
set -euo pipefail

PCAP="${1:?Usage: scripts/replay.sh <path-to-pcap> [pps]}"
PPS="${2:-100}"

mkdir -p pcaps
FILENAME="$(basename "${PCAP}")"

SOURCE_DIR="$(cd "$(dirname "${PCAP}")" && pwd)"
PCAPS_DIR="$(cd pcaps && pwd)"
if [ "${SOURCE_DIR}" != "${PCAPS_DIR}" ]; then
    cp "${PCAP}" "pcaps/${FILENAME}"
fi

docker compose exec sensor tcpreplay-edit --intf1=eth0 --pps="${PPS}" --fixcsum "/pcaps/${FILENAME}"
