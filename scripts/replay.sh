#!/usr/bin/env bash
set -euo pipefail

PCAP="${1:?Usage: scripts/replay.sh <path-to-pcap> [pps]}"
PPS="${2:-100}"

mkdir -p pcaps
cp "${PCAP}" pcaps/
FILENAME="$(basename "${PCAP}")"

docker compose exec sensor tcpreplay --intf1=eth0 --pps="${PPS}" --fix-checksums "/pcaps/${FILENAME}"
