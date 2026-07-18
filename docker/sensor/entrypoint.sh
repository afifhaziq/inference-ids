#!/usr/bin/env bash
set -euo pipefail

# Zeek is the long-running foreground process for this container. tcpreplay is
# invoked on demand against this same container's eth0 via `make replay`
# (docker compose exec sensor tcpreplay ...) - it is not started here.
exec zeek -i eth0 local
