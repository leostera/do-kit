#!/bin/bash
# A Worker's error events of the last minutes, by message, from Workers Observability:
#
#   CLOUDFLARE_ACCOUNT_ID=… bench/errors.sh <service> [minutes] [cf args, e.g. --profile name]
#
# A runtime that resets objects, or calls that fail and are retried, show here and nowhere in
# the results: this is how the work graph runner resets were found. Runs cf from outside the
# repository: inside it, cf hands over to the repository's own copy, which lacks this command.
set -euo pipefail
service=$1; minutes=${2:-30}; shift $(( $# >= 2 ? 2 : 1 ))
to=$(( $(date +%s) * 1000 )); from=$(( to - minutes * 60000 ))
query=$(printf '{"queryId":"adhoc","timeframe":{"from":%d,"to":%d},"view":"events","limit":1000,"parameters":{"filters":[{"key":"$metadata.service","operation":"eq","type":"string","value":"%s"},{"key":"$metadata.level","operation":"eq","type":"string","value":"error"}]}}' "$from" "$to" "$service")
(cd "${TMPDIR:-/tmp}" && cf observability telemetry query --body "$query" "$@") | python3 -c '
import collections, json, sys
d = json.load(sys.stdin)
events = (d.get("result") or d)["events"]["events"]
counts = collections.Counter()
for e in events:
    w, m = e["$workers"], e["$metadata"]
    what = m.get("message") or json.dumps(e.get("source"))[:200]
    counts[(w.get("scriptVersion", {}).get("id", "?")[:8], w.get("entrypoint", ""), w.get("eventType"), what[:200])] += 1
for (version, entrypoint, kind, what), n in counts.most_common():
    print(f"{n}\t{version} {entrypoint} {kind}: {what}")
print(f"{len(events)} error events")
'
