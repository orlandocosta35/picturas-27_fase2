#!/bin/sh
# QS-01 campaign: 3 runs with 1 video_tool replica, then 3 runs with 3 replicas.
# Run from the repository root. Needs Docker and a Python with pika==1.3.2.
set -e
HERE=docs/benchmarks/2026-11-27-qs01-mac
OUT=${OUT:-$HERE/results}
P=rnf-bench
mkdir -p "$OUT"

VIDEO_WORKER_REPLICAS=1 docker compose -p $P up -d --build rabbitmq video_tool
C=$P-video_tool-1
until docker compose -p $P logs video_tool | grep -q 'waiting for jobs'; do sleep 2; done
docker exec $C mkdir -p /app/images/bench/src /app/images/bench/out
docker cp samples/video/speech_mandela_177s_1080p.webm $C:/app/images/bench/src/
docker cp samples/video/sample_10s_720p.mp4 $C:/app/images/bench/src/

for run in r1-a r1-b r1-c; do python3 $HERE/bench_qs01.py --run $run --warmup 1 --out "$OUT"; done
docker compose -p $P logs --timestamps --no-log-prefix video_tool > "$OUT/worker-1replica.log"

VIDEO_WORKER_REPLICAS=3 docker compose -p $P up -d --scale video_tool=3 video_tool
until [ "$(docker compose -p $P logs video_tool | grep -c 'waiting for jobs')" -ge 4 ]; do sleep 2; done
for run in r3-a r3-b r3-c; do python3 $HERE/bench_qs01.py --run $run --warmup 3 --out "$OUT"; done
docker compose -p $P logs --timestamps video_tool > "$OUT/worker-3replicas.log"
