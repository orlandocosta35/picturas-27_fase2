"""QS-01 benchmark: one subtitles job + two short jobs, published back to back.

Publishes the same request format that `projects` sends (routing key
`video_queue`), waits for the worker replies on `project_queue`, and records
per-job submit/reply instants plus a 1 s `docker stats` series.
Bypasses HTTP, quotas and MongoDB: it measures broker + worker only.
"""
import argparse
import csv
import json
import subprocess
import threading
import time
import uuid
from datetime import datetime, timezone

import pika

EXCHANGE = "picturas"
ROUTING_KEY = "video_queue"
REPLY_QUEUE = "project_queue"
ROOT = "/app/images/bench"


def iso(t):
    return datetime.fromtimestamp(t, timezone.utc).isoformat(timespec="milliseconds")


def job(label, src, tools, run):
    msg_id = f"bench-{run}-{label}-{uuid.uuid4().hex[:8]}"
    return {
        "label": label,
        "msg": {
            "messageId": msg_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "procedure": "video",
            "parameters": {
                "inputImageURI": f"{ROOT}/src/{src}",
                "outputImageURI": f"{ROOT}/out/{run}/{label}.mp4",
                "tools": tools,
                "preview": False,
                "progress": {},
            },
        },
    }


def stats_sampler(stop, project, path):
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["t_utc", "container", "cpu_pct", "mem_usage"])
        while not stop.is_set():
            t = time.time()
            out = subprocess.run(
                ["docker", "stats", "--no-stream", "--format",
                 "{{.Name}};{{.CPUPerc}};{{.MemUsage}}"],
                capture_output=True, text=True).stdout
            for line in out.splitlines():
                name, cpu, mem = line.split(";")
                if name.startswith(f"{project}-video_tool"):
                    w.writerow([iso(t), name, cpu.rstrip("%"), mem])
            f.flush()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--run", required=True)
    p.add_argument("--project", default="rnf-bench")
    p.add_argument("--long", default="speech_mandela_177s_1080p.webm")
    p.add_argument("--short", default="sample_10s_720p.mp4")
    p.add_argument("--out", required=True)
    p.add_argument("--warmup", type=int, default=0,
                   help="publish N subtitle jobs on the short video first, wait, not measured")
    a = p.parse_args()

    conn = pika.BlockingConnection(pika.URLParameters(
        "amqp://user:password@localhost:5672/%2F"))
    ch = conn.channel()
    ch.confirm_delivery()
    ch.queue_purge(REPLY_QUEUE)

    def publish(j):
        t = time.time()
        ch.basic_publish(EXCHANGE, ROUTING_KEY, json.dumps(j["msg"]).encode(),
                         properties=pika.BasicProperties(delivery_mode=2,
                                                         content_type="application/json"),
                         mandatory=True)
        return t, time.time()  # submit, broker-confirmed

    def wait_replies(ids, timeout=1800):
        got = {}
        deadline = time.time() + timeout
        while len(got) < len(ids) and time.time() < deadline:
            method, _, body = ch.basic_get(REPLY_QUEUE, auto_ack=True)
            if method is None:
                conn.sleep(0.05)
                continue
            t = time.time()
            m = json.loads(body)
            if m.get("correlationId") in ids:
                got[m["correlationId"]] = (t, m)
        return got

    if a.warmup:
        w = [job(f"warmup{i}", a.short, [{"procedure": "subtitles", "params": {}}], a.run)
             for i in range(a.warmup)]
        for j in w:
            publish(j)
        wait_replies({j["msg"]["messageId"] for j in w})

    jobs = [
        job("subtitles", a.long, [{"procedure": "subtitles", "params": {}}], a.run),
        job("rotate", a.short, [{"procedure": "rotate", "params": {"degrees": 90}}], a.run),
        job("saturation", a.short,
            [{"procedure": "saturation", "params": {"saturationFactor": 1.5}}], a.run),
    ]
    stop = threading.Event()
    sampler = threading.Thread(target=stats_sampler,
                               args=(stop, a.project, f"{a.out}/{a.run}-stats.csv"))
    sampler.start()
    times = {}
    for j in jobs:
        times[j["msg"]["messageId"]] = (j["label"],) + publish(j)
    replies = wait_replies(set(times))
    stop.set()
    sampler.join()
    conn.close()

    rows = []
    for mid, (label, t_submit, t_confirm) in times.items():
        t_reply, m = replies.get(mid, (None, None))
        rows.append({
            "run": a.run, "job": label, "msg_id": mid,
            "t_submit": iso(t_submit), "t_confirm": iso(t_confirm),
            "t_reply": iso(t_reply) if t_reply else "",
            "latency_s": round(t_reply - t_submit, 2) if t_reply else "",
            "status": m["status"] if m else "timeout",
            "worker_processing_s": round(m["metadata"]["processingTime"] / 1000, 2) if m else "",
            "frames": (m.get("metadata", {}).get("video") or {}).get("frames", "") if m else "",
        })
    with open(f"{a.out}/{a.run}-jobs.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    for r in rows:
        print(json.dumps(r))


if __name__ == "__main__":
    main()
