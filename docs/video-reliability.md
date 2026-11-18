# Video parallelism and failure recovery

This change addresses **REQ-VID-EDIT-011** (at least two videos processed concurrently
when resources permit) and **REQ-VID-FAIL-004** (recover video jobs after internal failures).
Configuration and unit tests are implementation evidence; the acceptance results also
need the broker tests and measurements described below.

## Processing and retry topology

- `projects` publishes persistent requests on the `picturas` direct exchange, using
  routing key `video_queue`.
- The versioned **quorum** queue `video_queue_v2` distributes requests to three
  `video_tool` replicas. Each replica has `prefetch_count=1` and processes one video
  at a time. Compose limits each replica to 2 CPUs and 2.5 GB RAM.
- An unexpected processing exception requeues the request. The quorum queue permits
  one redelivery after the initial attempt, including re-delivery after a consumer
  connection closes or a worker is killed. Invalid video parameters are reported
  immediately without retry.
- Exhausted requests go through `picturas.dlx`, routing key `video_dlq`. The original
  request remains in `video_dlq` for inspection. A separate queue,
  `video_failed_events`, receives a copy so the worker can report terminal errors
  to `projects` without consuming the inspection copy.
- The quorum queue uses `at-least-once` dead lettering with `reject-publish` overflow.
  Replies are published with confirmation before the original delivery is acknowledged.
  The model weights for subtitles are preloaded into the worker image during its build.

The job is restarted from its original input; this is not a checkpoint that resumes
at an intermediate frame. Delivery is **at least once**, not exactly once. A crash
between a confirmed publication and its acknowledgement can produce a repeated reply.
The orchestrator handles ordinary repeated replies without adding duplicate results,
but AMQP acknowledgements and MongoDB updates do not form a distributed transaction.
Three replicas improve capacity only when the machine has enough CPU and memory;
they do not imply three times the throughput.

A terminal failure of a full processing run invalidates that run's token before
removing its pending Process records. Previous committed results remain available,
and late replies from sibling workers cannot commit an incomplete failed run. If
the error notification cannot be confirmed, its retained job lets the orchestrator
retry that notification. A failed preview removes only its own job and does not
invalidate a full run. A replay after successful commit resumes completion reporting
without uploading or rolling back already committed outputs.

## Upgrading an existing installation

RabbitMQ queue types and declaration arguments cannot be changed in place. The new
queue name avoids redeclaring an existing classic `video_queue` as quorum. Existing
RabbitMQ data can be retained: **do not run `docker compose down -v` for this upgrade**.

Schedule a maintenance window and keep video publishers and workers stopped throughout
the transfer. `--producers-stopped` records that operator precondition; the migration
also refuses an active consumer on either the source or destination queue.

From the repository root:

```bash
docker compose stop projects video_tool
docker compose build video_tool projects
docker compose up -d --build rabbitmq
docker compose run --rm --no-deps video_tool python migrate_queue.py
docker compose run --rm --no-deps video_tool python migrate_queue.py --apply --producers-stopped
docker compose exec rabbitmq rabbitmqctl list_queues name type messages_ready messages_unacknowledged consumers
docker compose up -d projects video_tool
```

The first migration command only inspects the legacy queue. The applied migration:

1. Declares the new topology and verifies that no video consumer is active.
2. Removes the old queue binding for routing key `video_queue`, so future requests
   go only to `video_queue_v2`.
3. Republishes pending jobs persistently to the new queue, waiting for publisher
   confirmation before acknowledging each source message.
4. Leaves the old queue in place, empty, for inspection. It does not delete data or volumes.

If the transfer is interrupted, leave publishers stopped and rerun it. Unacknowledged
source jobs return to the old queue when the connection closes. A confirmed copy can
already be in the new queue if interruption happened before source acknowledgement,
so an interrupted transfer can create a duplicate; it does not intentionally discard
the original. Keep the resulting jobs and replies available for investigation.

For a fresh installation the legacy queue is absent and the utility reports that no
migration is needed. `RABBITMQ_URL` can override the migration connection URL; by
default it connects to the Compose broker. Do not put real credentials in committed files.

## Automated verification

The host needs Python 3.11+, FFmpeg and ffprobe. The regular suite needs no broker and
simulates speech recognition rather than downloading a model:

```bash
cd Tools/video
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m pytest -q tests
```

Broker integration tests are opt-in, require RabbitMQ 4.x, and create uniquely named
queues and exchanges. They clean up their own resources without using application queues.
With a test broker reachable on localhost:

```bash
PICTURAS_TEST_AMQP_URL='amqp://user:password@localhost:5672/%2F' .venv/bin/python -m pytest -q tests/test_worker_integration.py
```

These tests cover transient failure followed by success, exhaustion into the real DLQ,
bounded repeated consumer interruption, two workers executing concurrently, and migration
from the classic queue. The unit suite separately exercises error classification and
acknowledgement decisions. To verify Node orchestration from the repository root:

```bash
node --test projects/tests/*.test.js
```

## Acceptance evidence to collect

For **EDIT-011**, run the same batch of at least two videos first with one replica and
then with three. Keep video inputs, pipeline, machine, and resource limits identical.
Record each job's start/end time, worker identity, total batch duration, and CPU/memory.
The start/end intervals must overlap for at least two different workers. Report measured
results rather than assuming a speedup from the replica count.

```bash
docker compose up -d --scale video_tool=1 video_tool
docker compose logs -f video_tool
docker stats
docker compose up -d --scale video_tool=3 video_tool
```

For **FAIL-004**, use a controlled test job and the isolated broker tests to introduce:

- One transient failure: the retry succeeds and the user receives one usable result.
- A persistent failure: the retry budget is exhausted, the original request remains in
  the DLQ, and the user receives a terminal error.
- A worker interruption while a job is in flight: another available worker reprocesses
  the unacknowledged job. Repeat interruptions to verify the broker-enforced limit.
- A broker restart: workers reconnect and pending confirmed jobs remain available.

Check original media preservation, result count, terminal project state, and absence
of partial files after handled failures. An uncatchable kill can leave temporary files;
do not equate cleanup after exceptions with guaranteed cleanup after every process crash.
Also verify the user-facing progress and error notification through the browser, since
worker tests alone do not exercise the complete HTTP/WebSocket flow.

## Validation recorded on 2026-11-18

- **105 Python tests passed**, including all five opt-in integration tests, in 39.21 s.
  The isolated test environment used Python 3.11, FFmpeg 7.1.5 and RabbitMQ
  `4.0.5-management`. Recognition was simulated; no speech model was needed.
- Real broker deliveries verified transient recovery with a valid MP4 result,
  retained dead letters plus terminal notification, a bounded sequence of two
  consumer process crashes, migration of pending classic-queue jobs, and overlapping
  processing by two workers producing distinct valid outputs.
- All four Node test files passed (24 cases), covering batch ordering, cancellation,
  publication confirmation, asynchronous reply handling, result idempotency and
  interrupted completion/error notification recovery.
- Compose configuration, RabbitMQ definitions JSON and JavaScript/Python syntax
  checks passed. Existing RabbitMQ volumes were not migrated or deleted during validation.

These are automated component and broker integration results. They do not replace
the end-to-end browser check or the one-versus-three-worker throughput measurement
on representative videos described above. The complete application stack and
frontend production build were not run during this validation.

## Inspecting failed jobs

Use RabbitMQ management at `http://localhost:15672` or:

```bash
docker compose exec rabbitmq rabbitmqctl list_queues name type messages_ready messages_unacknowledged consumers
docker compose logs --since 10m video_tool projects
```

`video_dlq` is an inspection queue, not an automatic infinite retry loop. Investigate
the cause and check whether the project's run is still active before manually resubmitting
a failed request. Replaying an obsolete or cancelled run must not replace current results.
