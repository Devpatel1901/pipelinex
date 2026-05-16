# PipelineX — Q&A Preparation Notes

> Cheat sheet for the 15-minute Q&A after the KLA presentation. Every answer
> is grounded in actual code in this repo — file paths and line numbers in
> parentheses so I can re-read before the interview. Answers are kept tight
> (4–5 lines). If the panel pushes further, I expand from the **Stretch:**
> line.

---

## Section A — Concurrency & async runtime

### A1. *"Walk me through what happens when the consumer is slower than the producer."*
The producer reads lines and calls `await queue.put(record)` (pipeline.py:131). The queue has `maxsize=1000` by default; when full, `put()` suspends the producer task until a consumer dequeues. Workers continue at their natural rate, the queue stays at the cap, and memory is bounded. No records are dropped — the producer just runs at consumer pace.
**Stretch:** I verify this with a benchmark using a `SlowRepository` that sleeps 50 ms per batch; queue saturates at 100/100 and zero records are lost.

### A2. *"Why N workers? Why not one coroutine per record?"*
Three reasons. (1) Batching to the repository requires a bounded number of accumulating buffers — unbounded coroutines erase the batch boundary. (2) The queue is the single ordering and backpressure point; per-record coroutines bypass it. (3) Memory: 1 million in-flight coroutines is gigabytes of stack frames. Fixed N workers + bounded queue keeps the runtime predictable.

### A3. *"You're using asyncio. What if a stage does CPU-heavy work and blocks the event loop?"*
That would block all workers in the same process — the regex parsers and IQR bisect are fast enough today (~6 µs p99 in benchmark), so it isn't a problem. If it became one, the fix is `loop.run_in_executor()` to push the stage into a `ProcessPoolExecutor`. That's a documented v2 — the `IPipelineStage.process()` signature already returns an awaitable, so no caller changes.

### A4. *"Why asyncio at all? Couldn't threads or multiprocessing be simpler?"*
The workload is I/O-bound — file reads, network repository writes, awaiting queue puts. Threads carry GIL contention and ~MB stack overhead per worker; multiprocessing carries serialization cost for every record across the process boundary. Asyncio gives me thousands of cheap concurrent tasks in one process with deterministic scheduling, which is the right shape for this workload.

### A5. *"What guarantees no record is lost in flight when something crashes?"*
The worker's `finally` block always calls `queue.task_done()` (pipeline.py:159), so `await queue.join()` (line 96) waits for *every* dequeued item to finish even if a stage raises. Exceptions are caught by class: `FatalStageError`, `TransientStageError`, and a catch-all `Exception` — each path increments `_records_failed` and continues. The conservation property is `_records_processed + _records_failed == lines_in`.

### A6. *"Walk me through shutdown second by second."*
On Ctrl-C, the CLI calls `executor.shutdown()`, which sets `_shutdown_event`. Workers poll the queue with a 0.5 s timeout (pipeline.py:137), notice the event, drain remaining items, flush their batch buffer, and exit. The sessionizer's `aclose()` flushes every open trace with `closed_reason="shutdown"`. Repository commits the final batch. Property test triggers shutdown at random points and the records-conservation invariant holds every run.

### A7. *"Why a bounded queue specifically, and not just letting it grow?"*
Unbounded queues trade a clear failure mode (producer blocks) for a destructive one (OOM kill — every in-flight record dies with the process). The bounded queue gives me backpressure for free: no rate limiter needed because the queue *is* the rate limiter. The downside is the producer must be pause-able, which is fine for file/stdin sources; a network source with no pause control would buffer at the edge.

### A8. *"Default queue size is 1000. How did you pick that?"*
It's a knob, not a magic number — set in YAML. 1000 is big enough to absorb short consumer stalls (one slow Postgres batch is ~50 ms; at 46 K/s that's ~2300 records, so 1000 trades latency for memory predictability). At ~200 bytes per `LogRecord`, the queue costs ~200 KB max. Tuning advice: bigger queue = more burst absorption, less responsiveness to pressure.

### A9. *"What's the maximum throughput you measured, and where's the bottleneck?"*
46,662 records/sec at 4 workers on a 50K-line parser-only workload (`benchmarks/results/throughput.json`). Single worker gets 47,665 rec/s — adding workers barely helps because the asyncio event loop is the bottleneck, not the per-stage work. p99 latency is 8.50 µs per record. To push past this, move the parser to a process pool — documented v2.

### A10. *"Why doesn't throughput scale with worker count?"*
Per-record work is ~6 µs of CPU (regex + a few dict ops). At that grain size, the asyncio scheduling overhead — context switches, queue operations, awaitable bookkeeping — dominates the actual stage work. The event loop runs on one thread, so all "parallel" coroutines share one CPU. True parallelism needs multiple processes; v2 plan.

---

## Section B — Anomaly detection (sequence)

### B1. *"Why n-gram instead of an LSTM or autoencoder?"*
Three reasons. (1) ~150 lines of code, zero ML dependencies — deterministic, debuggable, no GPU. (2) Published HDFS baselines (Loghub paper, DeepLog comparisons) hit F1 ~0.95 with n-gram methods, so it's a strong baseline, not a strawman. (3) Fully explainable — I can point at an exact 2-gram and say "this pair never appeared in any normal training trace." LSTMs give you "the model said anomalous" and you're done.

### B2. *"Why 2-grams? Why not 3 or 4?"*
2-grams cover almost all observed bigram transitions in normal HDFS traces — 137 distinct ones from 29 templates, out of 841 possible. 3-grams explode the vocabulary to 24,389 cells with very sparse coverage, which inflates novelty by chance and would crush precision. The right way to use longer context is a frequency model (KL-divergence on bigram distributions), not bigger n — documented v2.

### B3. *"Precision = 1.0 looks suspicious. What's the catch?"*
It's structural, not luck. The detector fires *only* on 2-grams that never appeared in any labeled-Normal training trace (sequence.py:124). By construction every fire is correct *given clean training labels*. The real risk is training-set contamination — one mislabeled-anomaly block in "Normal" would silently whitelist its bigrams forever. I depend on Loghub labels being clean.

### B4. *"Recall is only 28.5%. Why?"*
About 71% of true anomalies use only bigrams that also appear in normal traces — just in different proportions, lengths, or positions. A truncated write trace, for example, is entirely "normal" bigrams; the anomaly is the *absence* of the closing bigram, not the presence of a novel one. Set-membership can't capture absence — divergence-based detection can.

### B5. *"What would KL-divergence detection actually look like in code?"*
Replace the frozenset of normal bigrams with a normalized count distribution P over bigrams (with Laplace smoothing for unseen pairs). For each trace, compute its bigram distribution Q. Fire if `KL(Q || P) > tau`, where tau is tuned on a held-out validation split. That captures "wrong proportions" which is exactly the failure mode of v1.

### B6. *"How do you compute the anomaly score?"*
`ratio = len(novel_bigrams) / len(observed_unique_bigrams)` (sequence.py:125). Both sides use the deduplicated set, so a trace that repeats one novel bigram 50 times scores the same as one that uses it once. Fire when `ratio > threshold` — default 0.0, so any novel bigram fires. Score is returned as `severity_score` on the AnomalyEvent.

### B7. *"What if a block has fewer than 2 events?"*
`ngrams()` yields nothing for sequences shorter than n (sequence.py:120). The detector returns `None` — no anomaly. In practice this is rare: even truncated HDFS blocks have at least one `allocateBlock` + one `Receiving block`. Could be a blind spot for severely truncated traces; v2 frequency model with smoothing would assign nonzero probability to short sequences.

### B8. *"How is the model trained?"*
`scripts/train_sequence_model.py` (~3 min on full corpus). Stream-parse `HDFS.log` → template-match each line to E1–E29 → group by `block_id` → join against `anomaly_label.csv` → keep only `Label == "Normal"` (558,223 blocks) → collect every 2-gram that appears in any of them (137 unique) → write JSON `{n: 2, normal_ngrams: [...], vocabulary: [...]}` to `models/hdfs_ngram_v1.json`.

### B9. *"How did you evaluate accuracy?"*
`scripts/evaluate_hdfs_sequence.py` runs the full pipeline against all 575,061 labeled blocks, builds a confusion matrix by comparing predicted-anomaly vs. labeled-anomaly. TP=4,798, FP=0, FN=12,040 → P=1.0, R=0.285, F1=0.4435. Output is `benchmarks/results/hdfs_eval.json` — reproducible from a fresh checkout: `python scripts/train_sequence_model.py && python scripts/evaluate_hdfs_sequence.py`.

### B10. *"What happens if an anomalous block hits a template you've never seen?"*
The template matcher returns no match and the line gets no `event_id` in enrichment. The sessionizer still tracks the record under `block_id`, but the missing event becomes a gap in the sequence. The 2-gram detector sees the surrounding pairs; if either is novel it fires, otherwise it misses. Real risk on logs with templates that weren't in training — Loghub's 29 are stable for HDFS_v1.

### B11. *"How would you handle concept drift — templates evolving over time?"*
Two layers. (1) Detect: monitor `unmatched_template_count` per minute; spike means new templates. (2) Adapt: periodic offline retraining (current model is a JSON file, so this is just running the trainer on the latest week). Online adaptation is harder because you'd whitelist novelty by definition; safer to keep training offline with human-labeled normal windows.

---

## Section C — Anomaly detection (point — BGL)

### C1. *"Why three different point detectors instead of one?"*
They catch different anomaly shapes. Z-Score catches gaussian-tail outliers — fast spikes against a stable mean. IQR (Tukey fence) is robust to outliers in the training window — it doesn't drift its threshold up when prior anomalies inflated the mean. CUSUM catches *gradual drifts* — small persistent shifts that never trip a per-sample threshold. They complement each other.

### C2. *"Why is BGL F1 so low (~0.20) — does that mean the system doesn't work?"*
The opposite — it's the finding that justifies the whole architecture. BGL alerts are *individual anomalous lines* mixed into normal traffic, not rate spikes. Counting events per minute is simply not the right signal. The fix is feature-engineered per-window features (alert density, message-length variance, template novelty) — documented v2. Low F1 on the wrong signal is the *correct* result.

### C3. *"How does the IQR detector handle the moving window efficiently?"*
Two views over the same data (iqr.py): a `deque[float]` for insertion-order eviction, plus a `sorted list` maintained via `bisect` for O(log n) percentile lookup. When the deque fills, evicted value is binary-searched out of the sorted list. Quartiles are recomputed each call — cheap because the sorted list is already sorted.

### C4. *"What's the warmup behavior — first few samples?"*
Each detector has a minimum sample requirement. Z-Score needs 30 (zscore.py:36), IQR needs 4 for quartiles, CUSUM collects 30 to auto-calibrate target and sigma. Below that, `detect()` returns `None` — no anomaly fired. Prevents the obvious bug where a window of 2 samples gives a nonsense mean and fires every record.

### C5. *"CUSUM has a 'slack' parameter — what is it?"*
The Page-CUSUM allowance — the per-sample slack added before accumulating into the running sum. With slack k=0.5σ, the upper CUSUM is `S+ = max(0, S+ + (x - target) - k)`. It absorbs natural noise: small deviations don't accumulate, persistent shifts do. Without it, CUSUM would alarm on every minor wobble.

### C6. *"Why does Z-Score score *before* adding the current value to the window?"*
Otherwise the current value pollutes its own statistics — a true outlier would inflate sigma when added, making the z-score artificially small. Score against the *prior* distribution, then update. This is the standard convention; I added a comment at zscore.py:85 because it's the kind of bug a fresh reader would "fix" into existence.

### C7. *"Are the detectors stateful? What happens on shutdown?"*
Yes — each detector holds a sliding window in memory. On clean shutdown the windows are discarded; on next start the warmup re-engages. That's fine because BGL detection is window-local; restart costs you ~30 records of warmup, not historical data. If durable state mattered I'd checkpoint the window to the repository — not needed in v1.

---

## Section D — Sessionizer (deep dive)

### D1. *"Why store `record_ids` (UUIDs) instead of full `LogRecord` objects?"*
Memory. HDFS has ~575K blocks averaging 19 lines each — 11 M records. Holding full records in open traces would pin multiple gigabytes while the pipeline runs. UUIDs are 16 bytes; an open trace is ~50 bytes regardless of length. If the detector needs the full records later, it queries the repository. Bounded memory is a contract, not a hope.

### D2. *"How do you decide when a block is 'done'?"*
Primary trigger is timeout — a janitor coroutine runs every 5 s (`flush_interval_s`) and closes any trace whose last event is more than 30 log-seconds old (`idle_window_s`). Terminal events (E9, E21) are a *hint* that fast-tracks the trace to the next pass. Plus a hard cap (1000 events on one block force-closes), LRU eviction at 50,000 open traces, and shutdown flushes all open traces.

### D3. *"Why not just emit on the terminal event?"*
**Anomalous traces by definition often miss the terminal event** — that's *what's anomalous about them*. Terminal-only emission would silently drop the most interesting cases. Timeout-as-primary is what makes incomplete traces detectable at all.

### D4. *"What if the janitor and a worker hit the same trace at the same time?"*
The open-traces `OrderedDict` is guarded by an `asyncio.Lock()` (block_sessionizer.py:135). Both worker (appending events) and janitor (scanning and closing) acquire it before touching `_open`. asyncio locks are cooperative, not preemptive, so this is sufficient — no other task runs between `acquire` and `release`.

### D5. *"What's an `OrderedDict` doing here?"*
LRU eviction. Python dicts preserve insertion order since 3.7, but `OrderedDict.popitem(last=False)` pops the *oldest* in O(1), and `move_to_end()` re-marks recency on access — both faster and clearer than equivalent plain-dict gymnastics. When `len(_open) > max_open_traces`, the oldest open trace is evicted with `closed_reason="evicted"`.

### D6. *"What are the four closed_reason values, and why does each exist?"*
`"terminal"` — saw E9 or E21, the natural end-of-life events for HDFS blocks. `"timeout"` — janitor closed it because it sat idle past `idle_window_s`. `"shutdown"` — Ctrl-C/SIGTERM forced a flush of all open traces. `"evicted"` — LRU cap hit; oldest trace forced out to keep memory bounded. Detector sees `closed_reason` in metadata so it can weight scores differently if needed.

### D7. *"How big can one trace get before it's a problem?"*
Hard cap is 1000 events per block (force-close past that). On real HDFS the 99th percentile is ~30 events per block; 1000 is a generous safety net for a runaway block, not a normal-case limit. At 1000 events × ~50 bytes per open-trace overhead, worst case is still well under a megabyte per pathological block.

### D8. *"Does the sessionizer change the records that flow through it?"*
No — it's a tee. Each record continues downstream to the repository unchanged. The trace is a *derived* artifact emitted on the event bus when closed, separate from the per-line record stream. Records and traces are two different persistence paths.

---

## Section E — Architecture & design patterns

### E1. *"Six design patterns feels like a lot. Aren't you over-engineering?"*
My rule was every pattern must earn its place. Strategy → multiple log formats. Factory → YAML string to class. Decorator → cross-cutting concerns (timing/retry/logging) without per-stage duplication. Observer → multiple reactions to one trace close. Chain of Responsibility → multi-validator short-circuit. Repository → swap memory↔Postgres without app changes. Remove any of them and the project gets worse, not simpler.

### E2. *"Which pattern was the closest call?"*
Factory. It's a wrapper around a dict — easy to argue it's overkill. The reason it stays is the YAML config writes strings like `kind: hdfs`; without a factory, the loader gets a giant if/else chain that grows with every parser. The dict isolates that growth into one file (`stages/parsing/factory.py`) instead of leaking it across the codebase.

### E3. *"Why an EventBus when you have one subscriber?"*
Insurance for the second subscriber. The moment I add a Slack alerter, a metrics exporter, or an audit sink, the sessionizer would otherwise need to know about each. The bus' value is the *absence* of coupling that would grow without it. Cost is ~70 lines (events/bus.py). It also gives me exception isolation — a buggy listener doesn't poison the others.

### E4. *"What's the weakest part of the architecture?"*
Decorator-stack ordering is implicit — `timing(retry(stage))` and `retry(timing(stage))` behave differently (the first times all attempts, the second times each attempt separately), and nothing in the YAML enforces sensible order. v2 fix: explicit ordering in the schema, or a `DecoratorChain` builder that documents intent. Not yet a real problem because only one decorator is wired in production configs.

### E5. *"How would you scale to multiple nodes?"*
Replace the in-process queue with Kafka or Redis Streams. Producer publishes to a topic; multiple worker processes subscribe via a consumer group. Each process keeps its own internal bounded queue with the same backpressure semantics. Hard part is sessionization — block traces have to be partitioned by `block_id` (Kafka key) so all events for one block land on the same consumer, or merged post-hoc.

### E6. *"Pydantic v2 vs dataclasses for config — why?"*
Runtime validation. `extra="forbid"` on every config class (`config.py`) catches typos in YAML at load time instead of silently using a default. Dataclasses give you the type hints but no actual checking. For user-edited YAML — which is the whole point of "extensible by config" — the dependency is worth it.

### E7. *"What does the repository abstraction actually buy you?"*
Two concrete things. (1) Tests run against in-memory in microseconds — no Docker, no Postgres setup, fast feedback loop. (2) Same code path for dev and prod — the same `save_batch` interface, same conservation guarantees, same shutdown semantics. The integration test suite runs against both implementations to ensure they're truly interchangeable, not just nominally.

### E8. *"What was the hardest design decision?"*
Choosing UUIDs over LogRecords inside open traces. It's an irreversible commitment — once detectors can only see UUIDs, they can't trivially read fields, they have to query the repository. I gave up some local convenience for bounded memory. On the 2K-line sample it was invisible; on 11 M lines it's the difference between a working system and one that OOMs.

---

## Section F — Errors, validation, shutdown

### F1. *"Why two exception types instead of one?"*
Different responses. A bad config or broken schema (`FatalStageError`) means tearing down the pipeline — you can't continue past it. A single malformed line (`TransientStageError`) means log it, count it, move on — the other 999,999 lines per second don't deserve to die for it. Catching by class (pipeline.py:147,150) is robust against error-message changes; string-matching exceptions is a code smell.

### F2. *"What does the retry decorator actually do?"*
Wraps any stage; catches only `TransientStageError` (retry.py:48) so fatals propagate immediately. Exponential backoff: 50 ms base × 2^attempt, capped at 1 s, max 3 retries. After exhausting, re-raises the `TransientStageError` — which the worker then catches, logs, and counts. Decoupled from the stage's actual logic.

### F3. *"What validators do you have, and in what order?"*
Three, run cheap-first: (1) Schema — required fields present (~1 µs). (2) Size — message under 64 KB (~1 µs). (3) Rate-limit — per-source token bucket (~5 µs). Chain short-circuits on first failure (`chain.py:39-44`) so an oversized record doesn't waste a rate-limit check. Any rejection becomes a `ValidationError` (a `TransientStageError` subclass).

### F4. *"How does the rate limiter work — token bucket or sliding window?"*
Token bucket per source. Default capacity 1000 tokens, refill 1000/sec (so a steady-state max of 1000 records/sec per source). On each record, refill `tokens += elapsed × refill_rate` capped at capacity, then consume 1 token; reject if `tokens < 1`. Simple, no clock-window edge cases, no allocations per check.

### F5. *"What happens during graceful shutdown that prevents data loss?"*
Six steps: (1) shutdown event set; (2) producer stops reading source; (3) workers drain queue with 0.5 s polling; (4) on empty queue, workers flush their batch buffer; (5) sessionizer's `aclose()` flushes every open trace with `closed_reason="shutdown"`; (6) repository commits the final transaction. The conservation invariant is checked by property tests that trigger shutdown at random times.

### F6. *"What if shutdown takes too long — is there a hard timeout?"*
Currently no — `shutdown()` awaits `queue.join()` and worker completion indefinitely. In a real prod deployment I'd wrap it in `asyncio.wait_for(executor.shutdown(), timeout=30)` so a hung repository can't block SIGTERM forever. The cost is the records still in queue at the deadline are lost — graceful shutdown is best-effort under a deadline.

### F7. *"What if the EventBus listener throws?"*
Logged and isolated. The bus dispatches via `asyncio.gather(*handlers, return_exceptions=True)` (bus.py:52) so one listener's exception is captured, logged as a warning, and doesn't propagate to other listeners or the publisher. The sessionizer doesn't know or care if the sequence detector failed.

---

## Section G — Testing & correctness

### G1. *"How did you test that backpressure actually works?"*
Three layers. (1) Unit tests with mocked sleepers asserting `queue.put` blocks when full. (2) Hypothesis property test `test_records_in_equals_records_out` varying workers, queue size, batch size across 30+ random configs. (3) A dedicated benchmark with `SlowRepository` (50 ms per batch) and queue size 100 — verified queue saturates at 100/100 with zero records lost.

### G2. *"What did Hypothesis catch that unit tests didn't?"*
A real BGL parser bug. `parts[0]` was indexed without checking that `parts` was non-empty. The property test `test_can_parse_never_raises` fed it whitespace-only inputs; Hypothesis shrank a failing case to a single `"\r"` and reported it. I added the empty-list check (bgl_parser.py:53). Generalizable lesson: defensive coding at parse boundaries is invisible until property tests expose it.

### G3. *"What's your test coverage?"*
About 82%, ~200 tests, mypy strict everywhere. Coverage is incidental — the meaningful number is that every concurrency-correctness invariant has a property test, every parser has a `can_parse_never_raises` Hypothesis suite, and every repository implementation passes the same integration test suite. I value invariant coverage over line coverage.

### G4. *"What's not tested?"*
Multi-process scaling (no v1 implementation to test). Real Postgres failover (only tested against ephemeral asyncpg). Long-soak — I've run hours, not days. The Slack/email alerters are stubbed because I didn't build them. None of these are correctness gaps in the current scope; they're the boundary of what's in v1.

### G5. *"How do you test the in-memory and Postgres repositories don't drift?"*
Both implement `ILogRepository` and the same integration test suite runs against both — same fixtures, same assertions on save/retrieve/count. If one drifts in behavior (e.g., Postgres serializes JSON differently), the suite fails on whichever implementation broke. Catches the classic "tests-pass-on-mocks-fail-in-prod" trap I've burned hours on before.

---

## Section H — KLA domain bridging

### H1. *"How does this map to EBeam tool logs?"*
EBeam logs almost certainly contain both anomaly shapes — point (single-line hardware faults, like an interlock trip) and sequence (multi-step recipe execution that deviates). The architecture is already shaped for both. I'd plug in an EBeam-specific parser (Strategy), define an event template set for the recipe steps (matcher), sessionize by tool-or-wafer-id, and run the existing sequence detector on top. The infrastructure is reusable; the domain knowledge is the new work.

### H2. *"How would real-time stream input change the design?"*
Less than you'd expect. The source becomes a `KafkaLogSource` or `SocketLogSource` behind the same `ILogSource` interface. Sessionizer timeouts shift from log-time-based to wall-time-based (one config change). Detection becomes online — updating distributions as events arrive instead of scoring closed traces. Throughput target gets harder; architectural shape doesn't.

### H3. *"What's the integration story with an existing tool log collector?"*
Two options. (1) Source-side: implement `ILogSource` against the collector's API (Splunk forwarder, Fluent Bit tail, etc.) — same interface as the file source. (2) Output-side: wire an EventBus listener that re-emits `AnomalyEvent` to whatever alerting system already exists (PagerDuty, ServiceNow, Slack). Both are additive — no changes to the core pipeline.

### H4. *"What labeling effort would KLA need to invest to use this?"*
Sequence detection needs a corpus of known-normal sessions to learn from — couple of weeks of clean production logs would be enough. No anomaly labels needed for training (one-class). For evaluation, a few hundred labeled anomalous sessions to compute precision/recall. Point detection needs less — IQR/Z-score are unsupervised; only threshold tuning needs validation labels.

### H5. *"Could this run on tool-side hardware or only on a central server?"*
Either. The single-process executor fits on modest hardware (250 MB resident at 8 workers). For tool-side, in-memory repository + local Postgres + file source. For central, swap the source to Kafka and run multiple processes. The configurable runtime (`num_workers`, `queue_size`, `batch_size` in YAML) lets you size to whatever box you've got.

---

## Section I — Honest self-critique & what I'd do next

### I1. *"If you had another week, what would you fix first?"*
KL-divergence sequence detector. F1=0.44 is the most prominent visible result, and the v2 lift to ~0.85 is one module of math — not new architecture, not a rewrite. About 200 lines, fits behind the same `ISequenceDetector` interface. Highest reward-to-effort ratio of anything left.

### I2. *"What did you not get to?"*
Three things. (1) Web UI — low risk, low signal, didn't demonstrate anything new. (2) Distributed multi-process executor — a 2-week project on its own, would need real load testing. (3) Feature-engineered BGL detector that would move F1 above 0.5 — the most painful omission because it's tractable. I prioritized architectural depth over breadth of detector menu.

### I3. *"What surprised you during the project?"*
The wildcard syntax bug. Loghub publishes HDFS templates in two files — the sample file uses `<*>`, the full corpus uses `[*]`. My first full-corpus training produced zero matches across 11 M lines. One-line regex fix, but the lesson is: sanity-check intermediate counts before trusting any downstream metric. Now my evaluation script asserts non-zero template matches before computing F1.

### I4. *"What would you do differently if starting over?"*
Build the evaluation harness first, before any detector. I built detectors first and then realized I had no clean way to compute precision/recall reproducibly. The two days I spent retrofitting `evaluate_hdfs_sequence.py` could have been one day building it upfront — and the detectors would have been better because I'd have measured them from day one rather than tuned by gut.

### I5. *"Where's the most unsafe code in the project?"*
The Postgres connection pool has no explicit configuration (`postgres_repo.py:105`) — relies on asyncpg defaults (5–10 connections). Under sustained high write load with multiple workers, that pool will saturate before the queue does, and the backpressure story breaks because flushes will start failing rather than slowing. Fix is explicit pool sizing tied to `num_workers`. Tracked as v2.

### I6. *"What's something the code does that would confuse a new contributor?"*
The sequence detector's `severity_score` uses set-deduplicated bigrams, not raw bigram count. A trace with 10 copies of one novel pair scores the same as a trace with 1 copy. Defensible — the *unique* novelty matters — but counterintuitive. I'd add a docstring example. Stretch: a "frequency-weighted" mode would be the natural v2.

---

## Section J — Quick-fire facts (memorize these)

| Question | Answer |
|---|---|
| HDFS lines | 11,175,629 |
| HDFS blocks (labeled) | 575,061 — 558,223 Normal, 16,838 Anomaly (2.93%) |
| HDFS templates | 29 (E1–E29) |
| HDFS normal 2-grams | 137 |
| HDFS results | P=1.0, R=0.285, F1=0.4435 |
| BGL lines | 4,747,963 — ~7.3% anomalous |
| BGL alert codes | 30+ (KERNDTLB leads at 152,734) |
| Throughput | 46,662 rec/s at 4 workers |
| Latency p99 | 8.5 µs |
| Default queue size | 1000 |
| Default workers | 4 |
| Default batch size | 100 |
| Sessionizer idle window | 30 s |
| Janitor interval | 5 s |
| Max open traces (LRU) | 50,000 |
| Retry: max attempts | 3 |
| Retry: base/cap | 50 ms / 1 s |
| Tests | ~200, ~82% coverage, mypy strict |
| Code size | Builder ~220 LOC, Sessionizer ~275 LOC, Sequence detector ~150 LOC |

---

## Things NOT to say

- "Just" — "I just used Pydantic for..." downplays the work.
- "Simple" — "It's a simple bounded queue..." it's not simple, it's carefully chosen.
- "Hopefully" — replace with "I've verified that..."
- "That's a good question" — stalling phrase, every panel hears it.
- "I think" — when I do know, say so; when I don't, "I don't know, but here's how I'd find out."
