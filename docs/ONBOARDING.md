# PipelineX — Onboarding Guide for New Developers

> **Audience:** A new junior developer who just joined the team. You know Python.
> You may not know asyncio deeply, may not have seen large-scale log analytics
> before, and you certainly haven't seen this codebase. By the end of this
> document you should be able to: (a) draw the system on a whiteboard, (b)
> explain why every major architectural choice was made, and (c) trace a single
> log line through every phase of the pipeline.

---

## Table of contents

1. [What problem does PipelineX solve?](#1-what-problem-does-pipelinex-solve)
2. [The 30-second mental model](#2-the-30-second-mental-model)
3. [The two datasets — what real data looks like](#3-the-two-datasets--what-real-data-looks-like)
4. [Phase A — Ingestion: getting raw bytes into the system](#4-phase-a--ingestion-getting-raw-bytes-into-the-system)
5. [Phase B — Parsing: turning a string into a `LogRecord`](#5-phase-b--parsing-turning-a-string-into-a-logrecord)
6. [Phase C — Validation: rejecting bad records cheaply](#6-phase-c--validation-rejecting-bad-records-cheaply)
7. [Phase D — Enrichment: adding derived context](#7-phase-d--enrichment-adding-derived-context)
8. [Phase E — Sessionization: grouping per-line records into block traces](#8-phase-e--sessionization-grouping-per-line-records-into-block-traces)
9. [Phase F — Detection: finding anomalies](#9-phase-f--detection-finding-anomalies)
10. [Phase G — Persistence: durably storing results](#10-phase-g--persistence-durably-storing-results)
11. [Phase H — API & Query: getting results back out](#11-phase-h--api--query-getting-results-back-out)
12. [The cross-cutting concerns](#12-the-cross-cutting-concerns)
13. [The async concurrency engine, walked end-to-end](#13-the-async-concurrency-engine-walked-end-to-end)
14. [Configuration: how YAML wires the pipeline](#14-configuration-how-yaml-wires-the-pipeline)
15. [Testing strategy: how we know it works](#15-testing-strategy-how-we-know-it-works)
16. [The numbers — what we measured, and what they mean](#16-the-numbers--what-we-measured-and-what-they-mean)
17. [How to onboard your hands: a 90-minute exercise](#17-how-to-onboard-your-hands-a-90-minute-exercise)
18. [Glossary](#18-glossary)

---

## 1. What problem does PipelineX solve?

Imagine you're operating a 200-node Hadoop cluster (the kind of thing KLA's
EBeam tools generate alongside themselves). Each node emits log lines at maybe
50 lines/second. That's **10,000 lines per second**, every second, forever.
Most of it is fine. A tiny fraction — maybe 3% — indicates something has gone
wrong: a disk failing, a block that didn't replicate, a kernel panic on a
compute node.

The naive approach — `tail -f /var/log/* | grep ERROR` — falls apart for three
reasons:

1. **Volume.** 10K lines/sec × 86,400 sec/day = ~1 billion lines/day. `grep`
   isn't the bottleneck; storage and structure are.
2. **Structure.** "ERROR" appears in plenty of innocuous lines. Real anomalies
   are often *patterns*: a block-write sequence that's truncated, a rate of
   events that spikes, a sequence of events in the wrong order.
3. **Heterogeneity.** Different systems emit different log formats. HDFS looks
   nothing like a Blue Gene/L supercomputer log, which looks nothing like
   nginx. A useful tool can't be hard-coded to one format.

PipelineX is the **structured-pipeline-with-pluggable-stages** answer:

```
       raw lines                  structured records             durable storage
            │                              │                            │
            ▼                              ▼                            ▼
   ┌────────────────┐   ┌─────────────────────────────────┐   ┌──────────────────┐
   │  ILogSource    │──▶│   Parse → Validate → Enrich     │──▶│  ILogRepository  │
   │  (file/stdin/  │   │   → Sessionize → Detect         │   │  (Postgres or    │
   │   synthetic)   │   │                                  │   │   in-memory)     │
   └────────────────┘   └─────────────────────────────────┘   └──────────────────┘
                                       │
                                       │ (anomalies + traces)
                                       ▼
                                ┌──────────────┐
                                │   EventBus   │
                                │  fan-out:    │
                                │  - sequence  │
                                │  - console   │
                                │  - metrics   │
                                └──────────────┘
```

Every box is **swappable**. Adding a new log format = one new class implementing
`IParser`, register it in the factory, point YAML at it. No core changes.

---

## 2. The 30-second mental model

Three sentences:

1. **A producer reads raw log lines and puts them into a bounded asyncio.Queue.**
2. **N consumer workers pull from the queue, run each line through a chain of stages, and accumulate batches.**
3. **When a batch fills (or a flush timer fires) it goes to the repository for durable storage.**

That's it. The bounded queue is what makes this work under load — when the
producer outruns the consumers, it blocks on `queue.put()` instead of filling
RAM. We call this **backpressure** and it's the single most important property
of the system. (More on this in §13.)

Everything else — parsers, validators, detectors, the event bus, the API — is
either *what gets done inside one consumer* or *what happens to the results
afterwards*.

---

## 3. The two datasets — what real data looks like

Before any code, you need to **see what we're actually parsing**. Open these
files yourself.

### 3.1 HDFS_v1 — the "sequence anomaly" dataset

A real Hadoop cluster log. **11.17 million lines, 1.47 GB**, recorded by
Wei Xu et al. at UC Berkeley (2009). Three lines, picked at random from
`data/HDFS_v1_sample/HDFS_2k.log`:

```
081109 203615 148 INFO dfs.DataNode$PacketResponder: PacketResponder 1 for block blk_38865049064139660 terminating
081109 203807 222 INFO dfs.DataNode$PacketResponder: PacketResponder 0 for block blk_-6952295868487656571 terminating
081109 204005 35  INFO dfs.FSNamesystem: BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.251.73.220:50010 is added to blk_7128370237687728475 size 67108864
```

**Parse this in your head.** Each line is:

- `081109` — date (Nov 9 2008, in `yymmdd`)
- `203615` — time (`HHMMSS`)
- `148` — process ID
- `INFO` — severity
- `dfs.DataNode$PacketResponder` — component (the dollar-sign means an inner Java class)
- `PacketResponder 1 for block blk_38865049064139660 terminating` — free-form message

The crucial piece: every line that touches a block contains a reference like
`blk_<some_id>`. **The "anomaly" in HDFS is not a single line being weird — it's
the entire *sequence* of lines about one block being weird.**

For example, a normal block lifecycle looks like:
```
allocate → start receiving → receive packets... → terminate → close → delete
   E22         E5              E26 × N              E11        E9     E21
```

An anomalous block might look like:
```
allocate → start receiving → receive packets... → terminate
   E22         E5              E26 × N              E11      ← truncated! no E9
```

It's *missing* the terminal event. The individual lines are all syntactically
valid; the *pattern* is wrong. This is why you need **sessionization** (group
lines by `blk_id`) and **sequence detection** (look at the pattern of event IDs
in a group).

The ground truth labels are in `anomaly_label.csv`:

```
BlockId,Label
blk_-1608999687919862906,Normal
blk_7503483334202473044,Normal
blk_-3544583377289625738,Anomaly
...
```

575,061 blocks total; 16,838 (~2.93%) labeled Anomaly. Realistic class imbalance.

### 3.2 BGL — the "point anomaly" dataset

A real supercomputer log from Lawrence Livermore Nat. Lab. (Blue Gene/L).
**4.75 million lines, 709 MB**. Three lines from `data/samples/BGL_sample_100.log`:

```
- 1117838570 2005.06.03 R02-M1-N0-C:J12-U11 2005-06-03-15.42.50.363779 R02-M1-N0-C:J12-U11 RAS KERNEL INFO instruction cache parity error corrected
- 1117838570 2005.06.03 R02-M1-N0-C:J12-U11 2005-06-03-15.42.50.527847 R02-M1-N0-C:J12-U11 RAS KERNEL INFO instruction cache parity error corrected
KERNDTLB 1117838611 2005.06.03 R23-M1-N6-I:J18-U01 2005-06-03-15.43.31.041218 R23-M1-N6-I:J18-U01 RAS KERNEL FATAL data TLB error interrupt
```

Notice the **first token**:

- Line 1 and 2 begin with `-` → normal lines
- Line 3 begins with `KERNDTLB` → an alert (kernel data TLB error)

In BGL, **each line carries its own label**. Anomalies are per-line, not
per-group. Roughly 7% of all lines are alerts.

This is a fundamentally different anomaly model from HDFS. And **that's the
point**: a single pipeline that handles both proves the architecture is
genuinely pluggable.

### 3.3 Why two datasets, in one sentence

> HDFS exercises sessionization + sequence detection; BGL exercises point
> detection over a numeric stream. One dataset wouldn't prove the architecture
> handles both.

---

## 4. Phase A — Ingestion: getting raw bytes into the system

**Module:** `src/pipelinex/core/sources.py`
**Interface:** `ILogSource` in `src/pipelinex/core/interfaces.py`

### 4.1 The interface

```python
class ILogSource(ABC):
    @abstractmethod
    async def stream(self) -> AsyncIterator[str]:
        """Yield raw log lines as strings."""
```

That's the entire contract. A source yields strings. It doesn't care what
happens next.

### 4.2 The three implementations

| Implementation | What it does | When you'd use it |
|---|---|---|
| `FileLogSource(path)` | Reads a file line-by-line; auto-detects `.gz` / `.zip` / plain | Production: replaying historical logs, batch processing |
| `StdinLogSource()` | Yields from `sys.stdin` | `cat foo.log \| pipelinex run` |
| `SyntheticLogSource(config)` | Generates fake but realistic log lines on demand | Benchmarks, load tests — you control the rate |

### 4.3 Why this design?

**Source-of-the-data is orthogonal to format-of-the-data.** A file might
contain HDFS lines, or BGL lines, or nginx lines. The source just hands you
the bytes; the parser (next phase) figures out the format.

If we merged source + parser, we'd end up with `HDFSFileSource`,
`HDFSStdinSource`, `BGLFileSource`, `BGLStdinSource`, ... a combinatorial
explosion. Keeping them orthogonal is the **Single Responsibility Principle**
in practice.

### 4.4 Real-world example

```python
source = FileLogSource(Path("data/HDFS/HDFS.log"))
async for line in source.stream():
    print(line)
    # 081109 203615 148 INFO dfs.DataNode$PacketResponder: ...
    # 081109 203615 149 INFO dfs.DataNode$DataXceiver: ...
    # ...
```

That's it. The source is a generator. Nothing fancy.

---

## 5. Phase B — Parsing: turning a string into a `LogRecord`

**Modules:** `src/pipelinex/stages/parsing/`
**Interface:** `IParser`

### 5.1 The `LogRecord` data type

Every parser produces a `LogRecord` — our canonical, format-agnostic
representation of one log line. From `src/pipelinex/core/models.py`:

```python
@dataclass
class LogRecord:
    id: UUID
    timestamp: datetime
    severity: Severity        # DEBUG | INFO | WARNING | ERROR | CRITICAL
    source: str               # component name, e.g. "dfs.DataNode"
    message: str              # the free-form content
    raw_payload: dict         # everything else parser-specific
    enrichment: dict          # populated by later stages
```

The whole point of the pipeline is to turn **strings → `LogRecord`s** as
quickly and reliably as possible, and then let every downstream stage operate
on the same type regardless of where the data came from.

### 5.2 Walking through HDFS parsing

Take this input:
```
081109 203615 148 INFO dfs.DataNode$PacketResponder: PacketResponder 1 for block blk_38865049064139660 terminating
```

`HDFSParser` runs this regex against it:
```python
r"^(?P<date>\d{6}) (?P<time>\d{6}) (?P<pid>\d+) (?P<level>\w+) (?P<component>[\w$.]+): (?P<content>.*)$"
```

That gives us:

| Group | Value |
|---|---|
| `date` | `081109` |
| `time` | `203615` |
| `pid` | `148` |
| `level` | `INFO` |
| `component` | `dfs.DataNode$PacketResponder` |
| `content` | `PacketResponder 1 for block blk_38865049064139660 terminating` |

The parser then:

1. Parses `date + time` as `datetime(2008, 11, 9, 20, 36, 15)`.
2. Maps `INFO → Severity.INFO`. (Edge cases: `WARN → WARNING`, `FATAL → CRITICAL`.)
3. Extracts the block ID with a second regex: `re.search(r"blk_(-?\d+)", content)` → `blk_38865049064139660`.
4. Stores it in `enrichment["block_id"]`.

The output `LogRecord` looks like:

```python
LogRecord(
    id=UUID('a3f2…'),
    timestamp=datetime(2008, 11, 9, 20, 36, 15),
    severity=Severity.INFO,
    source="dfs.DataNode$PacketResponder",
    message="PacketResponder 1 for block blk_38865049064139660 terminating",
    raw_payload={"pid": "148", "raw": "<original line>"},
    enrichment={"block_id": "blk_38865049064139660"},
)
```

### 5.3 Why `enrichment["block_id"]` in the *parser*?

This is the one piece of dataset-specific logic the parser carries — extracting
the block ID. Could we put it in a separate enrichment stage? Yes. We don't,
because:

- The regex is already running on every HDFS line.
- The block ID is the **primary key** for sessionization. If we put it in a
  later stage and that stage was misconfigured, we'd silently sessionize on
  nothing. Keeping it in the parser makes the parser a hard contract: "every
  HDFS line that mentions a block has block_id in enrichment."

### 5.4 BGL parsing — same shape, different format

For:
```
KERNDTLB 1117838611 2005.06.03 R23-M1-N6-I:J18-U01 2005-06-03-15.43.31.041218 R23-M1-N6-I:J18-U01 RAS KERNEL FATAL data TLB error interrupt
```

`BGLParser` does `str.split(maxsplit=9)`, not a regex. Why? Because the tail
("data TLB error interrupt") is free-form and may contain anything; we don't
want a regex backtracking on every line.

The split gives 10 fields. Field 0 is the label (`KERNDTLB`), field 4 is the
ISO-ish timestamp, field 8 is the severity (`FATAL`). The parser builds the
same `LogRecord` shape, with `enrichment["bgl_label"] = "KERNDTLB"`.

### 5.5 The `ParserFactory`

`src/pipelinex/stages/parsing/factory.py` is a tiny registry:

```python
_PARSERS = {
    "hdfs": HDFSParser,
    "bgl": BGLParser,
    "json": JSONParser,
}

def make_parser(kind: str) -> IParser:
    return _PARSERS[kind]()
```

**This is the Factory pattern.** YAML says `parser: { kind: hdfs }`, the
factory returns an `HDFSParser`, and the pipeline doesn't have to know any
specifics. Adding a new format is *one line in the registry*.

### 5.6 The bug Hypothesis caught

`BGLParser.can_parse(line)` used to do `parts[0] == '-'` without checking that
`parts` was non-empty. The property-based test
`test_can_parse_never_raises` fed it whitespace-only input (`"\r"`), `parts`
came back empty, and `parts[0]` raised `IndexError`. Fix:

```python
parts = line.split(maxsplit=9)
if not parts:
    return False
return parts[0] == '-' or parts[0].isupper()
```

**Lesson for you as a new dev:** Hypothesis isn't just for math problems. It
finds boundary cases your unit tests will never think of.

---

## 6. Phase C — Validation: rejecting bad records cheaply

**Modules:** `src/pipelinex/stages/validation/`
**Interface:** `IValidator`

### 6.1 The interface

```python
class IValidator(ABC):
    @abstractmethod
    def validate(self, record: LogRecord) -> ValidationResult:
        """Return ValidationResult(ok=True) or ValidationResult(ok=False, reason=...)"""
```

### 6.2 The validators

| Validator | What it rejects | Example reject |
|---|---|---|
| `SchemaValidator` | Missing required fields | `LogRecord` with empty `source` |
| `SizeValidator` | Records over a configurable byte limit | `message` longer than 64KB |
| `RateLimitValidator` | More than N records per second per source | A DataNode flooding the pipeline |

### 6.3 The Chain of Responsibility

This is the textbook example of the pattern. Each validator gets a chance to
reject; if it passes, the chain moves on:

```python
class ValidationChain:
    def __init__(self, validators: list[IValidator]):
        self._validators = validators

    def validate(self, record: LogRecord) -> ValidationResult:
        for v in self._validators:
            result = v.validate(record)
            if not result.ok:
                return result   # short-circuit on first rejection
        return ValidationResult(ok=True)
```

### 6.4 Why a chain and not one big `validate()` method?

- **Each validator is testable in isolation.** Unit tests for size limits don't
  need to set up rate-limit state.
- **YAML composes them.** A demo config might have only the schema check; a
  production config layers all three.
- **Adding a 4th validator** (say, GDPR redaction) is a new class — no core changes.

---

## 7. Phase D — Enrichment: adding derived context

**Modules:** `src/pipelinex/stages/enrichment/`

### 7.1 What's enrichment, really?

A `LogRecord` straight out of the parser tells you what the log line *says*.
Enrichment adds derived information the parser couldn't produce on its own —
typically things that need a *lookup table* or a *side computation*.

### 7.2 `TemplateMatcherStage` — the headline enricher

Real Hadoop logs follow a small number of message templates. For example,
`HDFS_2k.log_templates.csv` has 30 templates total (HDFS_v1 has 29). Three of them:

```
E1,<*>:<*> Served block blk_<*> to /<*>
E2,<*>:<*> Starting thread to transfer block blk_<*> to <*>:<*>
E4,BLOCK* ask <*>:<*> to delete  blk_<*>
```

Where `<*>` is a wildcard. The job of `TemplateMatcherStage` is: given a
message, figure out which template it matches and record the **event ID**.

For our line `PacketResponder 1 for block blk_38865049064139660 terminating`,
the matcher would find that this matches the template `PacketResponder <*> for
block blk_<*> terminating` (which is, say, E10), and add:

```python
enrichment["event_id"] = "E10"
```

That `event_id` is what the **sequence detector** later groups into 2-grams.

### 7.3 The wildcard bug — memorize this for the interview

The loghub project provides templates in **two different files** for HDFS:

- The 2k-line preprocessed sample uses `<*>` as the wildcard.
- The full-corpus templates file uses `[*]`.

I (Claude) wrote the regex once with only `<*>` support. First training run on
the full corpus produced **zero template matches**. Total. Fix:

```python
_WILDCARD = re.compile(r"<\*>|\[\*\]")  # handle both
```

This is a great example of **how datasets surprise you** — you can't tell from
reading a README that two files in the same project use different conventions.
You only find it when matched-count = 0 doesn't pass the sanity check.

### 7.4 Why no Drain / no ML?

[Drain](https://github.com/logpai/Drain3) is the standard log-template miner.
We don't use it because:

- We **already have the templates** for both datasets (loghub publishes them).
- Drain adds a heavyweight dependency for a problem we don't have.
- A precompiled regex list of 30 templates is ~50 lines of code and runs in microseconds.

**Engineering principle:** don't bring in a library to solve a problem you've
already solved with `re`.

---

## 8. Phase E — Sessionization: grouping per-line records into block traces

**Module:** `src/pipelinex/stages/sessionizing/block_sessionizer.py`
**This is the most novel component in the project. Read it twice.**

### 8.1 The problem

HDFS anomalies are about *sequences*. A single line is just a line. To detect
the "missing terminal event" anomaly, you need to look at *all* the lines for
one block.

But you're streaming. You can't load 11M lines into RAM and group them.

### 8.2 The data structure: `BlockTrace`

From `models.py`:

```python
@dataclass(frozen=True)
class BlockTrace:
    block_id: str
    event_sequence: tuple[str, ...]    # e.g. ("E22", "E5", "E26", "E26", "E11", "E9")
    record_ids: tuple[UUID, ...]       # NOT full LogRecords — see below
    first_timestamp: datetime
    last_timestamp: datetime
    record_count: int
    pipeline_run_id: UUID
    closed_reason: str                  # "terminal" | "timeout" | "shutdown" | "evicted"
```

**The single most important design decision in the whole project:** `record_ids` is
`tuple[UUID, ...]`, **not** `tuple[LogRecord, ...]`.

Why? Math:

- HDFS_v1 has ~575,000 blocks.
- Each block averages ~19 log lines.
- That's ~11 million `LogRecord`s.
- Each `LogRecord` is hundreds of bytes minimum (UUID + datetime + several strings + two dicts).
- Pinning all of them in open traces = **multi-gigabyte resident set**.

By storing **only the UUID references**, an open trace is ~50 bytes regardless
of how long the trace is. If a detector needs the full records, it queries the
repository.

**This is the kind of decision that separates a toy from a system.** Always
ask: "what does this look like at 1000× the test data?"

### 8.3 The emission policy

When do we "close" a trace and emit it? Four triggers:

1. **Timeout (the primary trigger).** A janitor coroutine scans every 5
   seconds. Any trace whose `last_timestamp` is more than 30 log-seconds old
   gets emitted with `closed_reason="timeout"`.

2. **Terminal-event hint.** If we see `event_id` E9 or E21 (block close /
   delete), mark the trace "likely complete". It still waits one janitor pass
   in case there are out-of-order arrivals. So `closed_reason` stays `timeout`
   but emission happens faster.

3. **Hard cap.** If a single block accumulates more than 1000 events, force-emit
   and start fresh. This caps the "one runaway block" failure mode.

4. **Shutdown.** When the pipeline shuts down, emit *all* open traces with
   `closed_reason="shutdown"`. This is what the property test relies on —
   records-conservation requires that no in-flight trace is lost.

5. **LRU eviction.** If open-trace count exceeds `max_open_traces` (50K
   default), force-emit the oldest. Bounded memory contract.

### 8.4 Why not terminal-only emission?

Tempting design: "emit when you see E9". But **anomalous traces by definition
often miss the terminal event**. Terminal-only emission would silently drop
the most interesting cases. Memorize this — it's a classic interview question
("how do you know your detector will see the bad data?").

### 8.5 The tee pattern — sessionizer doesn't transform

Other stages transform: parser turns string → LogRecord, validator may reject.
The sessionizer **passes the record through unchanged**:

```python
async def process(self, record: LogRecord) -> LogRecord:
    block_id = record.enrichment.get("block_id")
    if block_id:
        self._add_to_trace(block_id, record)  # mutate internal state
    return record  # pass through unchanged
```

The trace is a *derived* artifact, published on the `EventBus` as a
`BlockTraceClosed` event when the trace closes. Per-line records continue
downstream to the repository. **Persistence and sessionization are
independent.**

This is the Observer pattern earning its place. Two listeners can subscribe to
`BlockTraceClosed`: the sequence detector (for analysis) and the repository
(for trace-level storage). Neither knows the other exists.

---

## 9. Phase F — Detection: finding anomalies

**Modules:** `src/pipelinex/detectors/`

We have **four detectors** with deliberately different mathematical foundations:

| Detector | Operates on | Math | Used for |
|---|---|---|---|
| `ZScoreDetector` | Numeric stream | `(x - μ) / σ > threshold` | BGL event-rate windows |
| `IQRDetector` | Numeric stream | Tukey fences: outside `Q1 - 1.5·IQR` or `Q3 + 1.5·IQR` | BGL event-rate windows |
| `CUSUMDetector` | Numeric stream | Cumulative sum of deviations | BGL drift detection |
| `SequenceAnomalyDetector` | `BlockTrace` | Novel 2-gram detection | HDFS block traces |

### 9.1 Z-Score, IQR, CUSUM — the point detectors

All three implement `IAnomalyDetector`:

```python
class IAnomalyDetector(ABC):
    async def detect(self, record: LogRecord) -> AnomalyEvent | None:
        """Return AnomalyEvent if record is anomalous, else None."""
```

But they have **opposite assumptions** about what "anomalous" means:

- **Z-Score assumes a normal distribution.** Good for symmetric, bell-curved data; sensitive to outliers in the training data.
- **IQR is non-parametric.** Doesn't assume a distribution. More robust to skewed data.
- **CUSUM detects *drift*, not outliers.** Fires when the running mean shifts, even if no single point is extreme.

For BGL, we bin events into 1-minute windows and feed the per-window count to
each detector. Real numbers from `BENCHMARKS.md`:

| Detector | Precision | Recall | F1 |
|---|---|---|---|
| Z-Score | 0.16 | 0.09 | 0.11 |
| IQR | 0.16 | 0.28 | 0.20 |
| CUSUM | 0.03 | 0.02 | 0.03 |

**Low F1 is not a bug — it's the honest finding.** BGL alerts don't correlate
well with rate spikes; they're individual lines mixed into otherwise-normal
traffic. This is *exactly* the result that justifies pairing point detectors
with sequence detectors. Different anomaly models need different detection
methods.

### 9.2 SequenceAnomalyDetector — the n-gram

For HDFS we operate on a different level — a whole `BlockTrace` at a time. So
it implements a *sibling* interface:

```python
class ISequenceAnomalyDetector(ABC):
    async def detect_trace(self, trace: BlockTrace) -> AnomalyEvent | None: ...
```

**Why a sibling interface and not a unified one?** Wrapping a trace inside a
synthetic `LogRecord` to fit one interface would be lying in the type system.
30 lines of honest typing beats 100 lines of clever indirection.

#### How n-gram detection works

A 2-gram is a pair of consecutive events. For the normal sequence:
```
E22 → E5 → E26 → E11 → E9
```
The 2-grams are:
```
(E22, E5), (E5, E26), (E26, E11), (E11, E9)
```

**Training:** read all labeled-Normal traces, collect every 2-gram that ever
appears, save the set to disk. We end up with 137 distinct "normal" 2-grams
(out of ~841 possible: 29 × 29 = 841 — most never co-occur).

**Detection:** for a new trace, compute its 2-grams. If *any* of them are not
in the normal set, fire an anomaly.

#### The headline result

```
detector          blocks_scored   TP    FP   FN     precision  recall  F1
sequence_ngram    575,061         4,798  0    12,040  1.0000     0.2850  0.4435
```

**Precision = 1.0.** Every alarm is correct, because by construction we only
fire on bigrams that *never* appear in any normal block.

**Recall = 0.285.** We miss ~71% of true anomalies. Why? Because most
anomalous traces reuse the *same* 2-grams as normal traces, just in different
counts or orderings. To detect those we'd need a richer scoring function — KL
divergence over 2-gram frequencies, or 3-grams, or a fully Bayesian model.
**v1 scope is deliberate**; the lift to v2 is well-defined.

This is the kind of result you should *want* in an interview. "Recall is 0.28
and here's exactly why, and here's what v2 looks like" demonstrates that you
understand both the math and your own design choices.

### 9.3 The DetectorFactory

Same pattern as ParserFactory:

```python
_DETECTORS = {
    "z_score": ZScoreDetector,
    "iqr": IQRDetector,
    "cusum": CUSUMDetector,
    "sequence_ngram": SequenceAnomalyDetector,
}
```

YAML wires it up.

---

## 10. Phase G — Persistence: durably storing results

**Modules:** `src/pipelinex/persistence/`
**Interface:** `ILogRepository`

### 10.1 The interface

```python
class ILogRepository(ABC):
    async def save_batch(self, records: list[LogRecord]) -> None: ...
    async def save_trace(self, trace: BlockTrace) -> None: ...
    async def save_anomaly(self, event: AnomalyEvent) -> None: ...
    async def query_records(self, ...): ...
    async def query_anomalies(self, ...): ...
```

### 10.2 Two implementations

| Implementation | Use case |
|---|---|
| `InMemoryLogRepository` | Tests, benchmarks, local development |
| `PostgresLogRepository` | Production — uses SQLAlchemy 2.0 async + asyncpg |

Both pass the **same integration test suite** (gated on `POSTGRES_DSN`). This
is the **Liskov Substitution Principle** verified by tests: if your tests pass
against one, they pass against the other.

### 10.3 Why batch?

Single-row `INSERT`s into Postgres at 46K records/sec would melt the database
and consume the network. The pipeline accumulates batches of (say) 500
records, then issues one `INSERT … VALUES (…), (…), …`. Three orders of
magnitude faster than row-at-a-time.

### 10.4 The schema

`schema.sql` defines three tables:

```sql
CREATE TABLE log_records (
    id UUID PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    severity VARCHAR(10),
    source VARCHAR(255),
    message TEXT,
    enrichment JSONB,
    pipeline_run_id UUID
);

CREATE TABLE block_traces (
    block_id VARCHAR(64) PRIMARY KEY,
    event_sequence TEXT[],
    record_count INT,
    first_timestamp TIMESTAMPTZ,
    last_timestamp TIMESTAMPTZ,
    closed_reason VARCHAR(20),
    pipeline_run_id UUID
);

CREATE TABLE anomaly_events (
    id UUID PRIMARY KEY,
    detector_name VARCHAR(64),
    severity_score FLOAT,
    log_record_id UUID,
    metadata JSONB,
    pipeline_run_id UUID
);
```

Indexes on `timestamp` and `pipeline_run_id` — the two columns every common
query filters on.

---

## 11. Phase H — API & Query: getting results back out

**Modules:** `src/pipelinex/api/`

### 11.1 FastAPI app

`app.py` constructs the FastAPI instance and wires the repository into
`app.state.repository` at startup. Routes get the repo via dependency
injection:

```python
def get_repo(request: Request) -> ILogRepository:
    return request.app.state.repository
```

This is **not** a global — it's per-app-instance state, which means tests can
instantiate the app with a fake repository without monkeypatching.

### 11.2 The endpoints

| Endpoint | Returns |
|---|---|
| `GET /health` | `{"status": "ok"}` |
| `GET /records?severity=ERROR&since=...` | List of `LogRecord`s |
| `GET /anomalies?detector=sequence_ngram` | List of `AnomalyEvent`s |
| `GET /traces/{block_id}` | One `BlockTrace` |

All response schemas are Pydantic models in `schemas.py`. No raw dicts leak
out.

### 11.3 Why not flask/django?

FastAPI gives us async-native routing (so we can `await repo.query(...)`
without thread pools), automatic OpenAPI docs, and Pydantic validation
end-to-end. It's the right tool for the job.

---

## 12. The cross-cutting concerns

These aren't a phase — they apply *across* every phase.

### 12.1 EventBus (`src/pipelinex/events/`)

A publish/subscribe bus. Anything in the system can publish an event; anything
can subscribe.

```python
class EventBus:
    async def publish(self, event: Event) -> None: ...
    def subscribe(self, event_type: type, listener: IEventListener) -> None: ...
```

We use it for:

- `BlockTraceClosed` — sessionizer publishes; sequence detector + console alerter subscribe
- `AnomalyDetected` — detectors publish; alerting + metrics subscribe

**Why an event bus when there's only a few subscribers?** Decoupling. Adding a
Slack alerter is a new class implementing `IEventListener`, registered in the
builder. Zero core changes.

### 12.2 Decorators (`src/pipelinex/stages/decorators/`)

Three decorators, applied to any stage via YAML:

- `TimingDecorator` — measures execution time, publishes to metrics
- `RetryDecorator` — retries on `TransientStageError`, fails on `FatalStageError`
- `LoggingDecorator` — structured logs around each call

The classic Decorator pattern. Apply timing to a new stage = one YAML line, not
a code change.

### 12.3 MetricsRegistry (`src/pipelinex/observability/`)

A simple in-memory counter / histogram store. Records:
- `records_processed_total{stage=…}`
- `records_failed_total{stage=…, reason=…}`
- `stage_duration_seconds{stage=…}` (histogram)
- `queue_depth` (gauge)

Exposed via `/metrics` for scraping. Plain dict + `collections.Counter`; no
Prometheus dependency.

---

## 13. The async concurrency engine, walked end-to-end

**Module:** `src/pipelinex/core/pipeline.py`

This is where everything comes together. The diagram:

```
                ┌──────────────────────┐
                │   Producer task      │
                │  reads source.stream │
                │  awaits queue.put    │
                └──────────┬───────────┘
                           │
                           ▼
                ┌──────────────────────┐
                │   asyncio.Queue      │  maxsize=N (bounded!)
                │   FIFO, awaitable    │
                └──────────┬───────────┘
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
        ▼                  ▼                  ▼
   ┌─────────┐        ┌─────────┐        ┌─────────┐
   │worker 1 │        │worker 2 │  …     │worker N │
   │  parse  │        │  parse  │        │  parse  │
   │ validate│        │ validate│        │ validate│
   │ enrich  │        │ enrich  │        │ enrich  │
   │ session │        │ session │        │ session │
   │ detect  │        │ detect  │        │ detect  │
   └────┬────┘        └────┬────┘        └────┬────┘
        │                  │                  │
        └────────┬─────────┴─────────┬────────┘
                 │                   │
                 ▼                   ▼
          ┌─────────────┐    ┌─────────────┐
          │   batch     │    │   event     │
          │   buffer    │    │   bus       │
          │   ↓ flush   │    │   fanout    │
          │ repository  │    │             │
          └─────────────┘    └─────────────┘
```

### 13.1 The producer

```python
async def _producer(self):
    async for line in self._source.stream():
        await self._queue.put(line)
    await self._queue.put(SENTINEL)  # tell consumers we're done
```

Notice `await self._queue.put(...)`. If the queue is full, this blocks. The
producer slows down to match the consumers. **This is backpressure.**

### 13.2 The consumer (worker)

```python
async def _consumer(self, worker_id):
    batch = []
    while True:
        line = await self._queue.get()
        if line is SENTINEL:
            await self._flush(batch)
            return
        try:
            record = await self._stages.process(line)
            batch.append(record)
            if len(batch) >= self._batch_size:
                await self._flush(batch)
                batch = []
        except FatalStageError:
            raise  # tear down the whole pipeline
        except TransientStageError as e:
            self._metrics.inc("records_failed_total", reason=type(e).__name__)
```

N of these run concurrently.

### 13.3 The error classification

Two custom exception types:

- **`FatalStageError`** — config is broken, schema is wrong, something
  catastrophic. Tear down. Don't try to recover.
- **`TransientStageError`** — this one record failed for an isolatable reason
  (malformed line, validator rejected). Increment a counter, drop the record,
  carry on.

You **must** be able to tell these apart by class, not by string-matching the
message. That's why they're separate types.

### 13.4 The bounded queue is everything

If the queue were unbounded:

- Producer fills it as fast as it can.
- If consumers are slower than the producer (slow DB, slow disk, anything), the
  queue grows.
- RAM goes up. And up. And up.
- Eventually: OOM. Process dies. **You lose every record currently in the
  queue.**

With a bounded queue:

- Queue fills to `maxsize`.
- Producer blocks on `put`. It now runs at exactly the consumer's pace.
- Memory is bounded. Records are not lost.

**This is the single most important property of the system.** It's verified by
a Hypothesis property test that varies worker count, queue size, and batch
size, asserting `records_in == records_persisted + records_failed`. Memorize
this property name: **records-conservation invariant**.

### 13.5 Graceful shutdown

When `Ctrl-C` or `pipeline.shutdown()`:

1. Producer stops reading source.
2. Producer puts a SENTINEL on the queue.
3. Workers drain the queue, see SENTINEL, flush their batches, exit.
4. Sessionizer flushes all open traces with `closed_reason="shutdown"`.
5. Repository commits final batch.

No records lost. No partial state in memory.

---

## 14. Configuration: how YAML wires the pipeline

**Module:** `src/pipelinex/core/config.py` (Pydantic models)
**Wiring:** `src/pipelinex/core/builder.py`

### 14.1 A real config

`configs/hdfs_v1.yaml`:

```yaml
source:
  kind: file
  path: data/HDFS/HDFS.log

parser:
  kind: hdfs

stages:
  - kind: validation
    validators:
      - { kind: schema }
      - { kind: size, max_bytes: 65536 }
  - kind: enrichment
    matcher_templates: data/HDFS/preprocessed/HDFS.log_templates.csv
  - kind: sessionizer
    idle_window_s: 30
    max_open_traces: 50000

detectors:
  - kind: sequence_ngram
    model_path: models/hdfs_ngram_v1.json

repository:
  kind: postgres
  dsn: postgresql+asyncpg://user:pass@localhost/pipelinex
  batch_size: 500

pipeline:
  workers: 8
  queue_size: 1000
```

### 14.2 How it gets validated

Pydantic v2 with `extra="forbid"`. If you misspell `idle_window_s` as
`idle_window_seconds`, the config load fails fast with a clear error. No
silent default-fallback bugs.

**One tricky decision:** cross-field validation uses `@model_validator(mode="after")`,
not `@field_validator`. The latter doesn't reliably see sibling fields via
`info.data` in Pydantic v2 — you'd silently end up with `None`. Model
validators fire after the whole object is built; cross-field rules work.

### 14.3 The builder

`build_pipeline(config)` reads the validated config and returns a
`BuiltPipeline(executor, repository, bus, sessionizer, sequence_detector)`. It:

1. Instantiates the source via `make_source(config.source)`.
2. Instantiates the parser via `make_parser(config.parser.kind)`.
3. Instantiates each stage and chains them.
4. Instantiates the repository.
5. Subscribes the sequence detector to `BlockTraceClosed` on the bus.
6. Wires everything into a `PipelineExecutor`.

**This is the composition root.** All the `new ConcreteThing()` calls happen
here. Everything else depends only on interfaces.

---

## 15. Testing strategy: how we know it works

We have ~200 tests across three categories:

### 15.1 Unit tests (`tests/unit/`)

Fast (~12 seconds total). Each tests one class in isolation.

```python
def test_zscore_fires_on_outlier():
    detector = ZScoreDetector(window=100, threshold=3.0)
    for x in normal_distribution_sample(100):
        detector.detect(make_record(value=x))
    result = detector.detect(make_record(value=10_000))
    assert result is not None
    assert result.detector_name == "z_score"
```

### 15.2 Property tests (`tests/property/`)

Hypothesis-based. Two key ones:

- `test_pipeline_records_conservation` — varies workers, queue, batch sizes.
  Asserts `records_in == records_persisted + records_failed` for any
  combination. **This is how we prove backpressure works.**

- `test_can_parse_never_raises` — feeds parsers arbitrary strings; asserts
  `can_parse()` never throws. Caught the BGL whitespace-input bug.

### 15.3 Integration tests (`tests/integration/`)

Two flavors:

- **API tests** — spin up the FastAPI app with an in-memory repo, hit endpoints.
- **Postgres tests** — gated on `POSTGRES_DSN`. Run the same scenarios as the
  in-memory repo to prove Liskov substitution.

### 15.4 The current scoreboard

- 194 tests passing
- 6 Postgres-gated tests skipped (run with `POSTGRES_DSN=...`)
- 82% line coverage overall
- ~91% on production-exercised code

---

## 16. The numbers — what we measured, and what they mean

Read `BENCHMARKS.md` for the full table. The headlines:

### 16.1 Throughput

~46,000 records/second sustained at 8 workers. The plan asked for ≥20K. We're
above target by 2×.

Why does it not scale much beyond 1 worker? At ~50 microseconds of work per
record (parse + queue ops), the **asyncio event loop itself is the
bottleneck**, not stage serialization. CPU-bound parsing of a single regex
fits into the loop with no room to spare.

If we wanted to go higher we'd offload parsing to a `ProcessPoolExecutor` —
but for a portfolio project, sustaining 46K/sec on a laptop is more than
enough.

### 16.2 Latency

p50 = p95 = p99 = 7–8 microseconds. This is the time from `t_ingest` (stamped
when the producer puts the line on the queue) to just-before-batch (stamped
when the record enters the batch buffer). It's **not** end-to-durable — that
would include the Postgres flush time, which is dominated by the database.

### 16.3 Backpressure

Stress test: 5,000 records, queue size 100, slow repo simulating 50ms per
batch. Max observed queue depth: 100/100 (saturated). Records lost: 0.
Producer wall-clock dilated to match consumer pace.

### 16.4 HDFS sequence detection

F1 = 0.4435 (P=1.0, R=0.285) on the full 575,061 labeled blocks.

### 16.5 BGL point detection

Z-Score F1 = 0.11, IQR F1 = 0.20, CUSUM F1 = 0.03 on 27,666 windows.

### 16.6 The honest framing

If a panelist says "F1 of 0.44 isn't very high":

> "Correct — and here's why. We use novel-2-gram detection, which by
> construction has precision 1.0 but limited recall. About 71% of true
> anomalies reuse the same 2-grams as normal traces in different proportions.
> To recover those, v2 would use KL-divergence over 2-gram frequencies or
> 3-grams. The v1 scope is deliberate: prove the architecture end-to-end with
> a baseline whose math is fully understandable, document the lift path."

That answer demonstrates you know the math, know your scope, and have a real
roadmap. **Honest beats impressive.**

---

## 17. How to onboard your hands: a 90-minute exercise

Don't just read this guide. Do this:

### Minute 0–15: Run the smoke test

```bash
cd /Users/devpatel/Desktop/IUB/pipelinex
source .venv/bin/activate
pytest tests/unit -q
```

Watch ~190 tests pass. Skim any output you don't understand.

### Minute 15–30: Trace one log line by hand

Open `data/HDFS_v1_sample/HDFS_2k.log`. Pick the first line. With your
finger:

1. Find which regex in `hdfs_parser.py` matches it.
2. Find which field becomes `enrichment["block_id"]`.
3. Find which template in `HDFS_2k.log_templates.csv` matches the message.
4. Imagine which 2-grams its events contribute to.

### Minute 30–60: Run the pipeline end-to-end on the sample

```bash
pipelinex run configs/hdfs_v1.yaml \
    --source-path data/HDFS_v1_sample/HDFS_2k.log
```

Watch the logs. Notice the worker count, the batch flushes, the BlockTrace
emissions.

### Minute 60–80: Read the executor

Open `src/pipelinex/core/pipeline.py` cold. Read top to bottom. You should be
able to identify:

- The bounded queue
- The producer task
- The N consumer tasks
- The batch flush logic
- The shutdown sequence
- The two exception types

### Minute 80–90: Read the sessionizer

Open `src/pipelinex/stages/sessionizing/block_sessionizer.py`. Identify:

- The OrderedDict (for LRU)
- The janitor coroutine
- The terminal-event hint
- The shutdown flush
- Where `BlockTraceClosed` is published

If you can explain those two files to someone else by the end of the 90
minutes, you're onboarded.

---

## 18. Glossary

| Term | Meaning |
|---|---|
| **Backpressure** | When a consumer is slower than a producer, the producer is forced to slow down (here: by blocking on `queue.put()`) instead of buffering unboundedly. |
| **BlockTrace** | All log lines about a single HDFS block, grouped into one record. The unit of sequence-anomaly detection. |
| **Bounded queue** | An `asyncio.Queue` with a `maxsize`. The producer blocks once it's full. The mechanism that gives us backpressure. |
| **Chain of Responsibility** | Design pattern: a chain of handlers, each can short-circuit. Used in validation. |
| **CUSUM** | Cumulative Sum — a drift detector. Fires when the running mean of a stream shifts, even if no single point is extreme. |
| **Decorator (pattern)** | Wraps a stage with extra behavior (timing, retry, logging) without changing its code. |
| **EventBus** | Publish/subscribe hub. Decouples producers of events from consumers. |
| **Factory** | A function/class that constructs concrete implementations behind an interface, selected by a string key. |
| **HDFS** | Hadoop Distributed File System. Source of our sequence-anomaly dataset. |
| **IQR** | Inter-Quartile Range. Non-parametric outlier detection using Tukey fences. |
| **Liskov Substitution** | If `B` is a subtype of `A`, you can use `B` anywhere you use `A` without surprises. Proven by running the same tests against both implementations. |
| **LogRecord** | Our canonical, format-agnostic representation of one log line. |
| **n-gram** | A sliding window of n consecutive items. We use 2-grams (pairs). |
| **Observer** | Design pattern. Same idea as the EventBus. |
| **Open/Closed Principle** | Open for extension, closed for modification. Add a new parser without touching the core. |
| **Property-based testing** | Test that a *property* holds for *many* inputs (generated by Hypothesis), not just hand-picked examples. |
| **Repository (pattern)** | Encapsulates data access behind an interface. Lets you swap implementations (in-memory vs Postgres). |
| **Sessionization** | Grouping per-line records into logical sessions (here: per-block-trace). |
| **Strategy (pattern)** | A family of interchangeable algorithms behind one interface. Our parsers and detectors are Strategies. |
| **Z-Score** | Parametric outlier detection. `(x - mean) / stddev > threshold`. |

---

## What's NOT in this guide

- The CLI internals (`cli.py`) — it's a thin Click wrapper, read it cold.
- The synthetic generator (`scripts/generate_synthetic_logs.py`) — useful for
  benchmarks; not architecturally interesting.
- The full SQL schema (`schema.sql`) — straightforward, three tables, two
  indexes.

Read those when you need them. They won't change your mental model of the
system.

---

## Final checklist — before you walk into the interview

You should be able to answer **all of these without checking the code**:

- [ ] Draw the pipeline on a whiteboard. Label producer, queue, workers, batch, repo, event bus.
- [ ] Explain why the queue is bounded (not unbounded).
- [ ] Explain the records-conservation invariant.
- [ ] Explain why `BlockTrace.record_ids` is UUIDs, not full records.
- [ ] Explain why we have two detector interfaces, not one.
- [ ] Explain why HDFS recall is 0.28 and why precision is 1.0.
- [ ] Explain why BGL F1 is low and why that's the *right* finding.
- [ ] Explain the difference between `FatalStageError` and `TransientStageError`.
- [ ] Walk through what happens to a single log line from `source.stream()` to `repo.save_batch()`.
- [ ] Explain why we have *two* datasets and what each proves.

When you can do all ten, you own this project. Good luck.
