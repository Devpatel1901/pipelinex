# PipelineX — Technical Presentation
### KLA EBeam Division | Software Engineer Interview

> **Duration:** ~30 min presentation + 15 min Q&A
> **Audience:** KLA software engineers, a technical lead, the director of software engineering
> **Goal:** explain the project clearly to technical people — without going so deep that
> communication suffers. Show that you can simplify complex ideas, not just understand them.

---

## How to use this document

Each section below is **one slide**. The structure is:

- **[SLIDE N — Title]** — what's on the slide visually
- **[SAY]** — the words you actually speak (read it 3–4 times, then speak naturally)
- **[NOTES]** — anchors / things to remember / what the panel may probe

Speak the **[SAY]** lines naturally — don't read. The slide is for the panel; you're the
explainer. Your job is to make the room understand, not to recite bullets.

**Total slide count:** 14 slides over ~33 minutes.

---

## Slide-by-slide timing budget

| # | Slide | Minutes |
|---|---|---|
| 1 | Title & introduction | 2 |
| 2 | Agenda — what I'll cover | 1 |
| 3 | Problem statement | 3 |
| 4 | Architecture diagram | 4 |
| 5 | **Datasets — HDFS & BGL: annotated log lines + anomaly shapes** | 3 |
| 6 | **Config → Pipeline** (YAML to built pipeline, two examples) | 3 |
| 7 | Example trace — HDFS & BGL through every stage | 4 |
| 8 | Concurrency engine | 3 |
| 9 | Error classification & graceful shutdown | 2 |
| 10 | **Sessionizer & KL-divergence sequence detector — deep dive** | 5 |
| 11 | Design patterns — problem → pattern mapping | 3 |
| 12 | Results — HDFS sequence detection | 2 |
| 13 | Results — BGL point detection + throughput | 2 |
| 14 | Summary | 1 |
| 15 | Thank you / Q&A | — |
| | **Total** | **38** |

---

# Slide 1 — Title & Introduction

**Visual:**

> # PipelineX
> ### *Extensible Async Log Analytics Engine*
>
> Dev Patel
> KLA EBeam Division — Software Engineer Interview
> [Date]

**[SAY] (~2 minutes):**
> Good morning, and thank you for having me. My name is Dev Patel, and over the
> past three weeks I built a project I call PipelineX. In two sentences:
> PipelineX is an extensible, asynchronous engine for analyzing log streams. It
> ingests raw log lines, processes them through a configurable chain of stages,
> and detects anomalies — both single-line outliers and pattern-based
> sequence anomalies — using real, labeled, publicly available datasets.
>
> I'll spend about 30 minutes walking you through the problem it solves, the
> architecture I designed, the implementation choices I made, and the
> measurable results I got on real data. I'm happy to take questions any time
> — please jump in.

**[NOTES]**
- Make eye contact. Don't rush. The first 30 seconds set the tone.
- "Real, labeled, publicly available datasets" plants the flag early.

---

# Slide 2 — Agenda

**Visual:**

> ## What I'll cover
>
> 1. **Problem statement** — why log analytics is hard
> 2. **Architecture** — the system at a glance
> 3. **Datasets** — HDFS & BGL — what one log line looks like and what an anomaly looks like
> 4. **Config → Pipeline** — how YAML becomes a running pipeline
> 5. **Example trace** — one HDFS line and one BGL line through every stage
> 6. **Concurrency engine** — how the async pipeline actually runs
> 7. **Error classification & graceful shutdown**
> 8. **Sessionizer & KL-divergence sequence detector** — the deep dive
> 9. **Design patterns** — problem → pattern mapping
> 10. **Results** — HDFS sequence detection, BGL point detection, throughput
> 11. **Summary**

**[SAY] (~1 minute):**
> Here's how I'd like to structure this. I'll start with the problem
> statement — what makes log analytics hard at scale. Then the architecture,
> at a high level. Before going into the implementation, I'll spend a few
> minutes on the two real datasets I evaluated against — HDFS and BGL —
> because the *shape* of their anomalies is what drives every design choice
> in the rest of the talk. After that I'll show how a YAML config becomes a
> running pipeline — two real config files, two real pipeline layouts side
> by side. To make that concrete, I'll walk one HDFS line and one BGL line
> through every single stage. From there I'll go deeper on the concurrency
> engine, and on how I handle errors and shutdown. The piece I'm most proud
> of is the sessionizer paired with the sequence detector, so I'll spend a
> few extra minutes on those together. After that, I'll show how the design
> patterns I chose map to specific problems. Then results, then a short
> summary, then your questions.

**[NOTES]**
- Quick slide. Don't dwell. The agenda's job is to set expectations and
  remove anxiety about "when will he get to X?"

---

# Slide 3 — Problem Statement

**Visual:**

> ## Why log analytics at scale is hard
>
> One real cluster, one real day:
>
> > 200 nodes × ~50 log lines/sec/node = **~10,000 lines/sec**
> > × 86,400 seconds = **~1 billion lines per day**
>
> "tail -f | grep ERROR" fails for three reasons:
>
> | # | Reason | Example |
> |---|---|---|
> | 1 | **Volume** | grep isn't the bottleneck — storage and structure are |
> | 2 | **Structure** | "ERROR" appears in plenty of *normal* lines; real anomalies are *patterns* |
> | 3 | **Heterogeneity** | HDFS, supercomputer, nginx, application logs — all different shapes |

**[SAY] (~3 minutes):**
> Before I show you what I built, let me make the problem concrete. Imagine
> a 200-node Hadoop cluster — the kind of scale that I'd expect from KLA's
> EBeam tools and the supporting infrastructure around them. Each node emits
> roughly 50 log lines per second. That's 10,000 lines per second, every
> second, across the cluster. That works out to about a billion log lines per
> day.
>
> The naive answer to "how do I find problems in those logs?" is
> `tail -f | grep ERROR`. But that breaks down in three ways.
>
> First — **volume**. At a billion lines a day, grep isn't your bottleneck. The
> bottleneck is storing the lines, indexing them, and giving you a way to ask
> structured questions about them.
>
> Second — **structure**. The word "ERROR" appears in plenty of completely
> innocuous lines. The actual anomalies aren't single-line keyword matches —
> they're *patterns*. A block-write sequence that was truncated. An event
> rate that spiked for one minute. A sequence of events that happened in
> the wrong order. None of those are detectable by a regex on one line.
>
> And third — **heterogeneity**. Different systems emit different log formats.
> HDFS looks nothing like a Blue Gene/L supercomputer log. A useful tool
> can't be hard-coded to one format.
>
> So PipelineX is my answer to those three problems: a structured, extensible,
> asynchronous log analytics engine, with measured results on real labeled
> data.

**[NOTES]**
- The three problems are the spine of the entire talk. You'll refer back.
- Tie to KLA: their tools generate this kind of volume. Shows you researched
  the domain.

---

# Slide 4 — Architecture

**Visual:**

```
   ┌──────────────┐  raw   ┌─────────────────────────────────────────────┐  batch   ┌──────────────┐
   │ FileLogSource├───────▶│  Pipeline (N async workers, bounded queue)  ├─────────▶│  Repository  │
   │ StdinSource  │        │  parse → validate → enrich → sessionize     │          │  Postgres or │
   │ Synthetic    │        │       → detectors                            │          │  in-memory   │
   └──────────────┘        └────────────────────┬────────────────────────┘          └──────────────┘
                                                │
                                          BlockTraceClosed
                                                │
                                                ▼
                                          ┌──────────┐    ┌────────────────────┐
                                          │ EventBus ├───▶│ SequenceDetector   │
                                          │          ├───▶│ ConsoleAlerts      │
                                          │          ├───▶│ MetricsListener    │
                                          └──────────┘    └────────────────────┘
```

**[SAY] (~4 minutes):**
> This is the whole system in one diagram. Let me walk it left to right,
> spending about thirty seconds on each piece.
>
> **On the far left, the source.** This is where raw log lines come into the
> system. I have three implementations behind a single interface. A
> file source for reading from disk — that's what I use against the real
> HDFS and BGL datasets. A stdin source for piping logs in. And a synthetic
> source that generates fake but realistic log lines for benchmarks. The
> pipeline doesn't know which source it's reading from — it just sees a
> stream of strings.
>
> **The middle box is the pipeline.** Inside that box, a producer task reads
> from the source and feeds raw lines into a bounded queue. N consumer
> workers — typically 4 or 8 — pull from the queue and run each line
> through a chain of stages. Those stages are: **parse** — turn the string
> into a structured record; **validate** — reject anything malformed;
> **enrich** — add derived context, like which template the message matches;
> **sessionize** — group per-line records into block traces; and **detect** —
> run anomaly detectors over both records and traces. Records accumulate
> into batches as they flow through.
>
> **On the right, the repository.** When a batch fills, the workers flush it
> here. I have two implementations: an in-memory store for tests and
> benchmarks, and a Postgres store for production using SQLAlchemy 2.0
> async. Both pass the same integration tests, so they're truly
> interchangeable.
>
> **And below the main flow, the event bus.** When the sessionizer closes a
> trace, it publishes a `BlockTraceClosed` event. Listeners subscribe — the
> sequence detector is one, a console alerter is another. This is what lets
> me add a Slack notifier or a metrics exporter tomorrow without touching
> the sessionizer.
>
> The key idea is that every box on this diagram is **swappable behind an
> interface**. The core depends only on abstractions. Adding a new log
> format is one new class. Adding a new detector is one new class. I'll
> come back to that on the design-patterns slide.

**[NOTES]**
- Gesture along the diagram physically as you speak. The diagram is your
  ally.
- Don't rush — this is the slide they'll remember after the talk.
- If interrupted with a question here, that's *great* — you're already
  in the technical territory they want.

---

# Slide 5 — Datasets: HDFS & BGL — Anomaly Shapes

> *The panel needs to see what an "anomaly" actually means in each dataset
> before any architecture slide makes sense. One annotated raw line per
> dataset, plus the kinds of anomaly each one contains.*

---

### Why these two datasets — one architecture, two opposite anomaly shapes

> | | **HDFS** | **BGL** |
> |---|---|---|
> | **System** | Hadoop Distributed File System | IBM Blue Gene/L supercomputer |
> | **Anomaly shape** | **Sequence** — the *pattern* of events on one block is wrong | **Point** — one individual line is itself anomalous |

---

### HDFS — what a line looks like, what an anomaly looks like

**Annotated raw line:**
```
081109 203518 143 INFO dfs.DataNode$DataXceiver: Receiving block blk_-1608999687919862906 src: /10.250.19.102:54106 dest: /10.250.19.102:50010
└─┬──┘ └─┬──┘ └┬┘ └─┬┘ └─────────┬─────────┘  └──────────────────────────┬──────────────────────────────────────────┘
 date   time  pid  level     component                                content (free-form, contains block_id)
```

**Kinds of anomaly that exist in HDFS — every individual line still looks normal:**

> | Anomaly pattern | What goes wrong | Why a per-line check can't catch it |
> |---|---|---|
> | **Write never completed** | allocate fires, replicas start receiving, no `Received block … of size` confirmation | Anomaly is an **absent** event, not a present one |
> | **Replication failed mid-flight** | exception on one replica, surviving replicas re-replicate → unusual event order | Every line matches a normal template; the **order** is wrong |
> | **Delete-before-store** | `Deleting block` arrives before `addStoredBlock` for that replica | Each line is benign in isolation; only the **relative order** is wrong |

> **Takeaway:** the anomaly lives in the *sequence of events on one
> block*, not in any single line. HDFS is what forces the pipeline to
> sessionize before scoring.

---

### BGL — what a line looks like, what an anomaly looks like

**Annotated raw line:**
```
KERNDTLB 1117838611 2005.06.03 R23-M1-N6-I:J18-U01 2005-06-03-15.43.31.041218 R23-M1-N6-I:J18-U01 RAS KERNEL FATAL data TLB error interrupt
└──┬───┘ └──┬─────┘ └───┬────┘ └───────┬──────────┘ └──────────┬───────────┘ └─────┬──────────┘ └┬┘ └──┬─┘ └─┬──┘ └──────────┬──────────┘
 label   epoch ts    date       node loc (rack/midplane/    full timestamp    node (repeat)   type  comp  level    content (free-form)
                                  node/card/chip/CPU)
```

**Kinds of anomaly that exist in BGL — each one is locatable to one line:**

> | Alert kind | Example label | What that single line means |
> |---|---|---|
> | **Kernel memory fault** | `KERNDTLB`, `KERNSTOR` | data-TLB or storage error reported by the kernel |
> | **Kernel termination / recovery** | `KERNTERM`, `KERNREC`, `KERNMNTF` | kernel monitor signalled fatal / recovered |
> | **Application failure** | `APPSEV`, `APPREAD` | application severe error / I/O read failure |
> | **Hardware / link / power** | `LINKDISC`, `MMCS`, `MICROCODE`, … | link discovery, microcode, power-supply faults |
> | **Normal** | `-` | nothing is wrong with this line |

> **Takeaway:** every BGL anomaly is locatable to *one* line — no
> sessionization required. BGL is what stresses the per-line and
> per-window point detectors.

---

**[SAY] (~3 minutes):**
> Before I show you the implementation, I want to spend three minutes on
> the two datasets I evaluated against — because the *shape* of the
> anomalies in each one drives every architectural choice in the rest of
> the talk.
>
> The two-row table at the top is the whole reason I chose these two. HDFS
> is Hadoop, BGL is a Blue Gene supercomputer — but what I actually care
> about is the **anomaly shape**. HDFS anomalies are *sequence-shaped*: the
> pattern of events on a block is wrong. BGL anomalies are *point-shaped*:
> one individual line is the anomaly. Two opposite mathematical models —
> and if a single pipeline handles both, the extensibility claim is real,
> not aspirational.
>
> **HDFS first.** Look at the annotated line. There's a date, time, PID,
> level, a Java component name, and a free-form content tail. The crucial
> field is buried in `content`: the **block ID** — `blk_-1608…`. That's
> the sessionization key. Every line for one block must group together
> because the anomaly is across lines, not in any single one.
>
> Look at the anomaly-types table on the right. Three kinds: a write that
> never completed, replication that failed mid-flight, a delete that
> arrived before the store was confirmed. In every case, **each individual
> line still looks normal.** The anomaly is the absent event, or the
> wrong order, or the missing terminal step. A regex on one line cannot
> find any of these — the pipeline has to *reconstruct the sequence first*.
> That is why the HDFS path runs through the sessionizer.
>
> **BGL is the opposite case.** Look at its annotated line. The first
> column — `KERNDTLB` — is the ground-truth label itself, emitted by the
> supercomputer's own RAS monitoring. A dash means normal. Anything else
> is an alert code naming exactly what kind of fault this one line is.
> Kernel TLB errors, kernel terminations, application read failures,
> hardware link faults — the kinds table on the right shows the families.
> Each one is locatable to **one self-contained line**.
>
> That asymmetry is the whole reason this slide exists. HDFS forces
> sessionization plus sequence detection. BGL forces per-line parsing
> plus point detection. Same Strategy-pattern parser interface, two
> different downstream paths — configured purely from YAML. Every
> architectural choice you're about to see is a *response* to something
> on this slide.
>
> And the bridge to KLA: EBeam tool logs almost certainly contain *both*
> shapes simultaneously — single-line hardware faults that are
> point-shaped, and multi-step process recipes that deviate, which are
> sequence-shaped. The reason I picked these two datasets is precisely to
> prove the architecture handles both before applying the same approach
> to a proprietary log.

**[NOTES]**
- Stay on the two anomaly *shapes* — sequence vs. point. The rest of the
  talk leans on that distinction repeatedly (Slides 6, 7, 10, 12, 13).
- If asked "is BGL realistic for modern systems?" — yes. Same shape
  appears in syslog, nginx access logs, any tool log with per-line
  severity codes. BGL is the labelled stand-in.
- If asked about ground-truth provenance — HDFS labels are per-block from
  the published `anomaly_label.csv`; BGL labels are per-line from column
  1 emitted by the system's own RAS monitor. Both are in the data
  READMEs.

---

# Slide 6 — Config → Pipeline (YAML to Built Pipeline)

> *Two YAML files on the left, the resulting pipeline layout on the right,
> and the builder code snippet at the bottom. No prose on the slide — let
> the diagram speak.*

---

### Side-by-side: `hdfs_v1.yaml` → HDFS pipeline

```
┌─────────────────────────────────────┐         ┌────────────────────────────────────────────────┐
│   configs/hdfs_v1.yaml              │         │   Built pipeline layout                        │
│                                     │         │                                                │
│   source:                           │         │                                                │
│     kind: file                      │ ──────▶ │   FileLogSource(path="data/HDFS/HDFS.log")     │
│     path: data/HDFS/HDFS.log        │         │                                                │
│                                     │         │              │                                 │
│   parser:                           │         │              ▼                                 │
│     kind: hdfs                      │ ──────▶ │   HDFSParser  (regex + block_id extraction)    │
│                                     │         │                                                │
│   validation:                       │         │              │                                 │
│     - kind: schema                  │ ──────▶ │              ▼                                 │
│       require_message: true         │         │   ValidationChain([SchemaValidator])           │
│                                     │         │                                                │
│   template_matcher:                 │         │              │                                 │
│     enabled: true                   │ ──────▶ │              ▼                                 │
│     templates_path: ...templates.csv│         │   TemplateMatcherStage (29 templates loaded)   │
│                                     │         │                                                │
│   sessionizer:                      │         │              │                                 │
│     enabled: true                   │ ──────▶ │              ▼                                 │
│     idle_window_s: 30.0             │         │   BlockSessionizerStage                        │
│     max_open_traces: 50000          │         │     (janitor every 5s, 30s idle window,        │
│     flush_interval_s: 5.0           │         │      LRU at 50K open traces)                   │
│                                     │         │                                                │
│   detectors: []     # none inline   │         │              │ records continue                │
│                                     │         │              ▼                                 │
│   sequence_detector:                │         │   InMemoryLogRepository (batch=100)            │
│     kind: sequence_kl               │         │                                                │
│     model_path: models/hdfs_kl.json │ ──────▶ │   EventBus  ──▶  KLDivergenceSequenceDetector  │
│     threshold: 0.3                  │         │                  (subscribes to                │
│     alpha: 0.5                      │         │                   BlockTraceClosed)            │
│     score_mode: max_contrib         │         │                                                │
│   repository: { kind: memory }      │ ──────▶ │                                                │
│                                     │         │                                                │
│   runtime:                          │         │   PipelineExecutor                             │
│     num_workers: 4                  │ ──────▶ │     workers=4, queue_size=1000,                │
│     queue_size: 1000                │         │     batch_size=100                             │
│     batch_size: 100                 │         │                                                │
└─────────────────────────────────────┘         └────────────────────────────────────────────────┘
```

---

### Side-by-side: `bgl.yaml` → BGL pipeline

```
┌─────────────────────────────────────┐         ┌────────────────────────────────────────────────┐
│   configs/bgl.yaml                  │         │   Built pipeline layout                        │
│                                     │         │                                                │
│   source:                           │         │                                                │
│     kind: file                      │ ──────▶ │   FileLogSource(path="data/BGL/BGL.log")       │
│     path: data/BGL/BGL.log          │         │                                                │
│                                     │         │              │                                 │
│   parser:                           │         │              ▼                                 │
│     kind: bgl                       │ ──────▶ │   BGLParser  (str.split, label = field[0])     │
│                                     │         │                                                │
│   validation:                       │         │              │                                 │
│     - kind: schema                  │ ──────▶ │              ▼                                 │
│       require_message: true         │         │   ValidationChain([SchemaValidator])           │
│                                     │         │                                                │
│   template_matcher:                 │         │                                                │
│     enabled: false                  │ ──────▶ │   (no template matcher — skipped)              │
│                                     │         │                                                │
│   sessionizer:                      │         │                                                │
│     enabled: false                  │ ──────▶ │   (no sessionizer — BGL is point anomalies)    │
│                                     │         │                                                │
│   detectors:                        │         │              │                                 │
│     # per-line                      │         │              ▼                                 │
│     - kind: label                   │ ──────▶ │   DetectorStage([                              │
│       label_key: bgl_label          │         │     LabelAnomalyDetector(                      │
│       normal_value: "-"             │         │       label_key="bgl_label",                   │
│                                     │         │       normal_value="-"),                       │
│     # per-window                    │         │                                                │
│     - kind: window_features         │ ──────▶ │     WindowFeatureDetector(                     │
│       window_seconds: 60            │         │       window_seconds=60,                       │
│       weight_alert: 0.5             │         │       weights={alert:.5, entropy:.15,          │
│       weight_entropy: 0.15          │         │                node:.15, novelty:.20},         │
│       weight_node: 0.15             │         │       score_threshold=0.5),                    │
│       weight_novelty: 0.20          │         │                                                │
│       score_threshold: 0.5          │         │                                                │
│     # rate-only baselines           │         │                                                │
│     - kind: z_score                 │ ──────▶ │     ZScoreDetector(threshold=3.0, win=200),    │
│       threshold: 3.0                │         │     IQRDetector(k=1.5, win=200),               │
│     - kind: iqr                     │ ──────▶ │     CUSUMDetector(threshold=5.0, slack=0.5)    │
│       k: 1.5                        │         │   ])                                           │
│     - kind: cusum                   │         │                                                │
│       threshold: 5.0                │         │              │                                 │
│       slack: 0.5                    │         │              ▼                                 │
│                                     │         │                                                │
│   repository: { kind: memory }      │ ──────▶ │   InMemoryLogRepository (batch=100)            │
│                                     │         │                                                │
│   runtime:                          │         │   PipelineExecutor                             │
│     num_workers: 4                  │ ──────▶ │     workers=4, queue_size=1000,                │
│     queue_size: 1000                │         │     batch_size=100                             │
│     batch_size: 100                 │         │                                                │
└─────────────────────────────────────┘         └────────────────────────────────────────────────┘
```

---

### The builder — one function, both pipelines

```python
# src/pipelinex/core/builder.py

def build_pipeline(config: PipelineConfig) -> BuiltPipeline:
    source     = make_source(config.source)               # YAML "kind" → ILogSource
    parser     = make_parser(config.parser.kind)          # YAML "kind" → IParser
    validators = [make_validator(v) for v in config.validation]
    stages     = [ParserStage(parser), ValidationChain(validators)]

    if config.template_matcher.enabled:
        stages.append(TemplateMatcherStage(config.template_matcher.templates_path))

    sessionizer = None
    if config.sessionizer.enabled:
        sessionizer = BlockSessionizerStage(**config.sessionizer.model_dump())
        stages.append(sessionizer)

    detectors = [make_detector(d) for d in config.detectors]
    if detectors:
        stages.append(DetectorStage(detectors))

    repository = make_repository(config.repository)
    bus        = EventBus()

    sequence_detector = None
    if config.sequence_detector:
        sequence_detector = make_sequence_detector(config.sequence_detector)
        bus.subscribe(BlockTraceClosed, sequence_detector)   # ◀── HDFS-only wiring

    executor = PipelineExecutor(
        source=source, stages=stages, repository=repository, bus=bus,
        workers=config.runtime.num_workers,
        queue_size=config.runtime.queue_size,
        batch_size=config.runtime.batch_size,
    )
    return BuiltPipeline(executor, repository, bus, sessionizer, sequence_detector)
```

> **Same builder. Same code path. Two completely different pipelines.**

**[SAY] (~3 minutes):**
> Before I trace one log line through the system, I want to show how the
> *system itself* gets assembled from configuration. This is what makes
> "extensible" a literal property rather than a slogan.
>
> On the left of each row is a real YAML file from the repo — `hdfs_v1.yaml`
> on top, `bgl.yaml` on the bottom. On the right is the pipeline layout the
> builder produces from each file. The arrows show which YAML block maps to
> which constructed component.
>
> Look at the differences. The HDFS pipeline turns on the template matcher,
> turns on the sessionizer with a 30-second idle window, and wires a
> sequence detector to the event bus — because HDFS anomalies are
> sequence-shaped. The BGL pipeline turns off the template matcher, turns
> off the sessionizer, and instantiates a stack of inline detectors:
> `LabelAnomalyDetector` honours the per-line ground-truth label that
> BGL emits in column 1; `WindowFeatureDetector` scores each one-minute
> window on four engineered features (alert density, severity entropy,
> node diversity, template novelty); and Z-Score, IQR, and CUSUM keep
> the original rate-only baselines for comparison.
>
> The builder code at the bottom is what makes this work. About fifty lines.
> It reads the validated Pydantic config, calls a factory for each component,
> conditionally adds stages based on the `enabled` flags, and wires the
> event bus subscription only when a sequence detector is configured. The
> *exact same function* produces both pipelines from the two different YAML
> files. No HDFS-specific code path, no BGL-specific code path — just
> configuration driving construction.

**[NOTES]**
- This slide proves the extensibility claim before you make it again on
  the patterns slide.
- If asked "what happens if I misspell `idle_window_s`?" — Pydantic's
  `extra="forbid"` rejects the config at load time with a clear error.
- If asked "how big is the builder?" — about 220 lines total including
  factory imports and the `BuiltPipeline` dataclass.

---

# Slide 7 — Example Trace: HDFS and BGL through the Pipeline

> *This is a two-part slide. Show HDFS first, then BGL. If your slide tool
> allows builds/animations, reveal each stage one at a time as you speak.*

---

### Part A — HDFS line (sequence anomaly path)

```
INPUT (raw string from FileLogSource):
┌──────────────────────────────────────────────────────────────────────────────┐
│ "081109 203615 148 INFO dfs.DataNode$PacketResponder:                        │
│  PacketResponder 1 for block blk_38865049064139660 terminating"              │
└──────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ STAGE 1 · PARSE (HDFSParser)                                                 │
│                                                                              │
│   Regex extracts: date, time, pid, level, component, content                 │
│   Severity map:   INFO → Severity.INFO                                       │
│   Block-id regex: re.search(r"blk_(-?\d+)", content)                         │
│                                                                              │
│   OUTPUT — LogRecord:                                                        │
│     id=UUID('a3f2…'),                                                        │
│     timestamp=datetime(2008-11-09 20:36:15),                                 │
│     severity=Severity.INFO,                                                  │
│     source="dfs.DataNode$PacketResponder",                                   │
│     message="PacketResponder 1 for block blk_38865… terminating",            │
│     enrichment={"block_id": "blk_38865049064139660"}                         │
└──────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ STAGE 2 · VALIDATE (ValidationChain: schema → size → rate-limit)             │
│                                                                              │
│   schema:     timestamp / severity / source / message all present  ✓         │
│   size:       message length 58 B  <  64 KB                          ✓       │
│   rate-limit: source under 5K rec/s budget                           ✓       │
│                                                                              │
│   OUTPUT — same LogRecord, passes through unchanged                          │
└──────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ STAGE 3 · ENRICH (TemplateMatcherStage)                                      │
│                                                                              │
│   Loaded 29 HDFS templates at startup, each pre-compiled with                │
│   wildcards `<*>` and `[*]` accepted.                                        │
│   Match "PacketResponder <*> for block blk_<*> terminating"  →  E11          │
│                                                                              │
│   OUTPUT — LogRecord with:                                                   │
│     enrichment={"block_id": "blk_38865…", "event_id": "E11"}                 │
└──────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ STAGE 4 · SESSIONIZE (BlockSessionizerStage)        [TEE — pass-through]     │
│                                                                              │
│   Internal state for block_id "blk_38865…":                                  │
│     open_traces["blk_38865…"].append(record_id=UUID('a3f2…'),                │
│                                       event_id="E11",                        │
│                                       timestamp=...)                         │
│                                                                              │
│   The record continues downstream UNCHANGED.                                 │
│   When 30 log-seconds idle (or terminal hint) → janitor closes the trace.   │
│                                                                              │
│   OUTPUT-A — original LogRecord (continues to repository)                    │
│   OUTPUT-B — BlockTraceClosed event on the EventBus when trace closes:       │
│     BlockTrace(                                                              │
│       block_id="blk_38865049064139660",                                      │
│       event_sequence=("E22","E5","E11","E9","E21"),  ← 5 events             │
│       record_ids=(UUID('a3f2…'), UUID('b1c8…'), …),  ← UUIDs, not records   │
│       record_count=5, closed_reason="terminal")                              │
└──────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ STAGE 5 · DETECT — KLDivergenceSequenceDetector (subscriber on EventBus)     │
│                                                                              │
│   Trace bigrams (Q):  (E22,E5)×1, (E5,E11)×1, (E11,E9)×1, (E9,E21)×1         │
│   Reference P:        trained on 558K Normal blocks, V=29, 137 distinct      │
│                       bigrams seen, ~10.3 M total occurrences                │
│   Score:              KL(Q‖P)=0.07 nats; max_contrib=0.04 nats               │
│                       both below threshold=0.3 → healthy                     │
│                                                                              │
│   OUTPUT — no AnomalyEvent. Block is healthy.                                │
└──────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
                            Repository.save_batch([...record...])
                            Repository.save_trace(BlockTrace(...))
```

---

### Part B — BGL line (point anomaly path)

```
INPUT (raw string from FileLogSource):
┌──────────────────────────────────────────────────────────────────────────────┐
│ "KERNDTLB 1117838611 2005.06.03 R23-M1-N6-I:J18-U01                          │
│  2005-06-03-15.43.31.041218 R23-M1-N6-I:J18-U01 RAS KERNEL FATAL             │
│  data TLB error interrupt"                                                   │
└──────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ STAGE 1 · PARSE (BGLParser)                                                  │
│                                                                              │
│   str.split(maxsplit=9) — NOT regex (tail is free-form)                      │
│   Fields:  [0]=Label    [1]=Timestamp     [4]=ISO date    [8]=Level          │
│   Label "KERNDTLB" ≠ "-" → enrichment["bgl_label"]="KERNDTLB"                │
│   "FATAL" → Severity.CRITICAL                                                │
│                                                                              │
│   OUTPUT — LogRecord:                                                        │
│     timestamp=datetime(2005-06-03 15:43:31),                                 │
│     severity=Severity.CRITICAL,                                              │
│     source="R23-M1-N6-I:J18-U01",                                            │
│     message="data TLB error interrupt",                                      │
│     enrichment={"bgl_label": "KERNDTLB", "node": "R23-M1-N6-I:J18-U01"}      │
└──────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ STAGE 2 · VALIDATE                                                           │
│                                                                              │
│   schema ✓   size ✓   rate-limit ✓   → passes through                        │
└──────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ STAGE 3 · ENRICH                                                             │
│                                                                              │
│   No template matcher, no sessionizer — BGL is per-line / per-window only.   │
│   Record passes through unchanged.                                           │
└──────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ STAGE 4 · DETECT — DetectorStage fans out the record to every detector       │
│                                                                              │
│   ┌─ LabelAnomalyDetector ──────────────────────────────────────────────┐    │
│   │   Reads enrichment["bgl_label"] = "KERNDTLB"  (≠ "-")               │    │
│   │   FIRES IMMEDIATELY — single-line ground-truth alert                │    │
│   │   AnomalyEvent(detector_name="label", severity=1.0,                 │    │
│   │                metadata={"bgl_label": "KERNDTLB"})                  │    │
│   └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│   ┌─ WindowFeatureDetector ─────────────────────────────────────────────┐    │
│   │   Buffers record into 60-s window "2005-06-03 15:43:00"             │    │
│   │   Window stays open until a record from the *next* bucket arrives;  │    │
│   │   on roll-over the closed window scores 4 features:                 │    │
│   │     alert_density · severity_entropy · node_diversity · novelty     │    │
│   │   Weighted sum > 0.5 → fires on the FIRST record of the next bucket │    │
│   └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│   ┌─ Z-Score / IQR / CUSUM  (rate-only baselines — kept for comparison)  ┐   │
│   │   Run on per-minute counts; their unchanged F1 ~0.20 is part of      │   │
│   │   the v1→v2 "wrong feature, not wrong detector" story (Slide 13).    │   │
│   └──────────────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
                            Repository.save_batch([...record...])
                            Repository.save_anomaly(AnomalyEvent(...))
                            EventBus.publish(AnomalyDetected(...))  →  alerters
```

---

> **Same pipeline code. Two completely different anomaly models.**
> *HDFS exercises sessionization + KL-divergence sequence detection.*
> *BGL exercises per-line label detection + per-window feature scoring.*

**[SAY] (~4 minutes):**
> Let me make all of that concrete by tracing one real line from each
> dataset through every single stage. This slide is the most important one
> for understanding what the pipeline actually does — I'll spend a bit of
> time on it.
>
> **Part A — the HDFS line.** At the top is a real line from the Hadoop log:
> a date, a time, a process ID, a severity, a Java component name, and a
> message about a block called `blk_38865049064139660`.
>
> **Stage one — parse.** The HDFS parser runs a single regex against the
> line, splitting it into named groups. The severity word `INFO` maps to
> `Severity.INFO`. A second regex pulls out the block ID. The output is a
> fully-structured `LogRecord` — notice that the block ID lives in the
> enrichment dictionary, because the block ID is the primary key for what
> comes next.
>
> **Stage two — validate.** Three validators run in a chain: schema makes
> sure required fields are present, size checks the message isn't bigger
> than 64 kilobytes, rate-limit checks this source hasn't blown its budget.
> All three pass, the record continues unchanged.
>
> **Stage three — enrich.** The template matcher has 29 pre-compiled
> templates loaded from disk. It matches our message against
> "PacketResponder *X* for block blk_*Y* terminating" — that's event
> template E11. So enrichment now has both `block_id` and `event_id`.
>
> **Stage four — sessionize.** This is where things get interesting. The
> sessionizer doesn't transform the record; it lets the record continue
> downstream unchanged. But it *also* updates its internal state — it
> appends this record's UUID and event ID to the open trace for
> `blk_38865`. Eventually — either when the trace is idle for 30 log-seconds
> or when a terminal event is seen — the janitor closes the trace and
> publishes a `BlockTraceClosed` event on the bus. The event contains the
> full event sequence — five events here — and the UUIDs of all the records
> in the trace.
>
> **Stage five — detect.** The sequence detector is subscribed to the event
> bus. When it sees the closed trace, it computes the 2-grams — four
> consecutive event pairs. It checks each one against the set of 137
> 2-grams seen during training on normal blocks. All four are in the set,
> so this block is healthy. No anomaly fired.
>
> **Part B — the BGL line.** Now look at the bottom of the slide. Same
> framework, completely different anomaly story.
>
> **Parse.** The BGL parser uses `str.split`, not a regex, because the tail
> of a BGL line is free-form. The first field is the per-line label —
> `KERNDTLB` here, meaning a kernel data TLB error. That single field tells
> us this is an anomalous line, and it's stored in the record's enrichment.
>
> **Validate.** Same chain, all pass.
>
> **Enrich.** Nothing happens here for BGL. No template matcher, no
> sessionizer — BGL anomalies are per-line and per-window, not
> sequence-shaped. The record passes through unchanged.
>
> **Detect.** This is where the v2 BGL ensemble fans out. The
> `DetectorStage` hands the record to every detector in parallel.
> `LabelAnomalyDetector` reads the `bgl_label` field, sees `KERNDTLB`
> instead of a dash, and **fires immediately** — that's the trust-the-source
> path, and it's why per-line BGL F1 is 1.0. At the same time,
> `WindowFeatureDetector` buffers this record into its 60-second bucket;
> when the next record arrives in a later bucket, the closed window gets
> scored on four engineered features — alert density, severity entropy,
> node diversity, template novelty — and fires if the weighted sum
> crosses threshold. The original z-score, IQR, and CUSUM detectors run
> alongside, scoring per-minute event counts; their unchanged ~0.20 F1
> is part of the story I'll show on the results slide.
>
> **The punch line is at the bottom of the slide.** Same pipeline code, same
> stage chain, same async machinery. Two completely different anomaly
> models — HDFS exercises sessionization plus KL-divergence sequence
> detection; BGL exercises per-line label honouring plus per-window
> feature scoring. That's the architectural claim, demonstrated on real
> lines of real data.

**[NOTES]**
- This is the longest slide of the talk. Don't rush. Let the audience read
  along.
- If your slide tool supports it, animate each stage in one at a time as you
  speak — it makes the data flow feel like a journey.
- If asked "where do per-line records persist on the HDFS path?" — Stage 4
  is a tee. Records continue to `repository.save_batch` like any other; the
  trace is the *additional* output, not a replacement.

---

# Slide 8 — Concurrency Engine

**Visual:**

```
                ┌──────────────────────┐
                │   Producer task      │
                │  reads source.stream │
                │  awaits queue.put()  │ ◀─ blocks if queue is full
                └──────────┬───────────┘
                           │
                           ▼
                ┌──────────────────────┐
                │   asyncio.Queue      │
                │     maxsize = N      │ ◀─ BOUNDED
                └──────────┬───────────┘
                           │
        ┌──────────────────┼──────────────────┐
        ▼                  ▼                  ▼
   ┌─────────┐        ┌─────────┐        ┌─────────┐
   │worker 1 │        │worker 2 │  …N    │worker N │
   │ stage   │        │ stage   │        │ stage   │
   │ chain   │        │ chain   │        │ chain   │
   └────┬────┘        └────┬────┘        └────┬────┘
        └────────┬─────────┴─────────┬────────┘
                 ▼                   ▼
          batches → repository       events → event bus
```

> **The single most important property:** when the consumer is slower than
> the producer, `queue.put()` blocks. The producer waits. Memory stays
> bounded. **No records are lost.**

**[SAY] (~3 minutes):**
> Here's how the pipeline actually runs.
>
> One producer task on the left, N consumer tasks on the right. Between them,
> an `asyncio.Queue` with a fixed maximum size — typically 1,000.
>
> The producer reads from the source and calls `await queue.put(line)`. That
> single word — **bounded** — is the most important architectural choice in
> the whole project. When the queue is full, `queue.put()` doesn't raise.
> It doesn't drop. It doesn't buffer to disk. It **blocks** — the producer
> awaits until a consumer takes something out.
>
> On the consumer side, N workers — typically 4 or 8 — each run an `async
> for` loop pulling from the queue. They execute the stage chain on each
> record and accumulate batches. When a batch fills, they flush it to the
> repository.
>
> The benefit of this design is called **backpressure**. When the database
> slows down, the repository flushes slow down. The workers stop pulling
> from the queue. The queue fills. The producer blocks. Suddenly the producer
> is running at exactly the consumer's pace — automatically, without any
> explicit rate limiting.
>
> Compare that to an unbounded queue. If the consumers are slower than the
> producer, the queue grows. Memory grows. Eventually the process runs out
> of memory and dies — and at that moment, you lose every record currently in
> the queue. The bounded queue trades a clear, recoverable failure mode —
> producer waits — for an opaque, destructive one — out of memory.
>
> I verify this is real, by the way, with a property-based test that varies
> worker count, queue size, and batch size across many random combinations
> and asserts that records-in equals records-persisted plus records-failed.
> I call that the **records-conservation invariant**, and it's the
> correctness property the whole system rests on.

**[NOTES]**
- This is the most likely deep-dive question. Be ready for:
  - "What if you can't block the producer (e.g., network input)?" → buffer to
    disk + circuit-break, documented as v2.
  - "Why N workers, not one coroutine per record?" → batching needs bounded
    concurrency; unbounded coroutines defeat the queue.

---

# Slide 9 — Error Classification & Graceful Shutdown

**Visual:**

> ### Two exception types, never strings
> ```python
> class FatalStageError(Exception):
>     """Config broken, schema invalid, unrecoverable.
>        → tear down the whole pipeline."""
>
> class TransientStageError(Exception):
>     """One record failed in isolation
>        (malformed line, validator rejection).
>        → log it, count it, continue."""
> ```
>
> ### Graceful shutdown — six steps
> 1. Producer stops reading
> 2. Producer puts SENTINEL on the queue
> 3. Workers drain remaining items
> 4. Workers see SENTINEL → flush batches → exit
> 5. Sessionizer flushes **all open traces** (`closed_reason="shutdown"`)
> 6. Repository commits final batch
>
> → Zero records lost on Ctrl-C, SIGTERM, or `shutdown()` call.

**[SAY] (~2 minutes):**
> Two related correctness ideas worth a slide of their own.
>
> First, **error classification**. Real systems fail in two fundamentally
> different ways. Either something catastrophic has happened — configuration
> is wrong, the schema is broken, the database has gone away — in which case
> the sane response is to tear down the pipeline. Or one record is bad in
> isolation — a malformed line, a record that fails validation — in which
> case the right response is to log it, increment a counter, and move on. I
> made these distinct exception types — `FatalStageError` and
> `TransientStageError` — and the executor catches them by *class*, not by
> string-matching the error message. That's a robustness choice: error
> messages change, class hierarchies don't.
>
> Second, **graceful shutdown**. When you press Ctrl-C or send SIGTERM,
> the system does six things, in order. The producer stops reading. It puts
> a sentinel value on the queue. The workers drain anything still in the
> queue, recognize the sentinel, flush their batches, and exit. The
> sessionizer flushes every open trace — even mid-block traces — with
> `closed_reason = "shutdown"`. The repository commits its final batch.
> Process exits. Zero records lost.
>
> This is verified by the same property test that verifies backpressure: it
> triggers shutdown at random points during random workloads, and the
> records-conservation invariant holds every time.

**[NOTES]**
- "By class, not string-matching" is a senior-engineering signal — say it
  clearly.

---

# Slide 10 — Sessionizer + KL-Divergence Sequence Detector — Deep Dive

> *This is the deepest technical slide of the talk. Plan for ~5 minutes.
> Two halves: how the sessionizer builds a trace, then how the v2 KL
> detector scores it.*

---

### Part A — `BlockSessionizerStage`

> ### The problem
> HDFS anomalies are *sequence-shaped*. But data arrives one line at a time.
> Records for different blocks are interleaved across the stream. The
> sessionizer's job is to group records by `block_id` and emit each group
> as a `BlockTrace` once the block is "done."
>
> ### The crucial design choice — `record_ids`, not records
> ```python
> @dataclass(frozen=True)
> class BlockTrace:
>     block_id: str
>     event_sequence: tuple[str, ...]
>     record_ids: tuple[UUID, ...]   ◀── UUIDs only, NOT LogRecords!
>     first_timestamp: datetime
>     last_timestamp: datetime
>     record_count: int
>     closed_reason: str             # "terminal" | "timeout" | "shutdown" | "evicted"
> ```
> 575K blocks × ~19 lines avg = 11M LogRecords. Pinning them all in open
> traces would consume gigabytes. **Storing only UUIDs keeps memory bounded.**
>
> ### Emission triggers (priority order)
> | # | Trigger | Why |
> |---|---|---|
> | 1 | **Timeout** *(primary)* | janitor scans every 5s; emit traces idle > 30s |
> | 2 | **Terminal-event hint** | E9 / E21 seen → fast-track to next janitor pass |
> | 3 | **Hard cap** | > 1000 events on one block → force-emit |
> | 4 | **Shutdown** | emit all open traces (`closed_reason="shutdown"`) |
> | 5 | **LRU eviction** | > 50K open traces → evict oldest |
>
> ### Why NOT terminal-only emission
> > **Anomalous traces by definition often *miss* the terminal event.**
> > Terminal-only emission would silently drop the most interesting cases.

---

### Part B — `KLDivergenceSequenceDetector` — what it does, in one slide

> ### The intuition in one sentence
> Build a **fingerprint** of how normal blocks usually flow. For each new
> block, build *its* fingerprint. Fire when the two fingerprints look
> meaningfully different.

**Picture it — normal reference (P) vs. an anomalous trace (Q):**

```
Reference P  (learned offline from 558K normal blocks)
   (E5,E11)   ████████████████████████  most common pair in normal traffic
   (E22,E5)   ████████████████
   (E11,E9)   ███████████
   (E9,E21)   █████████
   (E5,E5)    .                          essentially never seen normally
   ...

Trace Q  (one new block being scored)
   (E5,E11)   █████                      under-represented vs. normal
   (E22,E5)   █████
   (E5,E5)    ██████████                 over-represented — and rare in P!
                                         ↑ this is the surprise that fires the detector
```

**What fires, what stays silent:**

> | The trace's fingerprint looks like… | Detector | Why |
> |---|---|---|
> | Every bigram appears at roughly normal proportions | **silent** | the two fingerprints overlap |
> | Bigrams seen in training, but in unusual proportions | **fires softly** | distribution drift — caught by full KL |
> | At least one bigram that's essentially never seen normally | **fires hard** | one surprise dominates — caught by `max_contrib` |

**The math is one line — that's it:**

```
KL(Q || P)  =  Σ   Q(g) · log( Q(g) / P(g) )         higher = more different
              g∈Q
```

*(A small smoothing constant on P keeps `log` finite when the trace contains a bigram the reference has never seen.)*

**The scoring loop — five lines of real Python:**

```python
async def detect_trace(self, trace: BlockTrace) -> AnomalyEvent | None:
    for g, q_prob in trace_bigram_distribution(trace):
        p_prob = reference.smoothed_prob(g)
        score += q_prob * math.log(q_prob / p_prob)
    return AnomalyEvent(...) if score > threshold else None
```

> ### Two scoring modes (configurable from YAML)
> | mode | the question it asks |
> |---|---|
> | **`kl`** | "Is the **whole** distribution different from normal?" |
> | **`max_contrib`** *(default)* | "Is there **one** surprising bigram?" — more robust on short traces |
>

**[SAY] (~5 minutes):**
> This is the deepest slide of the talk. I want to spend five minutes on it
> because it's the part of the project that goes from "log pipeline" to
> "anomaly detector." Two halves — first the sessionizer that builds a
> trace, then the detector that scores it.
>
> **Part A — the sessionizer.** HDFS anomalies are about *sequences* of
> events on one block. A write that never completed. A sequence of events
> in the wrong order. Detecting that requires having the whole sequence in
> hand. But the data arrives one line at a time, and records for different
> blocks are interleaved. So the sessionizer's job is to group records by
> block ID and emit each group as a `BlockTrace` when the block is "done."
>
> **The most important design choice** in the entire project is in this
> dataclass. Look at the `record_ids` field — it's a tuple of UUIDs, *not*
> a tuple of `LogRecord` objects. Here's the math behind why. HDFS has
> roughly 575,000 distinct blocks, averaging 19 lines each — about 11
> million records. If I held the full records inside the open traces, I'd
> be pinning multiple gigabytes of memory while the pipeline is running.
> By storing only UUID references, an open trace stays around 50 bytes
> regardless of length. If the detector needs the full records later, it
> queries the repository. On the 2,000-line sample everything fits in RAM
> and this choice is invisible. On 11 million lines, it's the difference
> between a system that works and a system that runs out of memory.
>
> **The second big question is: when do you decide a block is "done"?**
> The naive answer is "emit when you see the terminal event — E9 or E21."
> **That's wrong**, and it's the most important sentence on the slide.
> Anomalous traces by definition often *miss* the terminal event. If I
> emitted on terminal events only, I'd silently drop the worst anomalies.
>
> So my primary trigger is **timeout**. A janitor coroutine runs every 5
> seconds and emits any trace whose last event is more than 30 log-seconds
> old. The terminal event is a *hint* — when I see it, I fast-track the
> trace to the next janitor pass. There's also a hard cap to prevent one
> runaway block from eating memory, and an LRU eviction at 50,000 open
> traces. Bounded memory is a contract, not a hope.
>
> When a trace closes, the sessionizer publishes a `BlockTraceClosed` event
> on the bus. Per-line records continue downstream unchanged — the trace
> is a *derived* artifact, not a transformation.
>
> **Part B — the v2 sequence detector.** The detector subscribes to those
> closed-trace events. Here's the whole idea in one breath: **build a
> fingerprint of how normal blocks usually flow, build the same kind of
> fingerprint for each new block, and fire when the two fingerprints
> look meaningfully different.**
>
> The picture at the top of the slide shows what that means. The top
> bar chart is the **reference fingerprint** — call it P. I built it
> offline by parsing the full 11-million-line HDFS log, sessionizing it,
> keeping only the *labeled-normal* blocks, and counting how often each
> pair of consecutive events shows up. Some pairs are extremely common,
> some are essentially never seen. That's the reference for "what
> normal looks like."
>
> The bottom bar chart is **the trace being scored** — call it Q. The
> detector builds the same kind of fingerprint from just this one
> block's events. Then it compares the two distributions. If they
> overlap closely, the block is healthy. If a pair shows up in the
> trace that *almost never* appears in normal traffic — like the
> highlighted bar — that's the surprise that fires the detector.
>
> The "what fires, what stays silent" table is the intuition you should
> take away. Trace looks like normal traffic → silent. Trace uses
> familiar pairs but in *odd proportions* → fires softly. Trace contains
> a pair that's basically never seen normally → fires hard.
>
> The math that formalises this is **KL divergence** — Kullback-Leibler.
> One line, at the bottom of the slide. The intuition is exactly what
> the picture shows: sum up, across every pair in the trace, how
> *surprising* it is relative to normal. Bigger sum, bigger surprise.
> If that sum crosses my threshold, I fire.
>
> The actual scoring loop is five lines of Python. I won't read them out
> — the point is that it's *small*. About two hundred lines total for
> the whole detector, zero ML dependencies, deterministic, runs in
> microseconds per trace.
>
> Why I chose this over an LSTM or PCA or Drain. Three reasons. It's
> **small** — easy to read, easy to maintain. It's **explainable** —
> when it fires, the event metadata lists *exactly which pairs* drove
> the score, so I can show a reviewer "this is why." Embedding-space
> methods can't do that. And it **catches the distributional anomalies
> that the v1 set-membership detector missed** — reordered events,
> missing terminals, frequency drift. The results slide will show that
> this single change lifted recall from 0.29 to 0.73.

**[NOTES]**
- This is the slide that deserves the most rehearsal. Three strong claims
  must land cleanly: (1) UUIDs not records, (2) timeout-not-terminal,
  (3) KL is "comparing fingerprints" — keep the metaphor; do not
  derive math on the projector.
- The phrase "bounded memory is a contract, not a hope" is yours — use it.
- **Numeric walk-through (Q&A backup only — do not put on the slide).**
  Trace `E22 → E5 → E5 → E5 → E11` → bigrams (E22,E5)×1, (E5,E5)×2,
  (E5,E11)×1, total 4. (E5,E5) is essentially unseen in normal training,
  so its smoothed P is ~5×10⁻⁸. Its contribution alone is
  `0.5 · log(0.5 / 5e⁻⁸) ≈ 8 nats`, blowing past the 0.3-nat threshold.
  Score the same trace in `kl` mode and you get ~10.7 nats — same
  conclusion.
- If asked "what does smoothing actually do?" — keeps `log(Q/P)` finite
  when the trace contains a bigram the reference never saw. Without it,
  P=0 and `log` blows up. Lidstone with α=0.5 is Jeffreys' prior; a
  sweep showed the detector was insensitive between 0.1 and 1.0.
- If asked "why not just learn an embedding?" — explainability. KL tells
  you *which bigram fired*. An LSTM gives you a vector.
- If asked "two scoring modes — when does each one matter?" — `kl`
  catches diffuse drift across many bigrams; `max_contrib` is more
  robust on short noisy traces. Both ship; default is `max_contrib`.

---

# Slide 11 — Design Patterns: Problem → Pattern

**Visual:**

> ### Each pattern solves a specific problem
>
> | Problem | Pattern | Where it lives |
> |---|---|---|
> | Multiple log formats, one pipeline | **Strategy** | `IParser` + `HDFSParser`, `BGLParser`, `JSONParser` |
> | Construct the right concrete class from a YAML string | **Factory** | `ParserFactory`, `DetectorFactory` |
> | Cross-cutting concerns (timing, retry, logging) without duplicating code | **Decorator** | `TimingDecorator`, `RetryDecorator`, `LoggingDecorator` |
> | One event, many independent reactions | **Observer** | `EventBus` → SequenceDetector / Console / Metrics |
> | Multiple validators, any one can reject | **Chain of Responsibility** | `ValidationChain` |
> | Hide the data store behind a uniform API | **Repository** | `ILogRepository` → in-memory, Postgres |
>
> **Rule I followed:** *every pattern must earn its place.* If removing
> it makes the code simpler with no real loss, it shouldn't be there.

**[SAY] (~3 minutes):**
> Now that you've seen the core flow — including the sessionizer-plus-detector
> deep dive — let me zoom out and show how the design patterns I picked
> map directly to specific problems I had to solve. Patterns are easy to
> over-apply, so my rule was: every pattern in this project has to *earn
> its place*. Let me walk down the list as **problem → solution**.
>
> **Problem 1.** I need to support multiple log formats — HDFS, BGL, JSON,
> and more later — but I want the rest of the pipeline to not care which
> format it's looking at. **Solution:** the Strategy pattern. Every parser
> implements the same `IParser` interface. The pipeline holds an `IParser`,
> never an `HDFSParser`.
>
> **Problem 2.** My YAML config says `parser: { kind: hdfs }`. I need to
> turn that string into an actual parser object. **Solution:** the Factory
> pattern. A tiny dictionary maps "hdfs" to `HDFSParser`. Without this, the
> config loader would have a giant if-else chain that grows every time I
> add a parser.
>
> **Problem 3.** I want to add timing, retry, and structured logging to
> any stage, but I don't want to copy that code into every stage's
> implementation. **Solution:** the Decorator pattern. Three decorators
> wrap any stage. Apply one with a single YAML line.
>
> **Problem 4.** When the sessionizer closes a block trace — which you
> just saw — multiple things want to react: the sequence detector, an
> alerter, a metrics exporter. The sessionizer shouldn't know about any
> of them. **Solution:** the Observer pattern, in the form of the
> EventBus. The sessionizer publishes; listeners subscribe.
>
> **Problem 5.** I have three validators — schema, size, rate-limit — and
> any one of them can reject a record. **Solution:** Chain of
> Responsibility. Each validator gets a chance; first rejection
> short-circuits.
>
> **Problem 6.** I want to develop and test against an in-memory store but
> run production against Postgres, with the same code. **Solution:** the
> Repository pattern. One interface, `ILogRepository`, two
> implementations.
>
> Six patterns, six concrete problems. No decoration just for decoration's
> sake.

**[NOTES]**
- If asked "which one was the closest call?" — Factory. It's a wrapper
  around a dict; the reason it's worth it is YAML-driven config.
- The "earn its place" framing protects you against any "isn't this
  over-engineered?" probe.

---

# Slide 12 — Results: HDFS Sequence Detection (v2 KL-divergence)

**Visual:**

> ## HDFS_v1 — KL-divergence sequence detection
>
> | metric | v1 (set membership) | **v2 (KL-divergence)** |
> |---|---|---|
> | Universe | 575,061 blocks | 575,061 blocks |
> | Ground-truth anomalies | 16,838 (~2.93%) | 16,838 (~2.93%) |
> | True positives | 4,798 | **12,211** |
> | False positives | 0 | 17,842 |
> | False negatives | 12,040 | **4,627** |
> | Precision | 1.0000 | 0.4063 |
> | Recall | 0.2850 | **0.7252** |
> | **F1** | 0.4435 | **0.5208** |
>
> ### How to read it
> - **v1** was P=1.0 by construction — fired only on bigrams that *never* appeared in any normal block.
> - **v2** scores per-trace KL divergence on bigram *frequency distributions*, so it catches anomalies that use only seen bigrams in odd proportions.
> - Threshold tuned via 80/20 sweep; `score_mode='max_contrib'` reports the single worst bigram, more robust than the full KL sum.

**[SAY] (~2 minutes):**
> Here are the actual numbers — v1 baseline and v2 KL-divergence side by side
> on the same labeled universe.
>
> v1, on the left, hit precision 1.0 because it fired only on 2-grams that
> *never* appeared in any normal training trace. That's structural — by
> construction every fire was correct. The price was recall of 0.285:
> roughly 71% of true anomalies used only "normal" 2-grams in different
> proportions, lengths, or positions. Set-membership simply can't see that.
>
> v2 replaces set-membership with **KL divergence on bigram frequency
> distributions**. For each closed block trace, I compute the bigram
> distribution Q, compare against the normal reference P using Lidstone
> smoothing for unseen pairs, and score the divergence. The threshold is
> tuned on a held-out split; the default reports the single worst bigram's
> contribution to KL, which is more robust than the full sum on short
> noisy traces.
>
> The result: **recall lifts from 0.285 to 0.725**, **F1 from 0.44 to 0.52**.
> v2 catches 12,211 true anomalies vs v1's 4,798 — two and a half times as
> many. The cost is precision: 0.41 vs v1's 1.0. That's the honest
> tradeoff — and the right one for an anomaly detector where missing real
> issues costs more than chasing a false alarm.
>
> Same `ISequenceAnomalyDetector` interface. The factory points to a
> different detector class; the bus wiring, sessionizer, builder are all
> unchanged. That's the architectural claim demonstrated, not asserted.

**[NOTES]**
- The most-probed question: "Why isn't precision 1.0 anymore?" Be direct:
  "v1's perfect precision was structural — by construction. The price was
  missing 70% of anomalies. v2 trades structural precision for a 2.5×
  improvement in recall and 17% higher F1. In a real anomaly system,
  missing real issues hurts more than a higher FP rate."
- If pushed on the FP count (17,842): "Those are blocks the detector
  flagged that the dataset labels as normal. Some are real subtle
  anomalies the loghub labeling missed; the rest are normal-but-rare
  patterns. Threshold tuning is the lever — push to 1.5 nats and FP drops
  to 918 with precision back to 0.80, but recall drops to 0.22."

---

# Slide 13 — Results: BGL Detection + Throughput

**Visual:**

> ## BGL — two detection paths, two findings (4,713,493 lines)
>
> ### Per-line: `LabelAnomalyDetector` honors the upstream classification
>
> | metric | value |
> |---|---|
> | Lines evaluated | 4,713,493 |
> | Ground-truth alerts | 348,460 (~7.4%) |
> | TP / FP / FN | 348,460 / 0 / 0 |
> | **Precision / Recall / F1** | **1.0000 / 1.0000 / 1.0000** |
>
> ### Per-window ensemble (60 s windows)
>
> | detector | P | R | F1 |
> |---|---|---|---|
> | **window_features** (4-feature engineered) | **0.39** | **0.98** | **0.56** |
> | z_score (rate only) | 0.16 | 0.09 | 0.11 |
> | IQR (rate only) | 0.16 | 0.28 | 0.20 |
> | CUSUM (rate only) | 0.03 | 0.02 | 0.03 |
>
> *Rate-only baselines stay reported on purpose — they are the "wrong-feature failure mode."*
>
> ## Throughput, latency, backpressure
>
> | metric | result |
> |---|---|
> | Throughput | **~46,000 records/sec** at 8 workers (target was 20K) |
> | Latency p99 | **8 microseconds** (ingest → pre-batch) |
> | Backpressure stress | queue saturated 100/100, **zero records lost** |

**[SAY] (~2 minutes):**
> BGL has two complementary detection paths in v2.
>
> First, **per-line**. BGL's first column is the supercomputer's own
> classification of every log line — `KERNDTLB`, `APPREAD`, that family —
> with `-` for normal. v1 collapsed this into per-minute rates and got
> F1 0.20. v2 added a `LabelAnomalyDetector` that honors the upstream
> classification directly: 4.7 million lines, 348,000 alerts, **zero
> false positives, zero false negatives, F1 of one point zero**. This is
> the "trust-the-source" detector — and in production it's how
> PipelineX would honor any tool's own diagnostic codes, like the RAS
> codes an EBeam tool might emit.
>
> Second, **per-window**. The label is only one signal. The same dataset
> also has window-level anomaly shape — bursts, severity-mix shifts,
> novel component types appearing. So I added a `WindowFeatureDetector`
> that scores four engineered features per minute: alert density,
> severity entropy, node diversity, and template novelty. **F1 0.56** —
> a 2.8× lift over the best rate-only baseline. Crucially, I kept the
> rate-only detectors in the ensemble. Their unchanged 0.20 F1 is part
> of the story: it shows the architecture cleanly separates "wrong
> feature, right detector" from "right feature, right detector."
>
> Finally, **throughput, latency, and backpressure.** 46,000 records per
> second sustained at 8 workers — above my 20K target by more than 2x.
> Sub-10-microsecond p99 latency from ingest to pre-batch. And the
> backpressure stress test — queue size 100, slow repository simulating
> a 50ms-per-batch database — saturates at exactly 100 out of 100, with
> zero records lost.

**[NOTES]**
- The label detector is the most challenged claim. Pre-empt it:
  "It's honoring an upstream classifier — same as you'd do with a tool's
  own diagnostic codes. The window detector is the evidence the
  architecture isn't *only* trusting labels."
- If asked "why doesn't throughput scale with worker count?" — at this
  per-record work, the asyncio event loop is the bottleneck. Moving the
  parser to a `ProcessPoolExecutor` would push past it. Documented as v3.

---

# Slide 14 — Summary

**Visual:**

> ## PipelineX — closing the loop on the problem
>
> Three problems on Slide 3. One answer on this slide.
>
> | The problem | PipelineX's answer |
> |---|---|
> | **Volume** — a billion lines a day, grep can't keep up | An **async pipeline** that ingests, parses, and scores in one streaming pass |
> | **Structure** — real anomalies are patterns, not keywords | Detectors that work on the **right shape** of each anomaly — per-line *and* sequence |
> | **Heterogeneity** — every system emits a different log format | One **config-driven** architecture that handles new formats by adding configuration, not rewriting code |
>
>
> ### The one line to remember
> > **PipelineX turns log analytics from "tail and grep" into a structured, measurable, extensible system — and proves it on real data.**

**[SAY] (~1 minute):**
> To close.
>
> I opened on Slide 3 with three problems: volume, structure, and
> heterogeneity. Everything between then and now was an answer to one of
> those three.
>
> **Volume** — a billion lines a day. PipelineX answers that with a
> streaming async pipeline that ingests, parses, and scores in a single
> pass, and never silently loses a record under load.
>
> **Structure** — real anomalies are *patterns*, not keywords. PipelineX
> answers that with detectors that match the actual shape of the
> anomaly — per-line for point anomalies, sequence-aware for pattern
> anomalies — proven on two datasets whose anomalies are *opposite* in
> shape.
>
> **Heterogeneity** — every system emits a different format. PipelineX
> answers that by making the architecture **configuration-driven**:
> adding a new log format is a YAML change and a parser class, not a
> rewrite of the pipeline.
>
> If you remember one thing from this talk, it's this: **PipelineX takes
> log analytics from "tail and grep" to a structured, measurable,
> extensible system — and proves it on real data.** Every claim I made
> tonight is backed by a reproducible number in the repository.
>
> Thank you. I'd love to take your questions.

**[NOTES]**
- This is the slide they'll remember. The "tail and grep → structured,
  measurable, extensible system" line is the one to land cleanly. Pause
  before the final sentence.
- Do not introduce new material here. Every claim on this slide must
  have been said earlier in the talk.
- Eye contact for the last sentence. Then transition straight to Q&A.

---

# Slide 15 — Thank You

**Visual:**

> # Thank you.

**[SAY] (~30 seconds):**
> Thank you for your time. I'd be happy to go deeper on anything I covered,
> or anywhere I didn't. The full source, the architecture document, and the
> benchmark numbers are all in the repository.

---

# Appendix — Q&A preparation

The presentation is linear; the Q&A is where they probe your understanding.
Below are the most likely questions, organized by what each is actually
testing.

## A1. Concurrency depth

**Q:** *"Walk me through second-by-second what happens if Postgres suddenly takes 2 seconds per insert."*

**A:** At t=0 the producer reads at ~46K rec/s and the queue stays roughly
empty because consumers drain it quickly. The first slow flush starts; the
worker doing it stops dequeuing for 2 seconds. Other workers continue but
will hit slow flushes in turn. Within a second or two, all workers are
blocked on flushes. The queue fills to its `maxsize` of 1,000. At that
point the producer's next `queue.put()` blocks. The producer is now running
at the consumer's pace — automatically. Memory stays bounded, no records are
lost, and when Postgres recovers, everything drains in queue order.

**Q:** *"Why N workers? Why not one coroutine per record?"*

**A:** Batching requires bounded concurrency — with unbounded coroutines, no
natural batch boundary. Also, the queue is the single point of ordering and
backpressure; per-record coroutines erase both.

**Q:** *"How would you scale to multi-node?"*

**A:** Replace the in-process queue with Kafka or Redis Streams. Producer
publishes to the topic; multiple consumer-process instances subscribe via a
consumer group. Each instance keeps its own internal bounded queue. The hard
part is sessionization — block traces have to be partitioned by `block_id`
so all events for one block land on the same consumer, or merged post-hoc.

**Q:** *"What if the producer can't block — e.g., a network source?"*

**A:** Buffer to durable storage at the edge with a circuit breaker, then
shed load explicitly when the buffer fills. Documented as v2 work.

## A2. Detection depth

**Q:** *"Your HDFS recall is 0.28. Why didn't 3-grams fix it?"*

**A:** Three reasons. First, n=3 doesn't solve the actual problem — most
missed anomalies use only normal 2-grams, and only normal 3-grams as well,
in different *proportions*. The fix is divergence-based, not n-bigger.
Second, vocabulary explodes from 29² to 29³, most 3-grams have zero
training examples, and precision collapses. Third, the n-gram baseline was
the literal v1 scope I set.

**Q:** *"How would the KL-divergence detector work?"*

**A:** Aggregate normalized 2-gram frequencies across labeled-Normal traces
into a reference distribution P. For a new trace, compute its 2-gram
distribution Q. Fire if `KL(Q || P) > threshold`. Smooth P with a small
epsilon for unseen bigrams. Tune threshold on a held-out validation split.

**Q:** *"Precision = 1.0 sounds suspicious."*

**A:** Not luck, structural. The detector fires only on 2-grams that
literally never appeared in any normal trace, so by construction every fire
is correct *given the training set*. The real risk is training-set
contamination: a mislabeled-anomaly block in "normal" training would
whitelist its 2-grams forever. I rely on the loghub labels being clean.

## A3. Architecture

**Q:** *"Why an event bus when you have ~one subscriber?"*

**A:** It's there for the moment I add the second subscriber — a Slack
alerter, a metrics exporter, an audit log. Without it, the sessionizer
would need to know about every consumer of `BlockTraceClosed`. The value of
Observer is in the *absence of coupling* that would otherwise grow. Marginal
cost of the bus is ~80 lines.

**Q:** *"Pydantic v2 vs. dataclasses for config?"*

**A:** Pydantic gives runtime validation with `extra="forbid"`. Typos in
YAML fail at load time instead of producing a silent default-fallback bug.
Dataclasses don't validate. For user-edited YAML, runtime validation is
worth the dependency.

**Q:** *"What's the weakest part of the current architecture?"*

**A:** Decorator-stack ordering is implicit. `timing-then-retry` versus
`retry-then-timing` behave differently and nothing in the YAML enforces
sensible order. v2 fix: explicit ordering in the schema, or a
`DecoratorChain` builder that documents intent.

## A4. Engineering process

**Q:** *"How did you test the bounded-queue behavior?"*

**A:** Three layers. Unit tests with mocks asserting the queue blocks when
full. A Hypothesis property test that varies workers, queue size, and batch
size across thousands of random combinations and asserts
records-conservation. A dedicated benchmark with a `SlowRepository` that
samples queue depth in a sidecar coroutine and asserts saturation at
maxsize with zero records lost.

**Q:** *"What did Hypothesis catch that unit tests didn't?"*

**A:** A real bug. The BGL parser crashed on whitespace-only input because
`parts[0]` was indexed without checking that `parts` was non-empty.
`test_can_parse_never_raises` fed it `"\r"`, Hypothesis shrank the failing
case to that single character, and I added an empty-list check.

## A5. KLA-domain bridging

**Q:** *"How does this apply to EBeam tool logs?"*

**A:** I'd expect EBeam logs to be sequence-shaped — many asynchronous
events from many subsystems, where individual lines are normal but
specific patterns indicate problems. So sessionization by
tool-component-or-wafer-id, plus a sequence detector, would map directly.
The event vocabulary and the right session boundary need domain input, but
the architecture is already shaped for that workload.

**Q:** *"What changes for real-time streams instead of historical?"*

**A:** Less than you'd think. The source becomes a `KafkaLogSource` or
`SocketLogSource` — same `ILogSource` interface. Sessionizer timeouts move
from log-time-based to wall-time-based. Detection becomes online — updating
distributions as events arrive, alerting on threshold crossings, instead of
scoring closed traces. Throughput target gets harder; architectural shape
doesn't.

## A6. Honest self-critique

**Q:** *"If you had another week, what's the first thing you'd fix?"*

**A:** The KL-divergence sequence detector. F1=0.44 is the most prominent
result; the v2 lift to ~0.85 is one module of math, not new architecture.

**Q:** *"What did you not get to?"*

**A:** Three things. A web UI — low risk, doesn't demonstrate anything new.
A distributed executor — a 2-week project on its own. And the
feature-engineered BGL detector that would move BGL F1 above 0.5. The third
is the most painful omission because it's tractable.

**Q:** *"What surprised you?"*

**A:** The wildcard syntax bug. Loghub publishes HDFS templates in two
files — the sample uses `<*>`, the full corpus uses `[*]`. My first
full-corpus training run produced zero template matches. One-line regex
fix. Generalizable lesson: sanity-check match counts before trusting any
downstream metric.

---

# Delivery tips

## Pace and rhythm
- **Don't fill silence.** Pauses are confidence. After "the bounded queue is
  what makes all of this work," let it sit for two seconds.
- **One key sentence per slide.** Hit the one highlighted line; the rest is
  context.
- **Speak slower than feels natural.** Adrenaline will speed you up; aim for
  what feels like 80% pace.

## Body language
- Stand if you can. It helps your breathing.
- Gesture along your architecture diagram physically.
- Eye contact rotates across the panel — don't lock onto one person.

## Handling tough questions
- Skip "that's a good question" — it's a stalling phrase the panel has heard
  a thousand times.
- "I don't know, but here's how I'd find out" beats fabricating.
- If you don't understand a question, ask for clarification: "Could you say
  more about what you mean by..."
- If a question hits something you hadn't considered, say so: "I hadn't
  considered that — my instinct would be X, but I'd want to verify Y."

## Things not to say
- "Just" ("I just used Pydantic for...") — downplays your work.
- "Simple" ("It's a simple bounded queue...") — it's not simple, it's
  carefully chosen.
- "Hopefully" — replace with "I've verified that..."

## Final 10-minute checklist
- [ ] Slide deck open
- [ ] Code repo open in a second window
- [ ] Numbers memorized: 46K rec/s, 8 µs p99, F1=0.4435, P=1.0, R=0.285
- [ ] One-sentence answer to "biggest decision?" — *bounded queue → backpressure → records-conservation*
- [ ] Water within arm's reach
- [ ] Deep breath. You built this. You know it better than anyone in the room.
