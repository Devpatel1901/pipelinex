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
| 5 | **Datasets — HDFS & BGL schemas + anomaly shapes** | 3 |
| 6 | **Config → Pipeline** (YAML to built pipeline, two examples) | 3 |
| 7 | Example trace — HDFS & BGL through every stage | 4 |
| 8 | Concurrency engine | 3 |
| 9 | Error classification & graceful shutdown | 2 |
| 10 | **Sessionizer & sequence detector — deep dive** | 5 |
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
> 3. **Datasets** — HDFS & BGL schemas and the shape of their anomalies
> 4. **Config → Pipeline** — how YAML becomes a running pipeline
> 5. **Example trace** — one HDFS line and one BGL line through every stage
> 6. **Concurrency engine** — how the async pipeline actually runs
> 7. **Error classification & graceful shutdown**
> 8. **Sessionizer & sequence detector** — the deep dive
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

# Slide 5 — Datasets: HDFS & BGL — Schemas and Anomaly Shapes

> *Before going into config and code, the panel needs to know what the data
> actually looks like and what an "anomaly" actually means in each dataset.
> This slide is the foundation that every later slide builds on.*

---

### Why these two datasets

> | | **HDFS_v1** | **BGL** |
> |---|---|---|
> | **System** | Hadoop Distributed File System | IBM Blue Gene/L supercomputer |
> | **Source** | Private cloud, MapReduce benchmark workload | Lawrence Livermore National Lab — 131,072 CPUs, 32,768 GB RAM |
> | **Released by** | Loghub (Zhu et al., ISSRE 2023) — academic standard for log-analytics benchmarks | Same — original paper Oliner & Stearley, DSN 2007 |
> | **Size in repo** | 11.17 M lines, ~1.5 GB | 4.75 M lines, ~700 MB |
> | **Labeling granularity** | Per **block** — one label per `block_id` | Per **line** — first column of each line |
> | **Anomaly shape** | **Sequence** — the *pattern* of events on a block is wrong | **Point** — individual line is itself anomalous |
> | **What it stresses in PipelineX** | Sessionizer + sequence detector | Per-line parsing + point detectors |

> **The whole point of using both:** they're *orthogonal* anomaly models.
> One pipeline framework, two different mathematics. If the same architecture
> handles both, the extensibility claim is real.

---

### HDFS — schema and anomaly shape

**Raw line format:**
```
081109 203518 143 INFO dfs.DataNode$DataXceiver: Receiving block blk_-1608999687919862906 src: /10.250.19.102:54106 dest: /10.250.19.102:50010
└─┬──┘ └─┬──┘ └┬┘ └─┬┘ └─────────┬─────────┘  └──────────────────────────┬──────────────────────────────────────────┘
 date   time  pid  level     component                                content (free-form, contains block_id)
```

**The key field is `block_id`** (`blk_-1608999687919862906`). HDFS doesn't
label individual *lines* — it labels entire *blocks*. Each block is a
multi-line lifecycle: allocate → receive on replica 1 → receive on
replica 2 → receive on replica 3 → confirm → serve → delete. Loghub
provides:

> | Preprocessed artifact | What it contains |
> |---|---|
> | `HDFS.log_templates.csv` | **29 event templates** (E1–E29). Each raw line matches exactly one. E.g. `E5 = "Receiving block [*] src: [*] dest: [*]"`, `E11 = "PacketResponder [*] for block [*] terminating"` |
> | `anomaly_label.csv` | One row per block: `(block_id, Normal | Anomaly)` |
> | `Event_traces.csv` | The pre-computed event sequence per block, used as a sanity reference for my sessionizer |

**The numbers — labeled ground truth:**

> | | count | % |
> |---|---:|---:|
> | Total blocks | **575,061** | 100.0% |
> | Normal | 558,223 | 97.07% |
> | **Anomaly** | **16,838** | **2.93%** |

**What an HDFS anomaly looks like — three real examples from the corpus:**

> | Anomaly pattern | What goes wrong | Why a regex can't catch it |
> |---|---|---|
> | **Write never completed** | `allocateBlock` fires, replicas start receiving, then no `Received block ... of size` confirmation | Every individual line is *normal*. Anomaly is the *absent* terminal event. |
> | **Replication failed mid-flight** | `Receiving block` on 3 nodes, exception on one, the surviving replicas re-replicate → unusual ordering | Every line matches a known template. Anomaly is the unusual *sequence* of templates. |
> | **Block deleted that wasn't fully stored** | `Deleting block` before `addStoredBlock` for that replica | Each line in isolation is benign; only the relative *order* is wrong. |

> **The single takeaway:** no per-line anomaly label exists in HDFS *because
> no single line is anomalous*. The anomaly is a property of the **sequence
> of events on one block**. That's why HDFS forces sessionization, and why
> the sequence detector exists.

---

### BGL — schema and anomaly shape

**Raw line format (whitespace-separated, free-form tail):**
```
KERNDTLB 1117838611 2005.06.03 R23-M1-N6-I:J18-U01 2005-06-03-15.43.31.041218 R23-M1-N6-I:J18-U01 RAS KERNEL FATAL data TLB error interrupt
└──┬───┘ └──┬─────┘ └───┬────┘ └───────┬──────────┘ └──────────┬───────────┘ └─────┬──────────┘ └┬┘ └──┬─┘ └─┬──┘ └──────────┬──────────┘
 label   epoch ts    date       node loc (rack/midplane/    full timestamp    node (repeat)   type  comp  level    content (free-form)
                                  node/card/chip/CPU)
```

**The key field is the *first column*** — the **per-line ground-truth
label**. `-` means a normal line. Anything else is an alert category code
identifying *what kind* of anomaly that single line is.

**The numbers — labeled ground truth (this codebase, this file):**

> | Label | count | meaning |
> |---|---:|---|
> | `-` | 4,399,503 | normal |
> | `KERNDTLB` | 152,734 | kernel data TLB (translation lookaside buffer) error |
> | `KERNSTOR` | 63,491 | kernel storage error |
> | `APPSEV` | 49,651 | application severe failure |
> | `KERNMNTF` | 31,531 | kernel monitor / fatal |
> | `KERNTERM` | 23,338 | kernel termination |
> | `KERNREC` | 6,145 | kernel recovery |
> | `APPREAD` | 5,983 | application read failure (`ciod: failed to read message prefix on control stream`) |
> | … 30+ more codes … | | (link, microcode, power, etc.) |
> | **Total anomalous** | **~348,000** | **~7.3% of lines** |

**What a BGL anomaly looks like — one real example:**

```
APPREAD 1117869872 2005.06.04 R23-M1-N8-I:J18-U11 2005-06-04-00.24.32.398284
        R23-M1-N8-I:J18-U11 RAS APP FATAL
        ciod: failed to read message prefix on control stream
        (CioStream socket to 172.16.96.116:33399
```

That single line, in isolation, **is the anomaly.** No surrounding
context is needed to classify it — the `APPREAD` label in column 1 is
ground truth that this one line indicates an application I/O failure.

> **The single takeaway:** every anomaly in BGL is locatable to one line.
> No sequence, no block lifecycle, no time-correlation required for the
> ground-truth labeling. That's why BGL is the natural evaluation dataset
> for **point detectors** — and why my BGL pipeline skips the sessionizer
> and wires Z-Score / IQR / CUSUM instead.

---

### Why this matters for PipelineX

> | Dataset says… | PipelineX answers with… |
> |---|---|
> | "Anomalies are sequence-shaped" (HDFS) | `BlockSessionizerStage` + `SequenceAnomalyDetector` on the event bus |
> | "Anomalies are point-shaped numeric outliers" (BGL) | Inline detectors in the stage chain: `ZScoreDetector`, `IQRDetector`, `CUSUMDetector` |
> | "Schemas are completely different" (regex vs. split, block_id vs. node_id) | Strategy pattern — `HDFSParser` and `BGLParser` behind the same `IParser` interface |
> | "Anomaly labels live in different files in different formats" | Each dataset's evaluation script joins on its own key (block_id for HDFS, per-line label column for BGL) |

> **The bridge to KLA:** EBeam tool logs almost certainly contain *both*
> shapes simultaneously — single-line hardware faults (point-shaped) *and*
> multi-step process sequences that deviate (sequence-shaped). The reason
> I chose these two datasets specifically is that they let me prove the
> architecture handles both, on real published labeled data, before applying
> the same shape to a proprietary log.

**[SAY] (~3 minutes):**
> Before I show you the implementation, I want to spend three minutes on the
> two real datasets I evaluated against. Because the *shape* of the
> anomalies in each one is what drives every architectural choice in the
> rest of the talk. If I skip this, the next several slides will feel like
> abstract code rather than answers to concrete problems.
>
> Both datasets come from **Loghub**, which is the academic standard for
> log-analytics benchmarks — it's the canonical corpus that papers in this
> area evaluate against. I picked these two specifically because they're
> *orthogonal*. They represent two fundamentally different kinds of anomaly,
> and if a single architecture handles both, the extensibility claim
> isn't a slogan — it's demonstrated.
>
> **HDFS first.** This is logs from a 200-node Hadoop cluster running a
> MapReduce benchmark — 11 million lines, about a gigabyte and a half. Look
> at the raw line at the top. There's a date, a time, a process ID, a
> level, a Java component name, and a free-form content tail. The crucial
> field is buried in the content: the **block ID**. HDFS is a distributed
> file system, and every file is split into blocks. Each block has a
> *lifecycle* — allocate, receive on three replicas, confirm, serve,
> eventually delete. That lifecycle spans many log lines, scattered across
> the cluster and interleaved with every other block's lines.
>
> **Crucially, the dataset's ground-truth labels are per-block, not
> per-line.** Loghub gives me a CSV — 575,061 blocks total, of which
> 16,838 are labeled anomalous. About three percent. Look at the right
> column of that anomaly table — every example of what "anomalous" means
> here is a *pattern*: a write that never completed, replication that
> failed mid-flight, a delete that happened before the store was
> confirmed. **In every case, the individual log lines are
> indistinguishable from normal lines.** The anomaly is in the *sequence*.
> A regex on a single line cannot find these. That is why the HDFS
> pipeline has a sessionizer — it has to *reconstruct the sequence* before
> any detector can score it.
>
> Loghub also ships 29 pre-extracted **event templates** — patterns like
> "Receiving block [*]" or "PacketResponder [*] terminating". Every line in
> the corpus matches exactly one of those 29 templates. That collapses the
> 11 million unique strings into sequences of 29-symbol alphabets — which
> is exactly the input the n-gram detector wants.
>
> **BGL is the opposite case.** Logs from Blue Gene/L — an IBM
> supercomputer at Lawrence Livermore National Lab, 131,000 CPUs and
> 32,000 gigabytes of RAM. 4.75 million lines, about 700 megabytes. Look at
> the schema — the first column of every single line is the
> **ground-truth label itself**. A dash means normal. Anything else is an
> alert code that tells you what kind of fault this one line represents.
> `KERNDTLB` is a kernel data-TLB error. `APPREAD` is the application
> control stream failing. There are about 30 distinct alert codes covering
> roughly 348,000 lines — about seven percent of the corpus.
>
> Look at the example anomaly at the bottom of the BGL section. One line.
> Self-contained. The label `APPREAD` in column 1 is the ground truth that
> this line — by itself, in isolation — represents an application failure.
> No surrounding context required. That's the textbook **point anomaly**
> model — and that's why the BGL pipeline skips the sessionizer entirely
> and instead wires three numeric detectors against per-line or per-window
> features.
>
> **The bottom of the slide is the bridge.** Two datasets, two opposite
> anomaly shapes, one architecture. HDFS forces sessionization plus
> sequence detection. BGL forces per-line parsing plus point detection.
> The Strategy pattern handles the wildly different schemas. The factories
> and the YAML configs decide which path runs. Every architectural choice
> you're about to see is a *response* to something on this slide.
>
> And the reason I think this matters for KLA specifically: EBeam tool
> logs almost certainly contain *both* of these shapes simultaneously —
> single-line hardware faults that are point-shaped, and multi-step
> process recipes that deviate, which are sequence-shaped. The reason I
> picked these two datasets is precisely to prove the architecture handles
> both before ever applying the same approach to a proprietary log.

**[NOTES]**
- This slide is *context*, not deep technical content. Don't rush, but
  don't linger past three minutes — every subsequent slide will be richer
  for the audience knowing this.
- The orthogonality framing (point vs. sequence) is the most important
  takeaway. You will refer back to it on Slides 6, 7, 10, 12, and 13.
- If asked "why not just use one dataset?" — single-dataset projects
  prove the *detector*. Two-orthogonal-dataset projects prove the
  *architecture*. The architecture is the thing being interviewed for.
- If asked "is BGL realistic for modern systems?" — yes. Same architectural
  shape appears in nginx access logs, syslog, and any tool log with
  per-line severity codes. BGL is the labeled stand-in.
- If asked about Loghub or the citations — Loghub is Zhu et al. ISSRE
  2023. BGL's original paper is Oliner & Stearley DSN 2007. HDFS labels
  trace back to Xu et al. SOSP 2009. All three are in the data READMEs.
- If asked "what about HDFS_v2 or HDFS_v3?" — same schema family, larger
  corpus, no per-block labels published. v1 is the only one with
  reproducible ground truth, which is why every published baseline uses it.

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
│     kind: sequence_ngram            │         │                                                │
│     model_path: models/...json      │ ──────▶ │   EventBus  ──▶  SequenceAnomalyDetector       │
│     threshold: 0.0                  │         │                  (subscribes to                │
│                                     │         │                   BlockTraceClosed)            │
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
│     - kind: z_score                 │         │              ▼                                 │
│       threshold: 3.0                │ ──────▶ │   DetectorStage([                              │
│       window_size: 200              │         │     ZScoreDetector(threshold=3.0, win=200),    │
│     - kind: iqr                     │ ──────▶ │     IQRDetector(k=1.5, win=200),               │
│       k: 1.5                        │         │     CUSUMDetector(threshold=5.0, slack=0.5)    │
│     - kind: cusum                   │ ──────▶ │   ])                                           │
│       threshold: 5.0                │         │                                                │
│       slack: 0.5                    │         │              │                                 │
│                                     │         │              ▼                                 │
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
> off the sessionizer, and instead instantiates three inline point detectors
> — Z-Score, IQR, and CUSUM — because BGL anomalies are per-line and numeric.
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
│ STAGE 5 · DETECT — SequenceAnomalyDetector (subscriber on EventBus)          │
│                                                                              │
│   Trace 2-grams:   (E22,E5), (E5,E11), (E11,E9), (E9,E21)                    │
│   Normal set (trained on 558K Normal blocks): 137 distinct 2-grams           │
│   Lookup result:   all 4 of these 2-grams ∈ normal set                       │
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
│ STAGE 3 · ENRICH (RateAggregator — 1-minute windows)                         │
│                                                                              │
│   Bin by minute:   record falls into window "2005-06-03 15:43"               │
│   Increment counter for that window: count = 184                             │
│   (Note: no sessionizer for BGL — point anomalies, no block_id)              │
│                                                                              │
│   OUTPUT — same LogRecord; window-count side-channel for detector            │
└──────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ STAGE 4 · DETECT — IQRDetector                                               │
│                                                                              │
│   Running window stats (warmup over prior 100 minutes):                      │
│     Q1 = 5    median = 12    Q3 = 28    IQR = 23                             │
│     Upper Tukey fence = Q3 + 1.5·IQR = 28 + 34.5 = 62.5                      │
│   Current window count = 184                                                 │
│   184 > 62.5  →  outlier                                                     │
│                                                                              │
│   OUTPUT — AnomalyEvent:                                                     │
│     detector_name="iqr",                                                     │
│     severity_score=(184-62.5)/23 = 5.3,                                      │
│     log_record_id=UUID('c4d1…'),                                             │
│     metadata={"window_start": "2005-06-03 15:43", "count": 184,              │
│               "fence_upper": 62.5}                                           │
└──────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
                            Repository.save_batch([...record...])
                            Repository.save_anomaly(AnomalyEvent(...))
                            EventBus.publish(AnomalyDetected(...))  →  alerters
```

---

> **Same pipeline code. Two completely different anomaly models.**
> *HDFS exercises sessionization + sequence detection.*
> *BGL exercises rate aggregation + point detection.*

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
> us this is an anomalous line.
>
> **Validate.** Same chain, all pass.
>
> **Enrich.** Instead of template matching, we have a rate aggregator —
> it bins records by one-minute window. This record falls into the
> 15:43–15:44 window, whose count is now 184.
>
> **Detect.** The IQR detector — which I'll explain on a later slide — has
> been tracking the rolling distribution of per-minute counts. The
> interquartile range is from 5 to 28, so the upper Tukey fence sits at
> 62.5. The current window has 184 events, which is well above the fence.
> The detector fires an anomaly event with a severity score reflecting how
> far past the fence we are.
>
> **The punch line is at the bottom of the slide.** Same pipeline code, same
> stage chain, same async machinery. Two completely different anomaly
> models — one operates on sequences of events, the other on numeric
> windows. That's the architectural claim, demonstrated on real lines of
> real data.

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

# Slide 10 — Sessionizer & Sequence Detector — Deep Dive

> *This is the deepest technical slide of the talk. Plan for ~5 minutes.
> Two halves: how the sessionizer builds a trace, then how the sequence
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

### Part B — `SequenceAnomalyDetector` (n-gram, n=2)

```
Closed trace's event sequence:           E22 → E5 → E26 → E26 → E11 → E9 → E21
                                          │     │     │     │     │     │
Sliding 2-grams (pairs):                  ▼     ▼     ▼     ▼     ▼     ▼
                                  (E22,E5), (E5,E26), (E26,E26), (E26,E11),
                                  (E11,E9), (E9,E21)
                                          │
Lookup each 2-gram against the normal     ▼
set (137 distinct 2-grams from training):
                                  in set?  in set?  in set?  in set?  in set?  in set?
                                    ✓        ✓        ✓        ✓        ✓        ✓
                                          │
                                          ▼
                                    No novel 2-gram → trace is healthy
```

> ### Training (offline) — `scripts/train_sequence_model.py`
> 1. Parse the full 11.17M-line HDFS log
> 2. Sessionize → emit closed `BlockTrace`s
> 3. Join against `anomaly_label.csv` on `block_id`
> 4. Keep only **labeled-Normal** traces (558,223 of them)
> 5. Collect every 2-gram that appears in any of them → **137 unique 2-grams**
> 6. Save: `{"vocab": [E1..E29], "normal_2grams": [...], "n": 2}`  → `models/hdfs_ngram_v1.json`
>
> ### Scoring (online)
> ```python
> async def detect_trace(self, trace: BlockTrace) -> AnomalyEvent | None:
>     seq = trace.event_sequence
>     bigrams = list(zip(seq, seq[1:]))
>     novel = [g for g in bigrams if g not in self._normal_set]
>     if not novel:
>         return None                      # healthy
>     return AnomalyEvent(
>         detector_name="sequence_ngram",
>         severity_score=len(novel) / len(bigrams),
>         log_record_id=trace.record_ids[0],
>         metadata={"block_id": trace.block_id, "novel_2grams": novel}
>     )
> ```
>
> ### Why n-gram and not PCA / LSTM / Drain?
> - 150 lines of code, **zero ML dependencies**, deterministic
> - Published HDFS baselines achieve F1 ~0.95 with n-gram methods
> - Fully explainable to a panel: "this 2-gram never appeared in normal training data"
> - v2 path documented: KL-divergence over 2-gram **frequencies** (recall lift)

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
> **Part B — the sequence detector.** The detector subscribes to those
> closed-trace events. Its job is to look at the event sequence inside the
> trace and decide: is this block normal, or is it anomalous?
>
> The math is intentionally simple. Look at the diagram. I take the event
> sequence — say, E22 through E21, seven events — and slide a window of
> size 2 across it. That gives me six 2-grams: consecutive pairs of events.
> I check each pair against a precomputed set of "normal" 2-grams. If every
> single pair has been seen in a normal training trace, the block is
> healthy. If even one pair is *novel* — never seen in normal training —
> I fire an anomaly.
>
> **The training step is offline.** I parse the full 11-million-line HDFS
> log, sessionize it, join against the published anomaly labels, keep only
> the labeled-Normal traces, and collect every 2-gram. There are 29
> distinct event types in HDFS, which means 841 possible 2-grams in
> theory — but only 137 ever appear in a normal trace. Most pairs never
> co-occur. I save those 137 as a JSON model, takes about 3 minutes to
> train end-to-end.
>
> I deliberately chose n-gram over PCA or an LSTM or a sophisticated
> template miner like Drain. Three reasons. It's about 150 lines of code
> with zero ML dependencies. Published HDFS baselines hit F1 around 0.95
> with this kind of n-gram method. And — most importantly — it's fully
> explainable. If a panel asks "why did this fire," I can point at the
> exact 2-gram and say "this pair never appeared in normal training data."
> The v2 path is documented: replace exact-set membership with KL
> divergence over 2-gram *frequencies*, which is what would lift recall.
> I'll cover that math on the results slide.

**[NOTES]**
- This is the slide that deserves the most rehearsal. Two strong claims
  must land cleanly: (1) UUIDs not records, (2) timeout-not-terminal.
- The phrase "bounded memory is a contract, not a hope" is yours — use it.
- If asked "what happens to the 12,040 false negatives?" — they use only
  normal 2-grams, just in different proportions. That's the segue to the
  results slide and the v2 KL-divergence pitch.

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

# Slide 12 — Results: HDFS Sequence Detection

**Visual:**

> ## HDFS_v1 — sequence anomaly detection (n-gram)
>
> | metric | value |
> |---|---|
> | Universe | 575,061 labeled blocks |
> | Ground-truth anomalies | 16,838 (~2.93%) |
> | True positives | 4,798 |
> | False positives | **0** |
> | False negatives | 12,040 |
> | **Precision** | **1.0000** |
> | **Recall** | **0.2850** |
> | **F1** | **0.4435** |
>
> ### How to read it
> - **Precision = 1.0** — by construction. We fire only on 2-grams that *never* appear in any normal block.
> - **Recall = 0.28** — ~71% of true anomalies use only "normal" 2-grams, just in different counts or orderings.
> - **v2 path:** KL-divergence on 2-gram frequencies, or 3-grams, or a Bayesian sequence model.

**[SAY] (~2 minutes):**
> Here are the actual numbers on the actual data.
>
> 575,000 labeled blocks, almost 17,000 of them ground-truth anomalies. My
> detector flagged 4,798 of them as anomalies. **Zero false positives.** Out
> of all 575,000 blocks, every single one I flagged was actually anomalous.
> That gives me a precision of 1.0 and a recall of 0.28, for an F1 of 0.44.
>
> Let me be direct about this number, because it's probably the most-probed
> result in the presentation. Precision being 1.0 is not luck — it's
> *structural*. My detector fires only when it sees a 2-gram that has never
> appeared in any normal trace. By construction, every firing is correct.
>
> Recall being 0.28 has an equally specific cause. About 71% of true
> anomalies use only 2-grams that *also* appear in normal traces — just in
> different proportions, lengths, or positions. A truncated trace, for
> example, might consist entirely of "normal" 2-grams; the anomaly is the
> *absence* of the closing bigram, not the presence of an unusual one.
>
> To recover those, v2 would replace exact-set membership with a divergence
> measure — KL divergence between the trace's 2-gram distribution and the
> normal distribution. That's a documented next step. I deliberately stopped
> at the baseline because I wanted a result I could explain end-to-end and
> spend my remaining time on architecture rather than chasing F1.

**[NOTES]**
- This is the slide that demonstrates intellectual honesty. Don't dress it up.
- If a panelist pushes "0.44 isn't very high," you've already conceded that
  and explained *why* and *what comes next* — that defuses the critique.

---

# Slide 13 — Results: BGL Point Detection + Throughput

**Visual:**

> ## BGL — point detection on 27,666 1-minute windows
>
> | detector | P | R | F1 |
> |---|---|---|---|
> | Z-Score | 0.16 | 0.09 | 0.11 |
> | IQR (Tukey) | 0.16 | 0.28 | **0.20** |
> | CUSUM | 0.03 | 0.02 | 0.03 |
>
> ↑ low F1 is the *correct* finding: BGL alerts don't correlate with rate spikes
>
> ## Throughput, latency, backpressure
>
> | metric | result |
> |---|---|
> | Throughput | **~46,000 records/sec** at 8 workers (target was 20K) |
> | Latency p99 | **8 microseconds** (ingest → pre-batch) |
> | Backpressure stress | queue saturated 100/100, **zero records lost** |

**[SAY] (~2 minutes):**
> Two more sets of numbers.
>
> First, **BGL point detection.** I binned the 4.75 million BGL lines into
> one-minute windows, labeled each window anomalous if it contained at least
> one alert line, and fed the per-window event rate to each of three
> detectors. The F1 numbers are low — IQR was the best at 0.20. And I want
> to tell you why, because the *reason* the numbers are low is more
> interesting than the numbers themselves.
>
> **BGL alerts don't correlate with rate spikes.** They're individual
> anomalous lines mixed into otherwise-normal traffic. Counting events per
> minute is just not the right signal for that anomaly model. A v2 detector
> would feature-engineer each window — alert density, message-length
> variance, novelty of message templates — and score *those* features.
>
> The reason I'm telling you these low numbers, instead of hiding them, is
> that **this is exactly the finding that justifies my whole architecture**.
> HDFS needs sequence detection. BGL needs point detection — and even that
> needs richer features. The whole point of having pluggable detectors is
> that different anomaly models need different mathematics. *The result is
> the justification.*
>
> Second, **throughput, latency, and backpressure.** 46,000 records per
> second sustained at 8 workers — above my 20K target by more than 2x.
> Sub-10-microsecond p99 latency from ingest to pre-batch. And the
> backpressure stress test — queue size 100, slow repository simulating a
> 50ms-per-batch database — saturates at exactly 100 out of 100, with zero
> records lost.

**[NOTES]**
- "The result is the justification" — this is the key reframe. Practice it.
- If asked "why doesn't throughput scale with worker count?" — at this
  per-record work, the asyncio event loop is the bottleneck. Moving the
  parser to a `ProcessPoolExecutor` would push past it. Documented as v2.

---

# Slide 14 — Summary

**Visual:**

> ## PipelineX in one slide
>
> - **Async log analytics engine** with bounded-queue backpressure as its central correctness property
> - **Six design patterns**, each solving a specific problem
> - **Four detectors** across two fundamentally different anomaly models (point and sequence)
> - **Quantitative F1** on *real* labeled data — HDFS_v1, BGL — reproducible from a fresh checkout
> - **~46K rec/s, p99 8 µs, zero record loss** under backpressure
> - **~200 tests**, mypy strict, ~82% coverage
>
> ### The single biggest decision
> > A **bounded queue** gives backpressure → backpressure gives the records-conservation invariant → that invariant is what makes the whole system trustworthy.

**[SAY] (~1 minute):**
> To summarize.
>
> PipelineX is an asynchronous log-analytics engine. It rests on one
> central correctness property — backpressure from a bounded queue, which
> guarantees no records are silently lost. Six design patterns, each with a
> specific problem to solve. Four detectors covering two fundamentally
> different anomaly models. Real F1 numbers on real published labeled
> datasets — not made up, fully reproducible. About 46,000 records per
> second, sub-10-microsecond latency, zero data loss verified.
>
> If you remember one thing from this talk: the bounded queue is what makes
> everything else possible. It's a small architectural choice with very
> large downstream consequences. Every other decision in the project ladders
> up to that one.

**[NOTES]**
- This is the slide they'll remember. The last sentence is the one to land
  cleanly.

---

# Slide 15 — Thank You & Q&A

**Visual:**

> # Thank you.
>
> ### I'd love your questions.

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
