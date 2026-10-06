# Live Ops Dashboard Buffer

An in-memory backend that takes in a high-volume stream of order status events, keeps **real-time per-store counts** (total, open, packed, out for delivery, delivered), and **every second** publishes:

1. a **dashboard snapshot** of every store's metrics, and
2. an **AI-ready anomaly summary** covering unusual backlogs, delivery delays and status imbalances.

On top of the fixed rules, an **LLM agent (Google Gemini)** reviews the snapshot every 10 seconds. It returns structured anomalies with reasons and suggested actions.

Everything runs in one Python process with no database, broker or other infrastructure.

---

## Contents
- [Quick start](#quick-start)
- [Architecture & data flow](#architecture--data-flow)
- [Components](#components)
- [Event model & processing rules](#event-model--processing-rules)
- [Anomaly detection (rules)](#anomaly-detection-rules)
- [The LLM agent: what it does and doesn't do](#the-llm-agent-what-it-does-and-doesnt-do)
- [API reference](#api-reference)
- [Try it with curl](#try-it-with-curl)
- [Configuration](#configuration)
- [Project layout](#project-layout)
- [Design decisions & limitations](#design-decisions--limitations)

---

## Quick start

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

echo "GOOGLE_API_KEY=your-key" > .env    # only needed for /ai-anomalies

uvicorn app:app --host 0.0.0.0 --port 8000
```

- Open **http://localhost:8000/** for the live dashboard.
- The built-in simulator starts on its own and sends about 300 new orders per second. Stores `s3`, `s5` and `s7` are deliberately slow so anomalies appear within seconds.

---

## Architecture & data flow

```
┌───────────────────┐   ┌───────────────────┐
│  Simulator        │   │  External clients │
│  (~300 orders/s,  │   │  POST /events     │
│  s3/s5/s7 slow)   │   │  202 or 429       │
└─────────┬─────────┘   └─────────┬─────────┘
          │  order status events  │
          └───────────┬───────────┘
                      ▼
          ┌───────────────────────┐
          │  asyncio.Queue        │  holds up to 10,000 events
          │                       │  when full: event dropped, 429
          └───────────┬───────────┘
                      ▼
          ┌───────────────────────┐
          │  Consumer             │  takes up to 500 events at a time
          │  apply(event)         │  skips duplicate, out-of-order
          └───────────┬───────────┘  and backward events
                      ▼
┌─────────────────────────────────────────────────────┐
│  In-memory state (all in Python memory)             │
│   orders    : order_id → (store, status, ts)        │
│   stores    : store → total/open/packed/ofd/deliv   │
│   ofd_since : store → {order_id: out-for-delivery   │
│                        start time}                  │
└───────────┬─────────────────────────────────────────┘
            ▼  every 1s
┌───────────────────────┐      ┌──────────────────────────┐
│  Snapshot loop        │────▶ │  Rule-based anomalies    │
│  copy counts + delay  │      │  backlog > 2× average    │
│  stats + queue stats  │ ◀────│  out-for-delivery > 60s  │
└───────────┬───────────┘      │  open > 60% of active,   │
            │                  │  packed > 3× out-for-del │
            ▼                  └──────────────────────────┘
┌─────────────────────────────────────────────────┐
│  latest  (cached results, read by the API)      │
│   dashboard │ anomalies │ ai_anomalies          │
└──────┬──────────────────────────────▲───────────┘
       │                              │ writes structured JSON
       │ every 10s: snapshot +        │
       │ rule anomalies               │
       ▼                              │
┌─────────────────────────────────────┴───────────┐
│  LLM agent (agent.py) → Gemini API              │
│  system prompt: "ops analyst..."                │
│  response_schema = AIReport (Pydantic)          │
│  on error: status=error, retries next interval  │
└─────────────────────────────────────────────────┘
       │
       ▼  API (reads only from the cached results)
┌───────────────────────────────────────────────────────────┐
│ GET /dashboard   GET /anomalies   GET /ai-anomalies       │
│ GET /stream (pushes every 1s) ──▶ GET / (browser page)    │
│ GET /health                                               │
└───────────────────────────────────────────────────────────┘
```

### Three loops, each at its own speed

| Loop | Runs | Work per run | Output |
|---|---|---|---|
| **Consumer** | On every event | Moves one order from its old status count to its new one | Live state in `stores` |
| **Snapshot loop** | Every **1s** | Copies the counts, works out delay stats, runs the rules | `latest.dashboard`, `latest.anomalies` |
| **LLM agent** | Every **10s** | Sends the snapshot to Gemini and gets JSON back | `latest.ai_anomalies` |

Each loop only reads what the loop before it produced, so a slow or failed Gemini call never holds up the counting. The API always serves the cached `latest` results, so reads are fast and never wait on event processing.

---

## Components

| Component | File | What it does |
|---|---|---|
| **Queue** | `app.py` | An `asyncio.Queue` with a size limit. It separates producers from the consumer. When it is full, `POST /events` returns `429` and the event is counted in `dropped`. |
| **Consumer** | `app.py` → `consumer()` | The only code that changes state, so no locks are needed. It takes up to 500 events at a time and then yields to the event loop. |
| **State** | `app.py` | `orders` (the current status of each order), `stores` (per-store counts) and `ofd_since` (when each out-for-delivery order left, per store, used for delay detection). |
| **Snapshot loop** | `app.py` → `snapshot_loop()` | Runs every second. It copies the counts, adds `oldest_ofd_age_s` and `ofd_over_delay`, works out totals and queue stats, then calls `detect()`. |
| **Rule engine** | `app.py` → `detect()` | Fixed-threshold anomaly checks, described below. |
| **LLM agent** | `agent.py` | Calls Gemini every 10 seconds with structured output. Explained in its own section below. |
| **Simulator** | `simulator.py` | Built-in event generator. It creates orders and moves them through the statuses. Some stores are slow at one stage on purpose. |
| **Live UI** | `dashboard.html` | A single page that reads `/stream` and shows store, rule and AI tables. |

---

## Event model & processing rules

```json
{ "order_id": "o123", "store_id": "s1", "status": "PACKED", "ts": 1759740000.5 }
```

Statuses move forward only:

```
OPEN → PACKED → OUT_FOR_DELIVERY → DELIVERED
```

How each event is applied (`apply()`):

| Case | Action |
|---|---|
| Order seen for the first time | `total += 1` and `count[status] += 1` |
| Status moves forward | `count[old] -= 1` and `count[new] += 1`. Entering `OUT_FOR_DELIVERY` starts the delay timer; leaving it stops the timer. |
| Event older than the last one for this order | **Ignored** (out of order), counted in `ignored` |
| Same status or a backward move (e.g. `DELIVERED → OPEN`) | **Ignored**, counted in `ignored` |

An order can skip stages (e.g. `OPEN → OUT_FOR_DELIVERY`). Each event costs **O(1)**.

---

## Anomaly detection (rules)

These run every second on the snapshot. They are fast, predictable and free, and they are the **reliable source** for anomalies.

| Type | Fires when (per store) | Default |
|---|---|---|
| `UNUSUAL_BACKLOG` | `open + packed` is greater than `BACKLOG_FACTOR` × the average across all stores, **and** at least `BACKLOG_MIN` | 2× average, at least 20 |
| `DELIVERY_DELAY` | At least `DELAY_MIN_ORDERS` orders have been `OUT_FOR_DELIVERY` longer than `DELAY_SEC` | 3 orders over 60s |
| `STATUS_IMBALANCE` (`open_share`) | `open / (open + packed + out_for_delivery)` is greater than `OPEN_SHARE_MAX`, which means packing is behind | over 60% |
| `STATUS_IMBALANCE` (`packed_to_ofd_ratio`) | `packed / out_for_delivery` is greater than `PACKED_TO_OFD_MAX`, which means dispatch is behind | over 3× |

The imbalance checks only run when a store has at least 20 unfinished orders, so tiny stores don't trigger them.

**Severity:** `high` if the value is at least 2× the threshold, otherwise `medium`.

The output is "AI-ready": every anomaly has a machine-readable `type`, `metric`, `value` and `threshold`, a plain-English `message`, and the whole result comes with a one-line `summary`. It can go straight into an LLM, an alerting tool or a chat bot.

---

## The LLM agent: what it does and doesn't do

### Its job
Gemini has one job: it **reviews the snapshot and explains it**. It is **not** in the live event path.

| Stage | Done by | How often |
|---|---|---|
| Taking in and queuing events | Plain Python | Every event (~900/s) |
| Updating per-store counts | Plain Python | Every event |
| Building the snapshot | Plain Python | Every 1s |
| Rule-based anomaly checks | Plain Python | Every 1s |
| **Reviewing the snapshot and explaining it** | **Gemini** | **Every 10s** |

### What it receives
- The latest snapshot: counts for every store, `oldest_ofd_age_s`, `ofd_over_delay`, totals and queue stats.
- The anomalies the rules already flagged.

It **never sees individual events**, only numbers that are already counted, so each request is small (a few KB).

### What it returns
Structured JSON, enforced with `response_schema` (Pydantic `AIReport`):

```json
{
  "status": "ok",
  "model": "gemini-flash-latest",
  "summary": "Three stores have stage bottlenecks: s7 at picking, s5 at dispatch, s3 at delivery.",
  "anomalies": [
    {
      "store_id": "s5",
      "type": "STATUS_IMBALANCE",
      "severity": "high",
      "reason": "557 packed vs only 35 out for delivery (15.9x).",
      "suggested_action": "Increase rider allocation and expedite dispatch at s5."
    }
  ]
}
```

### What it adds on top of the rules
1. **It compares stores against each other instead of checking fixed thresholds.** For example, it flagged `s3`'s delivery delay when the oldest delivery was 36s old, before the 60s rule fired.
2. **Reasons that quote the actual numbers.**
3. **Suggested actions:** what an ops person should do next.
4. **A summary written for people,** ready for a chat message or an on-call note.

### Why it isn't in the main event path
- **Speed:** a call takes seconds, while counting has to keep up with hundreds of events per second.
- **Cost:** a call every second would be expensive; every 10 seconds costs much less.
- **Reliability:** if Gemini fails (e.g. `503 high demand`), only `/ai-anomalies` shows `status: "error"`. Counts and rule anomalies keep updating, and the agent tries again on the next interval.
- **Accuracy:** an LLM can be wrong, so the rule-based anomalies stay the reliable source and the AI output is advice on top.

### `/ai-anomalies` status values
| `status` | Meaning |
|---|---|
| `pending` | No report yet (the first one arrives after `AI_INTERVAL` seconds) |
| `ok` | A fresh report is available |
| `error` | The last call failed. `error` holds the message; fields from the previous good report are kept. |
| `disabled` | No API key, or the client couldn't be created |

---

## API reference

| Method & path | Description | Updated |
|---|---|---|
| `POST /events` | Accepts **one event or a list**. Returns `202 {"accepted","dropped"}`, or `429` if the queue was full for any event. | n/a |
| `GET /dashboard` | Per-store counts plus `oldest_ofd_age_s` and `ofd_over_delay`, totals, and `pipeline` stats | every 1s |
| `GET /anomalies` | Rule-based anomaly summary | every 1s |
| `GET /ai-anomalies` | Gemini's structured anomaly report | every `AI_INTERVAL`s |
| `GET /stream` | Server-Sent Events: `{dashboard, anomalies, ai_anomalies}` pushed every second | every 1s |
| `GET /` | Live HTML dashboard (uses `/stream`) | live |
| `GET /health` | `{"status":"ok","queue_depth":N}` | live |

**`pipeline` fields:**
- `queue_depth`: events waiting in the queue. If this keeps growing, the consumer is falling behind.
- `events_per_sec`: events processed in the last second.
- `processed`, `ignored`, `dropped`: running totals since startup.

Example `/dashboard` (shortened):
```json
{
  "generated_at": 1791284865.33,
  "stores": {
    "s3": { "total": 3638, "open": 62, "packed": 52, "out_for_delivery": 2072,
            "delivered": 1452, "oldest_ofd_age_s": 95.2, "ofd_over_delay": 909 }
  },
  "totals": { "total": 36750, "open": 1260, "packed": 1270, "out_for_delivery": 2652, "delivered": 31568 },
  "pipeline": { "queue_depth": 0, "events_per_sec": 927, "processed": 138028, "ignored": 0, "dropped": 0 }
}
```

Interactive API docs (FastAPI): **http://localhost:8000/docs**

---

## Try it with curl

```bash
# Snapshot of all stores
curl -s localhost:8000/dashboard | python3 -m json.tool

# Rule-based anomalies
curl -s localhost:8000/anomalies | python3 -m json.tool

# Gemini anomaly report
curl -s localhost:8000/ai-anomalies | python3 -m json.tool

# Live stream (Ctrl+C to stop)
curl -N localhost:8000/stream

# Send your own events: one order goes OPEN, then PACKED
curl -s -X POST localhost:8000/events -H 'Content-Type: application/json' \
  -d '[{"order_id":"t1","store_id":"s99","status":"OPEN","ts":'$(date +%s)'},
       {"order_id":"t1","store_id":"s99","status":"PACKED","ts":'$(date +%s.1)'}]'
curl -s localhost:8000/dashboard | python3 -c "import sys,json; print(json.load(sys.stdin)['stores']['s99'])"

# Health
curl -s localhost:8000/health
```

---

## Configuration

Set these as environment variables before starting, e.g. `AI_INTERVAL=5 DELAY_SEC=30 uvicorn app:app --port 8000`.

| Variable | Default | Meaning |
|---|---|---|
| `GOOGLE_API_KEY` | (none) | Gemini API key, read from `.env` |
| `AI_MODEL` | `gemini-flash-latest` | Gemini model, e.g. `gemini-3.5-flash` |
| `AI_INTERVAL` | `10` | Seconds between LLM calls |
| `LLM_AGENT` | `1` | `0` turns the LLM agent off |
| `SIMULATE` | `1` | `0` turns the built-in simulator off (then use `POST /events` only) |
| `DELAY_SEC` | `60` | Seconds out for delivery after which an order counts as delayed |
| `QUEUE_MAX` | `10000` | Queue size; beyond it events are dropped and `429` is returned |

Rule thresholds (`BACKLOG_FACTOR`, `BACKLOG_MIN`, `DELAY_MIN_ORDERS`, `OPEN_SHARE_MAX`, `PACKED_TO_OFD_MAX`, `IMBALANCE_MIN_ACTIVE`) are constants at the top of `app.py`.

> The switch is named `LLM_AGENT`, not `AI_AGENT`, because some tools (e.g. Claude Code) already set `AI_AGENT` in the shell.

---

## Project layout

```
store/
├── app.py            # API, queue, consumer, state, snapshot loop, rule engine
├── agent.py          # LLM agent (Gemini, structured output)
├── simulator.py      # built-in event generator (slow stores s3, s5, s7)
├── dashboard.html    # live browser dashboard (SSE)
├── requirements.txt  # fastapi, uvicorn, google-genai, python-dotenv
└── .env              # GOOGLE_API_KEY (don't commit)
```

---

## Design decisions & limitations

**Decisions**
- **Everything in memory, single process.** This keeps setup minimal. A queue, the async event loop and a single writer give consistent counts without locks.
- **A size-limited queue with 429s.** When producers send faster than the consumer can keep up, they get an explicit error instead of the server running out of memory.
- **The snapshot is computed once a second, not on each request.** Reads cost nothing, and every client sees the same picture for that second.
- **Rules plus an LLM.** The rules are fast and predictable, and they are the source the system relies on. The LLM adds reasoning across stores, explanations and suggested actions, outside the main path.

**Limitations (by design, to keep it minimal)**
- No persistence: a restart clears all state.
- Single process: running several uvicorn workers would split the state between them.
- The `orders` map keeps delivered orders forever, so memory grows over time. Production would evict them or use a time-to-live.
- Delay timing uses the event's `ts` against the server clock, so the producers' clocks need to be roughly in sync.
- No authentication on the API.
- LLM output is advice only, can be wrong, and depends on the Gemini API being available.
