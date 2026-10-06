"""Live ops dashboard buffer: queue -> consumer -> in-memory store metrics -> 1s snapshot + anomalies."""
import asyncio, json, os, time
from collections import defaultdict
from contextlib import asynccontextmanager
from typing import Literal

from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel

import agent, simulator

STATUSES = ["OPEN", "PACKED", "OUT_FOR_DELIVERY", "DELIVERED"]
RANK = {s: i for i, s in enumerate(STATUSES)}

# Config (env overridable)
QUEUE_MAX = int(os.getenv("QUEUE_MAX", 10_000))
BATCH = 500
DELAY_SEC = float(os.getenv("DELAY_SEC", 60))      # out-for-delivery longer than this = delayed
DELAY_MIN_ORDERS = 3                               # delayed orders needed to flag a store
BACKLOG_FACTOR = 2.0                               # backlog > factor * fleet avg
BACKLOG_MIN = 20
IMBALANCE_MIN_ACTIVE = 20
OPEN_SHARE_MAX = 0.6
PACKED_TO_OFD_MAX = 3.0


class Event(BaseModel):
    order_id: str
    store_id: str
    status: Literal["OPEN", "PACKED", "OUT_FOR_DELIVERY", "DELIVERED"]
    ts: float


# ---- In-memory state (mutated only by the consumer task) ----
queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAX)
orders: dict[str, tuple[str, str, float]] = {}                       # order_id -> (store, status, ts)
stores = defaultdict(lambda: {"total": 0, "open": 0, "packed": 0, "out_for_delivery": 0, "delivered": 0})
ofd_since: dict[str, dict[str, float]] = defaultdict(dict)           # store -> {order_id: entered OFD ts}
stats = {"processed": 0, "ignored": 0, "dropped": 0}
latest = {"dashboard": {}, "anomalies": {"anomalies": []}, "ai_anomalies": {"status": "pending"}}


def apply(ev: Event) -> None:
    prev = orders.get(ev.order_id)
    if prev:
        store, status, ts = prev
        # ignore out-of-order, duplicate or backward transitions
        if ev.ts < ts or RANK[ev.status] <= RANK[status]:
            stats["ignored"] += 1
            return
        stores[store][status.lower()] -= 1
        if status == "OUT_FOR_DELIVERY":
            ofd_since[store].pop(ev.order_id, None)
    else:
        store = ev.store_id
        stores[store]["total"] += 1
    stores[store][ev.status.lower()] += 1
    if ev.status == "OUT_FOR_DELIVERY":
        ofd_since[store][ev.order_id] = ev.ts
    orders[ev.order_id] = (store, ev.status, ev.ts)
    stats["processed"] += 1


async def consumer() -> None:
    while True:
        apply(await queue.get())
        for _ in range(BATCH - 1):  # drain in batches
            if queue.empty():
                break
            apply(queue.get_nowait())
        await asyncio.sleep(0)


def enqueue(ev: Event) -> bool:
    try:
        queue.put_nowait(ev)
        return True
    except asyncio.QueueFull:
        stats["dropped"] += 1
        return False


# ---- Anomaly detection ----
def anomaly(store, typ, metric, value, threshold, message):
    sev = "high" if value >= 2 * threshold else "medium"
    return {"store_id": store, "type": typ, "severity": sev, "metric": metric,
            "value": round(value, 2), "threshold": threshold, "message": message}


def detect(snap: dict) -> list[dict]:
    out = []
    backlogs = {s: m["open"] + m["packed"] for s, m in snap.items()}
    avg = sum(backlogs.values()) / len(backlogs) if backlogs else 0
    for s, m in snap.items():
        b = backlogs[s]
        if b >= BACKLOG_MIN and b > BACKLOG_FACTOR * avg:
            out.append(anomaly(s, "UNUSUAL_BACKLOG", "open_plus_packed", b, round(BACKLOG_FACTOR * avg),
                               f"Store {s} backlog is {b} orders vs fleet avg {avg:.0f}."))
        delayed = m["ofd_over_delay"]
        if delayed >= DELAY_MIN_ORDERS:
            out.append(anomaly(s, "DELIVERY_DELAY", f"ofd_over_{DELAY_SEC:g}s", delayed, DELAY_MIN_ORDERS,
                               f"Store {s} has {delayed} orders out for delivery over {DELAY_SEC:g}s."))
        active = m["open"] + m["packed"] + m["out_for_delivery"]
        if active >= IMBALANCE_MIN_ACTIVE:
            share = m["open"] / active
            if share > OPEN_SHARE_MAX:
                out.append(anomaly(s, "STATUS_IMBALANCE", "open_share", share, OPEN_SHARE_MAX,
                                   f"Store {s}: {share:.0%} of active orders still OPEN (not being packed)."))
            ratio = m["packed"] / max(m["out_for_delivery"], 1)
            if ratio > PACKED_TO_OFD_MAX:
                out.append(anomaly(s, "STATUS_IMBALANCE", "packed_to_ofd_ratio", ratio, PACKED_TO_OFD_MAX,
                                   f"Store {s}: packed orders {ratio:.1f}x out-for-delivery (dispatch lagging)."))
    return out


async def snapshot_loop() -> None:
    last = 0
    while True:
        await asyncio.sleep(1)
        now = time.time()
        snap = {s: dict(m) for s, m in sorted(stores.items())}
        for s, m in snap.items():  # delivery-age stats (also feeds the LLM agent)
            ages = [now - t for t in ofd_since[s].values()]
            m["oldest_ofd_age_s"] = round(max(ages, default=0), 1)
            m["ofd_over_delay"] = sum(a > DELAY_SEC for a in ages)
        totals = {k: sum(m[k] for m in snap.values()) for k in ("total", "open", "packed", "out_for_delivery", "delivered")}
        found = detect(snap)
        affected = sorted({a["store_id"] for a in found})
        latest["dashboard"] = {
            "generated_at": now, "stores": snap, "totals": totals,
            "pipeline": {"queue_depth": queue.qsize(), "events_per_sec": stats["processed"] - last, **stats},
        }
        latest["anomalies"] = {
            "generated_at": now, "anomaly_count": len(found), "anomalies": found,
            "summary": (f"{len(found)} anomalies across {len(affected)} stores: "
                        + "; ".join(f"{a['store_id']} {a['type'].lower()}" for a in found)) if found
                       else "All stores operating normally.",
        }
        last = stats["processed"]


@asynccontextmanager
async def lifespan(_):
    tasks = [asyncio.create_task(consumer()), asyncio.create_task(snapshot_loop())]
    if os.getenv("SIMULATE", "1") == "1":
        tasks.append(asyncio.create_task(simulator.run(lambda **e: enqueue(Event(**e)))))
    if os.getenv("LLM_AGENT", "1") == "1":
        tasks.append(asyncio.create_task(agent.run(latest)))
    yield
    for t in tasks:
        t.cancel()


app = FastAPI(title="Live Ops Dashboard Buffer", lifespan=lifespan)


@app.post("/events", status_code=202)
async def post_events(events: Event | list[Event]):
    events = events if isinstance(events, list) else [events]
    accepted = sum(enqueue(e) for e in events)
    body = {"accepted": accepted, "dropped": len(events) - accepted}
    return JSONResponse(body, status_code=202 if accepted == len(events) else 429)


@app.get("/dashboard")
async def dashboard():
    return latest["dashboard"]


@app.get("/anomalies")
async def anomalies():
    return latest["anomalies"]


@app.get("/ai-anomalies")
async def ai_anomalies():
    return latest["ai_anomalies"]


@app.get("/stream")
async def stream():
    async def gen():
        while True:
            yield f"data: {json.dumps(latest)}\n\n"
            await asyncio.sleep(1)
    return StreamingResponse(gen(), media_type="text/event-stream")


@app.get("/")
async def ui():
    return FileResponse("dashboard.html")


@app.get("/health")
async def health():
    return {"status": "ok", "queue_depth": queue.qsize()}
