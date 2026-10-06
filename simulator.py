"""In-process event producer. Some stores are deliberately slow so anomalies show up."""
import asyncio, random, time, uuid

STORES = [f"s{i}" for i in range(1, 11)]
NEXT = {"OPEN": "PACKED", "PACKED": "OUT_FOR_DELIVERY", "OUT_FOR_DELIVERY": "DELIVERED"}
TICK = 0.1             # seconds
NEW_PER_TICK = 30      # ~300 new orders/s
ADVANCE_P = 0.05       # per-tick chance an order moves to next status (~2s per stage)
SLOW = {               # store -> {status: slower advance prob}
    "s3": {"OUT_FOR_DELIVERY": 0.001},  # delivery delays
    "s5": {"PACKED": 0.004},            # packed piling up (dispatch lag)
    "s7": {"OPEN": 0.004},              # open backlog (packing lag)
}


async def run(emit) -> None:
    active: dict[str, tuple[str, str]] = {}  # order_id -> (store, status)
    while True:
        now = time.time()
        for _ in range(NEW_PER_TICK):
            oid, store = uuid.uuid4().hex[:12], random.choice(STORES)
            active[oid] = (store, "OPEN")
            emit(order_id=oid, store_id=store, status="OPEN", ts=now)
        for oid, (store, status) in list(active.items()):
            if random.random() < SLOW.get(store, {}).get(status, ADVANCE_P):
                new = NEXT[status]
                emit(order_id=oid, store_id=store, status=new, ts=now)
                if new == "DELIVERED":
                    del active[oid]
                else:
                    active[oid] = (store, new)
        await asyncio.sleep(TICK)
