"""LLM anomaly agent: every few seconds, sends the latest snapshot to Gemini and gets structured anomalies back."""
import asyncio, json, logging, os, time
from typing import Literal

from dotenv import load_dotenv
from google import genai
from google.genai import types
from pydantic import BaseModel

load_dotenv()  # GOOGLE_API_KEY from .env
log = logging.getLogger("uvicorn.error")

MODEL = os.getenv("AI_MODEL", "gemini-flash-latest")
INTERVAL = float(os.getenv("AI_INTERVAL", 10))  # seconds between LLM calls (1s would be too slow and costly)

SYSTEM = """You are an operations analyst for a quick-commerce delivery network.
You receive a live snapshot of per-store order counts (open, packed, out_for_delivery, delivered),
delivery-age stats, pipeline health, and the anomalies already flagged by fixed rules.
Find stores that look unusual compared to the rest of the fleet: backlogs, delivery delays,
status imbalances, or anything else operationally concerning (including issues the rules missed).
Only report real issues backed by the numbers. Keep reasons short and cite the numbers."""


class AIAnomaly(BaseModel):
    store_id: str
    type: Literal["UNUSUAL_BACKLOG", "DELIVERY_DELAY", "STATUS_IMBALANCE", "OTHER"]
    severity: Literal["low", "medium", "high"]
    reason: str
    suggested_action: str


class AIReport(BaseModel):
    summary: str
    anomalies: list[AIAnomaly]


async def run(latest: dict) -> None:
    try:
        client = genai.Client()
    except Exception as e:  # no API key configured
        latest["ai_anomalies"] = {"status": "disabled", "error": str(e)}
        return
    config = types.GenerateContentConfig(system_instruction=SYSTEM, response_mime_type="application/json",
                                         response_schema=AIReport)
    while True:
        await asyncio.sleep(INTERVAL)
        snapshot = {"dashboard": latest["dashboard"], "rule_anomalies": latest["anomalies"]["anomalies"]}
        try:
            resp = await client.aio.models.generate_content(model=MODEL, contents=json.dumps(snapshot), config=config)
            if resp.parsed is None:
                raise RuntimeError(f"no report: {resp.text[:200] if resp.text else 'empty response'}")
            latest["ai_anomalies"] = {"status": "ok", "generated_at": time.time(),
                                      "snapshot_at": snapshot["dashboard"].get("generated_at"),
                                      "model": MODEL, **resp.parsed.model_dump()}
        except Exception as e:
            log.warning("AI agent error: %s", e)
            latest["ai_anomalies"] = {**latest.get("ai_anomalies", {}), "status": "error", "error": str(e)}
