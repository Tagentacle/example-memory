"""
Tagentacle Memory Node: Session Persistence Service.

Subscribes to /memory/latest and persists conversation history to disk.
Also provides /memory/load and /memory/list Services for session recovery.

Think of this as the "hard disk" to the Agent's "RAM":
- Agent keeps messages[] in memory for fast access
- Memory Node persists every update to disk for durability
- On Agent restart, it can call /memory/load to recover state

Storage: sessions/{session_id}.json
"""

import asyncio
import json
import logging
import os
import time

from tagentacle_py_core import Node

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SESSIONS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sessions")
MAX_RETRIES = 3
RETRY_DELAY = 0.5


def _safe_write(filepath: str, data: dict, retries: int = MAX_RETRIES) -> bool:
    """Write JSON atomically with retry. Returns True on success."""
    tmp = filepath + ".tmp"
    for attempt in range(retries):
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, filepath)
            return True
        except Exception as e:
            logger.warning("Write attempt %d/%d failed for %s: %s",
                           attempt + 1, retries, filepath, e)
            if attempt < retries - 1:
                time.sleep(RETRY_DELAY)
    return False


async def main():
    os.makedirs(SESSIONS_DIR, exist_ok=True)

    node = Node("memory_node")
    await node.connect()

    @node.subscribe("/memory/latest")
    async def on_memory_update(msg: dict):
        """Persist conversation state to disk on every update."""
        payload = msg.get("payload", {})
        session_id = payload.get("session_id", "default")
        messages = payload.get("messages", [])

        data = {
            "session_id": session_id,
            "messages": messages,
            "message_count": len(messages),
            "last_updated": time.time(),
        }

        filepath = os.path.join(SESSIONS_DIR, f"{session_id}.json")
        ok = await asyncio.get_event_loop().run_in_executor(
            None, _safe_write, filepath, data,
        )
        if ok:
            logger.info("Persisted session '%s': %d messages", session_id, len(messages))
        else:
            logger.error("Failed to persist session '%s' after %d retries",
                         session_id, MAX_RETRIES)

    @node.service("/memory/load")
    async def handle_load(payload: dict) -> dict:
        """Load a saved session from disk.

        Request:  {"session_id": "xxx"}
        Response: {"session_id": "xxx", "messages": [...], "message_count": N}
        """
        session_id = payload.get("session_id", "default")
        filepath = os.path.join(SESSIONS_DIR, f"{session_id}.json")

        if not os.path.isfile(filepath):
            logger.info("Session '%s' not found on disk.", session_id)
            return {"session_id": session_id, "messages": [], "message_count": 0}

        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            messages = data.get("messages", [])
            logger.info("Loaded session '%s': %d messages", session_id, len(messages))
            return {
                "session_id": session_id,
                "messages": messages,
                "message_count": len(messages),
                "last_updated": data.get("last_updated"),
            }
        except Exception as e:
            logger.error("Failed to load session '%s': %s", session_id, e)
            return {"session_id": session_id, "messages": [], "error": str(e)}

    @node.service("/memory/list")
    async def handle_list(payload: dict) -> dict:
        """List all saved sessions with metadata.

        Response: {"sessions": [{"session_id": ..., "message_count": ..., "last_updated": ...}]}
        """
        sessions = []
        try:
            for fname in sorted(os.listdir(SESSIONS_DIR)):
                if not fname.endswith(".json"):
                    continue
                filepath = os.path.join(SESSIONS_DIR, fname)
                try:
                    with open(filepath, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    sessions.append({
                        "session_id": data.get("session_id", fname[:-5]),
                        "message_count": data.get("message_count", 0),
                        "last_updated": data.get("last_updated"),
                    })
                except Exception:
                    sessions.append({
                        "session_id": fname[:-5],
                        "message_count": -1,
                        "last_updated": None,
                    })
        except Exception as e:
            logger.error("Failed to list sessions: %s", e)
        return {"sessions": sessions}

    logger.info(f"Memory Node ready. Storage: {SESSIONS_DIR}")
    logger.info("Subscribed to /memory/latest | Services: /memory/load, /memory/list")
    await node.spin()


if __name__ == "__main__":
    asyncio.run(main())
