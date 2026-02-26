"""
Tagentacle Memory Node: Session Persistence Service.

Subscribes to /memory/latest and persists conversation history to disk.
Also provides /memory/load Service for session recovery.

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

from tagentacle_py_core import Node

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SESSIONS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sessions")


async def main():
    # Ensure storage directory exists
    os.makedirs(SESSIONS_DIR, exist_ok=True)

    node = Node("memory_node")
    await node.connect()

    @node.subscribe("/memory/latest")
    async def on_memory_update(msg: dict):
        """Persist conversation state to disk on every update."""
        payload = msg.get("payload", {})
        session_id = payload.get("session_id", "default")
        messages = payload.get("messages", [])

        filepath = os.path.join(SESSIONS_DIR, f"{session_id}.json")
        try:
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump({
                    "session_id": session_id,
                    "messages": messages,
                    "message_count": len(messages),
                }, f, ensure_ascii=False, indent=2)
            logger.info(f"Persisted session '{session_id}': {len(messages)} messages -> {filepath}")
        except Exception as e:
            logger.error(f"Failed to persist session '{session_id}': {e}")

    @node.service("/memory/load")
    async def handle_load(payload: dict) -> dict:
        """
        Load a saved session from disk.

        Request:  {"session_id": "xxx"}
        Response: {"session_id": "xxx", "messages": [...]}
        """
        session_id = payload.get("session_id", "default")
        filepath = os.path.join(SESSIONS_DIR, f"{session_id}.json")

        if not os.path.isfile(filepath):
            logger.info(f"Session '{session_id}' not found on disk.")
            return {"session_id": session_id, "messages": []}

        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            messages = data.get("messages", [])
            logger.info(f"Loaded session '{session_id}': {len(messages)} messages")
            return {"session_id": session_id, "messages": messages}
        except Exception as e:
            logger.error(f"Failed to load session '{session_id}': {e}")
            return {"session_id": session_id, "messages": [], "error": str(e)}

    @node.service("/memory/list")
    async def handle_list(payload: dict) -> dict:
        """
        List all saved sessions.

        Response: {"sessions": ["session_id_1", "session_id_2", ...]}
        """
        sessions = []
        try:
            for fname in os.listdir(SESSIONS_DIR):
                if fname.endswith(".json"):
                    sessions.append(fname[:-5])  # strip .json
        except Exception as e:
            logger.error(f"Failed to list sessions: {e}")
        return {"sessions": sorted(sessions)}

    logger.info(f"Memory Node ready. Storage: {SESSIONS_DIR}")
    logger.info("Subscribed to /memory/latest | Services: /memory/load, /memory/list")
    await node.spin()


if __name__ == "__main__":
    asyncio.run(main())
