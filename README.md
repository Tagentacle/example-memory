# Example Memory Node

Session persistence service for Tagentacle. Subscribes to `/memory/latest` and saves conversation history to disk as JSON files.

## Interfaces

- **Subscribe** `/memory/latest` — persists every memory update from Agent
- **Service** `/memory/load` — load a session by ID (`{"session_id": "xxx"}` → `{"messages": [...]}`)
- **Service** `/memory/list` — list all saved sessions

## Storage

Sessions are saved to `sessions/{session_id}.json` in the node's working directory.
