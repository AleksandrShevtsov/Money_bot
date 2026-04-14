import json
import time
from pathlib import Path


class BotStateStore:
    def __init__(self, path: str = "bot_runtime_state.json"):
        self.path = Path(path)

    def load(self) -> dict:
        if not self.path.exists():
            return {}

        try:
            return json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def save(self, state: dict):
        payload = dict(state)
        payload["saved_at"] = time.time()
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
