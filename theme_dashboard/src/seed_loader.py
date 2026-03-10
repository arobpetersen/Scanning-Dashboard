import json
from pathlib import Path


def load_seed_file(seed_path: Path) -> list[dict]:
    with seed_path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    return payload.get("themes", [])
