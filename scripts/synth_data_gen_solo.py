from __future__ import annotations

import json
import uuid
import random
import pandas as pd
import pyarrow

from pathlib import Path
from dataclasses import dataclass
from datetime import timedelta, datetime, timezone
from typing import Any

@dataclass(frozen=True)
class SynthDataConfig:
    num_messages: int = 200
    seed: int = 7
    jsonl_path = "data/raw/synthetic_posts.jsonl"
    parquet_path = "data/processed/synthetic_posts.parquet"


THEMES = {
    "server_connectivity": (
        "negative",
        [
            "Still can't connect after the patch",
            "Queue times are brutal",
            "Servers feel cooked right now",
            "Matchmaking keeps failing",
            "Constant disconnects mid-raid",
        ],
    ),
    "balance_nerf": (
        "negative",
        [
            "They stealth-nerfed my build",
            "Time-to-kill feels worse",
            "Why did they nerf this weapon?",
            "Movement feels slower",
            "This balance change is a miss",
        ],
    ),
    "balance_buff": (
        "positive",
        [
            "This buff finally makes it viable",
            "Combat feels smoother now",
            "Love the new tuning on weapons",
            "This is a W change",
            "Feels way more fair",
        ],
    ),
    "ui_qol": (
        "positive",
        [
            "UI is cleaner after patch",
            "QoL updates are actually great",
            "Finally fixed the annoying menu bug",
            "Inventory flow is better",
            "Small changes, big improvement",
        ],
    ),
    "bug_report": (
        "neutral",
        [
            "Bug: audio cuts out after extraction",
            "Seeing a weird clipping issue on textures",
            "Repro steps: open map then crash",
            "This might be a memory leak?",
            "Anyone else getting this error code?",
        ],
    ),
    "praise_general": (
        "positive",
        [
            "This patch is honestly solid",
            "Game feels better every update",
            "Big respect to the devs",
            "Love the direction lately",
            "This is why I keep coming back",
        ],
    ),
    "complaint_general": (
        "negative",
        [
            "This update is disappointing",
            "Feels rushed",
            "Not loving the direction",
            "Patch notes don't match what changed",
            "This is getting frustrating",
        ],
    ),


}

PATCHES = [
    ("patch_01_12_0", 0.45),        # represents cum = 0.00-0.45
    ("patch_01_13_0", 0.55),        # represents cum = 0.45-1.00 | +0.55
]
SOURCES = [
    ("reddit", 0.55),
    ("discord", 0.40),
    ("x", 0.05),
]

def _weighted_random_choice(items: list[tuple[Any, float]]) -> Any:
    r = random.random()
    cum = 0.0
    for val, w in items:
        cum += w
        if r <= cum:
            return val
    return items[-1][0]

def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("00:00", "Z")

def _pseudo_author_hash() -> str:
    return "_u" + uuid.uuid4().hex[:10]

def main(cfg: SynthDataConfig):
    random.seed(cfg.seed)

    # convert string paths to Path objects, mkdir if not existing
    jsonl_path = Path(cfg.jsonl_path)
    jsonl_path.parent.mkdir(parents=True, exist_ok=True)

    parquet_path = Path(cfg.parquet_path)
    parquet_path.parent.mkdir(parents=True, exist_ok=True)

    # calc time window
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=14)

    # create 250 random, pseudo threads
    thread_ids = [f"t_{uuid.uuid4().hex[:8]}" for _ in range(250)]

    # message records list of dicts type
    records: list[dict[str, Any]] = []

    # extract theme keys for random selection
    theme_key_labels = list(THEMES.keys())

    # random message loop generator
    for _ in range(cfg.num_messages):

        # create message id, patch name, source
        m_id = "m_" + uuid.uuid4().hex
        patch_id = _weighted_random_choice(PATCHES)
        source = _weighted_random_choice(SOURCES)

        # bias timestamp towards recent, add random seconds biased towards 0
        delta = random.random() ** 2
        created_at = start + timedelta(seconds=delta * (now - start).total_seconds())

        # choose a random thread, 25% message is reply, assign parent_id to message
        thread_id = random.choice(thread_ids)
        is_reply = random.random() < 0.25
        parent_id = "m_" + uuid.uuid4().hex if is_reply else None

        # get a random theme and message
        random_theme = random.choice(theme_key_labels)
        sentiment, message = THEMES[random_theme]


        # sprinkle slang
        random_message = random.choice(message)
        if random.random() < 0.35:
            random_message += random.choice(["like fr fr", "breh", "i'm done", "dope", "tbh", "lol", "wow"])
        # sprinkle ambiguity for language choice
        if random.random() < 0.15:
            random_theme = random_theme.replace("patch", "update").replace("nerf", "change")

        # gather metadata
        meta: dict[str, Any] = {}
        if source == "reddit":
            meta = {"score": int(random.gauss(25, 40)), "subreddit": "r/ArcRaiders"}
        elif source == "discord":
            meta = {"reactions": int(random.gauss(3, 6)), "channel": "patch_notes"}
        elif source == "x":
            meta = {"likes": int(random.gauss(10, 25)), "retweets": int(random.gauss(2, 10))}

        rec = {
            "m_id": m_id,
            "patch_id": patch_id,
            "source": source,
            "created_at": _iso(created_at),
            "thread_id": thread_id,
            "parent_id": parent_id,
            "author_hash": _pseudo_author_hash(),
            "post": random_message,
            "meta": meta,
            "label": random_theme,
            "sentiment": sentiment,
        }
        records.append(rec)


    with jsonl_path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    try:
        import pandas as  pd

        df = pd.DataFrame(records)
        df.to_parquet(parquet_path, index=False)
        print(f"Wrote:\n {jsonl_path}\n- {parquet_path}\nRows: {len(df)}")
    except Exception as e:
        print(f"Wrote: {jsonl_path}\nRows: {len(records)}")
        print("Parquet writing skipped, install pyarrow")
        print(f"Error: {repr(e)}")


if __name__ == "__main__":
    main(SynthDataConfig())


























