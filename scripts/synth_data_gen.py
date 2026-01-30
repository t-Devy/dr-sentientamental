from __future__ import annotations

import json
import uuid
import pandas as pd
import random

from pathlib import Path
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


@dataclass(frozen=True)
class SynthDataGenConfig:
    n_messages = 300
    seed = 12
    out_jsonl = "data/raw/synth_posts.jsonl"
    out_parq = "data/processed/synth_posts.parquet"

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
    ("patch_01_12_0", 0.45),
    ("patch_01_13_0", 0.55),
]
SOURCES = [
    ("reddit", 0.55),
    ("discord", 0.40),
    ("x", 0.05),
]

# weighted random choices helper
def _weighted_random(items: list[tuple[Any, float]]) -> Any:
    r = random.random()
    cum = 0.0
    for val, w in items:
        cum += w
        if r <= cum:
            return val
    return items[-1][0]

# datetime to string helper
def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("00:00", "Z")

# hash generator for users
def _pseudo_author_hash() -> str:
    return "u_" + uuid.uuid4().hex[:10]

def main(cfg: SynthDataGenConfig):
    # set random seed
    random.seed(cfg.seed)

    # create path objects for artifacts
    out_jsonl = Path("data/raw/synth_posts.jsonl")
    out_jsonl.parent.mkdir(parents=True, exist_ok=True)
    out_parq = Path("data/processed/synth_posts.parquet")
    out_parq.parent.mkdir(parents=True, exist_ok=True)

    # set time window 14 days ago to now
    now = datetime.now()
    start = now - timedelta(days=14)

    # generate thread ids
    thread_ids = [f"t_{uuid.uuid4().hex[:8]}" for _ in range(200)]      # the thread ids use a range for a set number

    # records list
    records: list[dict[str, Any]] = []

    # all theme keys extract
    theme_keys = list(THEMES.keys())


    # message gen loop
    for _ in range(cfg.n_messages):
        # create message with random patch and source
        msg_id = "m_" + uuid.uuid4().hex
        patch_id = _weighted_random(PATCHES)
        source = _weighted_random(SOURCES)

        # create time data attached to message, biased towards 0 and start date
        delta = random.random() ** 2
        created_at = start + timedelta(seconds=delta * (now - start).total_seconds())


        # assign message to thread, create parent thread and if it's a reply
        thread_id = random.choice(thread_ids)
        is_reply = random.random() < 0.30
        parent_id = "m_" + uuid.uuid4().hex if is_reply else None


        # randomly select theme
        random_theme = random.choice(theme_keys)

        # extract sentiment/posts
        sentiment, posts = THEMES[random_theme]

        # introduce random slang and ambiguity with interchangeable wording
        post = random.choice(posts)
        if random.random() < 0.20:
            post += " " + random.choice(["tbh", "lol", "imo", "fr fr", "wow", "bet", "dope", "thas crazy"])
        if random.random() < 0.15:
            post = post.replace("patch", "update").replace("nerf", "change")

        # build random meta-data for each post
        meta: dict[str, Any] = {}
        if source == "reddit":
            meta = {"score": int(random.gauss(25, 40)), "subreddit": "r/ArcRaiders"}
        elif source == "discord":
            meta = {"reactions": int(random.gauss(3, 6)), "channel": "patch_notes"}
        elif source == "x":
            meta = {"likes": int(random.gauss(10, 25)), "retweets": int(random.gauss(3, 15))}

        # create the final record format with all loop creations
        rec = {
            "m_id": msg_id,
            "source": source,
            "patch": patch_id,
            "timestamp": _iso(created_at),
            "thread_id": thread_id,
            "parent_id": parent_id,
            "author": _pseudo_author_hash(),
            "post": post,
            "metadata": meta,
            "theme": random_theme,
            "sentiment": sentiment,
        }
        records.append(rec)

    # file writing
    with out_jsonl.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    try:
        df = pd.DataFrame(records)
        df.to_parquet(out_parq, index=False)
        print(f"Wrote:\n- {out_jsonl}\n- {out_parq}\nRows: {len(df)}")
    except Exception as e:
        print(f"Wrote:\n- {out_jsonl}\nRows: {len(records)}")
        print("Skipped parquet, import pandas + pyarrow")
        print(f"Error:", repr(e))



if __name__ == "__main__":
    main(SynthDataGenConfig())























