from __future__ import annotations

import json
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

# package data in/out processing variables
@dataclass(frozen=True)
class SynthConfig:
    n_messages: int = 2500
    seed: int = 42
    out_jsonl: str = "data/raw/synth_messages.jsonl"
    out_parquet: str = "data/processed/synth_messages.parquet"


THEMES = {
    # theme_label: (sentiment, example_phrases)
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
    ("patch_1_12_0", 0.45),
    ("patch_1_13_0", 0.55),
]

SOURCES = [
    ("reddit", 0.55),
    ("discord", 0.40),      # represents 0.55-0.95 || +0.40
    ("x", 0.05)     # represents 0.95-1.00 || +0.05
]

# weighted random selection algo used for choosing PATCH and SOURCE to build synthetic data
def weighted_choice(items: list[tuple[Any, float]]) -> Any:
    r = random.random()
    cum = 0.0
    for val, w in items:
        cum += w
        if r <= cum:
            return val
    return items[-1][0]


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def make_author_hash() -> str:
    # pseudo has for synthetic data
    return "u_" + uuid.uuid4().hex[:10]

def main(cfg: SynthConfig) -> None:
    random.seed(cfg.seed)

    out_jsonl = Path(cfg.out_jsonl)
    out_parquet = Path(cfg.out_parquet)
    out_jsonl.parent.mkdir(parents=True, exist_ok=True)
    out_parquet.parent.mkdir(parents=True, exist_ok=True)

    # time window for data scrape 14 days prior to now
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=14)

    # thread simulation
    thread_ids = [f"t_{uuid.uuid4().hex[:8]}" for _ in range(250)]

    records: list[dict[str, Any]] = []

    theme_keys = list(THEMES.keys())

    for _ in range(cfg.n_messages):
        msg_id = "m_" + uuid.uuid4().hex
        patch_id = weighted_choice(PATCHES)
        source = weighted_choice(SOURCES)

        # timestamp distribution (bias for recent)
        delta = random.random() ** 2        # square 0.5 seconds = 0.25 seconds, smaller
        created_at = start + timedelta(seconds=delta * (now - start).total_seconds())       # adding some random amount of seconds to our start time

        thread_id = random.choice(thread_ids)
        is_reply = random.random() < 0.35
        parent_id = ("m_" + uuid.uuid4().hex) if is_reply else None

        label_true = random.choice(theme_keys)      # e.g. "server_connectivity"
        sentiment_true, phrases = THEMES[label_true]

        # randomly choose phrases and introduce noise into text/phrase data
        text = random.choice(phrases)
        if random.random() < 0.25:
            text += " " + random.choice(["tbh", "imo", "lol", "anyone else", "pls fix", "devs?", "this is wild"])
        if random.random() < 0.10:
            #introduce ambiguity
            text = text.replace("patch", "update").replace("nerf", "change")

        meta: dict[str, Any] = {}
        if source == "reddit":
            meta = {"score": int(random.gauss(25, 40)), "subreddit": "ArcRaiders"}      # mean and sd of floating point distribution
        elif source == "discord":
            meta = {"reactions": int(random.gauss(3, 6)), "channel": "patch_notes"}
        elif source == "x":
            meta = {"likes": int(random.gauss(10, 25)), "retweets": int(random.gauss(2, 6))}

        rec = {
            "id": msg_id,
            "source": source,
            "patch_id": patch_id,
            "created_at": iso(created_at),
            "thread_id": thread_id,
            "parent_id": parent_id,
            "author_hash": make_author_hash(),
            "text": text,
            "meta": meta,
            # synthetic truths
            "label_true": label_true,
            "sentiment_true": sentiment_true,
        }
        records.append(rec)

    # Write JSONL
    with out_jsonl.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    # Write Parquet
    try:
        import pandas as pd

        df = pd.DataFrame(records)
        df.to_parquet(out_parquet, index=False)
        print(f"Wrote:\n-{out_jsonl}\n- {out_parquet}\nRows: {len(df)}")
    except Exception as e:
        print(f"Wrote:\n- {out_jsonl}\nRows: {len(records)}")
        print("Parquet write skipped (install pandas + pyarrow)")
        print("Error:", repr(e))



if __name__ == "__main__":
    main(SynthConfig())     # allows us to configure SynthConfig at runtime e.g. n_messages=5000, seed=7, scales better when config params increase









