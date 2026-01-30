# Synthetic Data Generator — Step-by-Step Task List

## Setup & Imports

- Import the future annotations module to enable modern type hints
- Import modules for:
  - JSON handling
  - randomization
  - UUID generation
  - dataclasses
  - datetime handling
  - file path operations
  - type hints
- Add a comment explaining this section handles data generation configuration

---

## Configuration Class

- Create a frozen dataclass called `SynthConfig` with four fields:
  - number of messages (default: 2500)
  - random seed (default: 42)
  - output JSONL path
  - output Parquet path

---

## Theme Definitions

- Create a dictionary called `THEMES` where each key is a theme label
- For each theme, store a tuple containing:
  - sentiment (positive / negative / neutral)
  - a list of 5 example phrases
- Define 7 themes total:
  - server connectivity issues
  - balance nerfs
  - balance buffs
  - UI quality of life improvements
  - bug reports
  - general praise
  - general complaints

---

## Weighted Distribution Lists

- Create a `PATCHES` list with two patch versions and their probability weights:
  - 0.45
  - 0.55
- Create a `SOURCES` list with three platforms and weights:
  - reddit → 0.55
  - discord → 0.40
  - x → 0.05
- Add inline comments showing the cumulative ranges each weight represents

---

## Helper Functions

- Write a weighted random choice function that:
  - generates a random float
  - accumulates weights in a loop
  - returns the first item where random value ≤ cumulative sum

- Write an ISO timestamp formatter that:
  - converts datetime to UTC ISO format
  - appends a `"Z"` suffix

- Write an author hash generator that:
  - creates a pseudo-anonymous user ID
  - uses `"u_"` prefix + 10 random hex characters

---

## Main Function Setup

- Define `main()` that accepts a `SynthConfig` parameter
- Set the random seed using config value
- Convert output paths from strings to `Path` objects
- Create parent directories if they do not exist
- Calculate the time window:
  - `now` = current UTC time
  - `start` = 14 days ago
- Generate 250 unique thread IDs using UUID hex values
- Initialize empty list to store message records
- Extract all theme keys into a list for random selection

---

## Message Generation Loop

- Loop for configured number of messages
- For each message:
  - generate unique message ID
  - select patch using weighted choice
  - select source using weighted choice
  - create timestamp biased toward recent dates:
    - square random value
    - interpolate between start and now
  - assign message to existing thread ID
  - decide reply status (35% chance)
  - generate parent ID if reply

- Theme + Text:
  - randomly select theme
  - extract sentiment and phrases
  - pick random phrase
  - add casual gaming slang (25% chance)
  - introduce ambiguity (10% chance)

- Platform Metadata:
  - reddit → score + subreddit
  - discord → reactions + channel
  - x → likes + retweets
  - use Gaussian distributions for realism

- Build final record dictionary:
  - IDs
  - source
  - patch
  - timestamp
  - thread info
  - author
  - text
  - metadata
  - ground truth labels

- Append record to list

---

## File Writing

- Open JSONL file in write mode
- Write each record as one JSON line
- Attempt to import pandas
  - if successful:
    - convert records → DataFrame
    - write Parquet file
    - print success message with paths + row count
  - if pandas unavailable:
    - catch exception
    - print JSONL-only success message
    - include note about missing dependency

---

## Entry Point

- Add standard Python entry point check
- Call `main()` with default `SynthConfig`
- Add comment explaining runtime configuration pattern

---
