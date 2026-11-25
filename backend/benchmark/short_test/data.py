"""
The synthetic documents of the short test. Everything here is deterministic: the same call gives the same documents
on every run and on every branch (python's random.Random with a fixed seed; no numpy needed).

Documents are made in batches of BATCH. Batch b of a project only depends on (SEED, project, b), so batches can be
made in any order and in other processes.
"""

import random
from datetime import datetime, timedelta
from typing import Any

SEED = 20261125
BATCH = 5000
VOCAB_SIZE = 30_000

# The fields of both projects. "source" (keyword) and "date" get the standard sort slots automatically; the other
# fields are not in a sort slot.
FIELDS: dict[str, str] = {
    "title": "text",
    "text": "text",
    "source": "keyword",
    "author": "keyword",
    "section": "keyword",
    "n_int": "integer",
    "n_num": "number",
    "date": "date",
    "tags": "tag",
    "url": "url",
}

SOURCES = [f"source_{i:02d}" for i in range(50)]
# mixed case, digits, a space and a non-ascii letter: so the byte order of sorting is tested too
SECTIONS = ["Sport", "economy", "Politics", "art", "Zoo", "10", "9", "a b", "épsilon", "Opinion", "world", "Tech"]
TAGS = [f"tag{i:02d}" for i in range(30)]
N_AUTHORS = 20_000
_FIRST = ["Anna", "bob", "Chris", "dana", "Élise", "Finn", "greta", "Hugo", "ivo", "Zoë"]
DATE_START = datetime(2000, 1, 1)
DATE_DAYS = 25 * 365  # documents are dated 2000 to 2024


def author(i: int) -> str:
    return f"{_FIRST[i % len(_FIRST)]} Writer{i:05d}"


def make_vocabulary() -> list[str]:
    """30,000 made-up words in a fixed order. The position is the frequency rank: VOCAB[0] is the most common word"""
    rng = random.Random(SEED)
    syllables = ["ka", "lo", "mi", "ne", "ru", "sa", "ti", "vo", "ze", "pa", "do", "gi", "fu", "be", "xo", "wy"]
    vocab: set[str] = set()
    while len(vocab) < VOCAB_SIZE:
        vocab.add("".join(rng.choice(syllables) for _ in range(rng.randint(2, 4))))
    words = sorted(vocab)
    rng.shuffle(words)
    return words


VOCAB = make_vocabulary()


def word(rank: int) -> str:
    """The word with this frequency rank (1 = most common)"""
    return VOCAB[rank - 1]


# Words used by the fixed queries. A word of rank r is chosen with probability log(1 + 1/r) / log(30000) per position
# (Zipf's law), so with about 125 words per document: rank 1 is in almost every document, rank 300 in about 4%,
# rank 15000 in under 0.1%.
COMMON = word(1)
MEDIUM = word(300)
RARE = word(15_000)


def _pick(rng: random.Random) -> str:
    return VOCAB[int(VOCAB_SIZE ** rng.random()) - 1]


def make_doc(rng: random.Random, k: int) -> dict[str, Any]:
    """Document number k (0-based) of a project. Call in order within a batch: the rng state carries over"""
    n_words = 15 + int(386 * rng.random() ** 2.5)  # 15 to 400 words, most documents are short
    source = SOURCES[int(len(SOURCES) ** rng.random()) - 1]  # some sources are much bigger than others
    doc: dict[str, Any] = {
        "_id": f"d{k:07d}",
        "title": " ".join(_pick(rng) for _ in range(rng.randint(4, 10))).capitalize(),
        "text": " ".join([_pick(rng) for _ in range(n_words)]),
        "source": source,
        "section": SECTIONS[rng.randrange(len(SECTIONS))],
        # not random: exactly one in 1000 documents has each value, so "n_int < 100" is exactly 10% of a project
        "n_int": (k * 7919 + 13) % 1000,
        "url": f"https://example.org/{source}/{k}",
    }
    if rng.random() < 0.9:
        doc["author"] = author(int(N_AUTHORS * rng.random() ** 1.5))
    r = rng.random()
    if r < 0.45:
        doc["n_num"] = rng.randrange(0, 100)  # whole values
    elif r < 0.9:
        doc["n_num"] = rng.randrange(-2000, 10000) / 100  # decimal values (a few are whole, like 17.0)
    day = DATE_START + timedelta(days=rng.randrange(DATE_DAYS))
    if rng.random() < 0.5:
        doc["date"] = day.strftime("%Y-%m-%d")  # a date without time
    else:
        doc["date"] = (day + timedelta(seconds=rng.randrange(86400))).strftime("%Y-%m-%dT%H:%M:%S")
    n_tags = rng.choice([0, 1, 1, 2, 2, 3, 4])
    if n_tags:
        tags: list[str] = []
        while len(tags) < n_tags:
            tag = TAGS[int(len(TAGS) ** rng.random()) - 1]
            if tag not in tags:
                tags.append(tag)
        doc["tags"] = tags  # (in this order: not sorted)
    return doc


def make_batch(project: str, batch: int, total: int) -> list[dict[str, Any]]:
    """Batch number `batch` of a project with `total` documents"""
    rng = random.Random(f"{SEED}-{project}-{batch}")
    start = batch * BATCH
    return [make_doc(rng, k) for k in range(start, min(start + BATCH, total))]
