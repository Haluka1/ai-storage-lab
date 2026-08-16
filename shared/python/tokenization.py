from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Iterable


ALGORITHM = "approx-tokenizer-sha256-units-v1"
VOCAB_MODULUS = 1_000_000
TOKEN_ID_OFFSET = 1
CONFIG = {
    "algorithm": ALGORITHM,
    "normalization": "none",
    "unitization": "ascii_word_or_single_nonspace_rune",
    "token_hash": "sha256(revision + NUL + unit)",
    "vocab_modulus": VOCAB_MODULUS,
    "token_id_offset": TOKEN_ID_OFFSET,
    "scope": "router_runtime_approximation",
}
CONFIG_HASH = hashlib.sha256(json.dumps(CONFIG, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()[:16]
TOKENIZER_REVISION = f"approx-tokenizer-v1:config-sha256={CONFIG_HASH}"


@dataclass(frozen=True)
class TokenizationResult:
    tokenizer_revision: str
    units: list[str]
    tokens: list[int]


def is_ascii_word_char(ch: str) -> bool:
    return ch == "_" or ("0" <= ch <= "9") or ("A" <= ch <= "Z") or ("a" <= ch <= "z")


def iter_units(prompt: str) -> Iterable[str]:
    i = 0
    while i < len(prompt):
        ch = prompt[i]
        if ch.isspace():
            i += 1
            continue
        if ch.isascii() and is_ascii_word_char(ch):
            start = i
            i += 1
            while i < len(prompt) and prompt[i].isascii() and is_ascii_word_char(prompt[i]):
                i += 1
            yield prompt[start:i]
            continue
        yield ch
        i += 1


def token_id(unit: str, tokenizer_revision: str = TOKENIZER_REVISION) -> int:
    payload = tokenizer_revision.encode("utf-8") + b"\x00" + unit.encode("utf-8")
    digest = hashlib.sha256(payload).digest()
    return int.from_bytes(digest[:8], "big") % VOCAB_MODULUS + TOKEN_ID_OFFSET


def approximate_tokenize(prompt: str, tokenizer_revision: str = TOKENIZER_REVISION) -> TokenizationResult:
    units = list(iter_units(prompt))
    return TokenizationResult(
        tokenizer_revision=tokenizer_revision,
        units=units,
        tokens=[token_id(unit, tokenizer_revision) for unit in units],
    )
