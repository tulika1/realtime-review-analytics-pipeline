"""Simple embeddings + vector search over reviews (hashing embedder locally, Titan on AWS)."""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
from dataclasses import dataclass

DIM = 256
_TOKEN = re.compile(r"[a-z]+")


class HashingEmbedder:
    model_id = "hashing-256"

    def embed(self, text: str) -> list[float]:
        vec = [0.0] * DIM
        for tok in _TOKEN.findall(text.lower()):
            h = int(hashlib.md5(tok.encode()).hexdigest(), 16)
            vec[h % DIM] += 1.0 if (h >> 8) & 1 else -1.0
        norm = math.sqrt(sum(v * v for v in vec)) or 1.0
        return [v / norm for v in vec]


class TitanEmbedder:
    model_id = "amazon.titan-embed-text-v2:0"

    def __init__(self, region: str | None = None):
        import boto3

        self.client = boto3.client("bedrock-runtime", region_name=region or os.environ.get("AWS_REGION"))

    def embed(self, text: str) -> list[float]:
        resp = self.client.invoke_model(
            modelId=self.model_id,
            body=json.dumps({"inputText": text, "dimensions": DIM, "normalize": True}),
        )
        return json.loads(resp["body"].read())["embedding"]


@dataclass
class Hit:
    review_id: str
    score: float
    metadata: dict


class LocalVectorIndex:
    def __init__(self):
        self._items: list[tuple[str, list[float], dict]] = []

    def upsert(self, review_id: str, vector: list[float], metadata: dict) -> None:
        self._items = [i for i in self._items if i[0] != review_id]
        self._items.append((review_id, vector, metadata))

    def delete(self, review_id: str) -> None:
        self._items = [i for i in self._items if i[0] != review_id]

    def query(self, vector: list[float], k: int = 5, where: dict | None = None) -> list[Hit]:
        where = where or {}
        hits = [
            Hit(rid, sum(a * b for a, b in zip(vector, vec)), meta)
            for rid, vec, meta in self._items
            if all(meta.get(f) == v for f, v in where.items())
        ]
        return sorted(hits, key=lambda h: h.score, reverse=True)[:k]

    def __len__(self) -> int:
        return len(self._items)


ANSWER_SYSTEM = """You answer questions from product and operations teams using only the customer
reviews provided. Cite review IDs in square brackets after each claim, e.g. [R-00012].
If the reviews do not contain the answer, say so plainly."""


def answer_with_claude(question: str, hits: list[Hit], region: str | None = None) -> str:
    from anthropic import AnthropicBedrockMantle

    client = AnthropicBedrockMantle(aws_region=region or os.environ.get("AWS_REGION", "us-east-1"))
    context = "\n".join(f"[{h.review_id}] {h.metadata['text']}" for h in hits)
    response = client.messages.create(
        model=os.environ.get("REVIEWLENS_MODEL", "anthropic.claude-opus-5-5"),
        max_tokens=4096,
        system=ANSWER_SYSTEM,
        messages=[{"role": "user", "content": f"Reviews:\n{context}\n\nQuestion: {question}"}],
    )
    if response.stop_reason == "refusal":
        return "The model declined to answer this question."
    return "".join(b.text for b in response.content if b.type == "text")
