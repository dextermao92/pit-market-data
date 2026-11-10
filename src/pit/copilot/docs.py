"""Retrieval over the ops docs (ops/**/*.md) and the README: Markdown chunks by heading, BM25 (DuckDB fts) and
vectors (bge-small-en-v1.5 via fastembed, local), fused with reciprocal rank fusion. All in an in-memory DuckDB.

Why retrieval here: runbooks and incident write-ups are prose, so the question "what do I do about X" has to find
the right section. Structured data (runs, SLOs) goes through tools, not retrieval.

Usage: uv run python -m pit.copilot.docs "the 09:02 card says contract lookups stalled" [--mode bm25|vector|hybrid]
"""

import hashlib
import os
import re
import sys
from functools import cache
from pathlib import Path

import duckdb
import polars as pl

ROOT = Path(__file__).resolve().parents[3]
MODEL = "BAAI/bge-small-en-v1.5"  # 67 MB, 384 dims, English (the docs are English; the agent queries in English)
DIM = 384
MAX_CHARS = 1500
RRF_K = 60


def _data_dir() -> Path:
    return Path(os.environ.get("DATA_DIR", ROOT / "data"))


def sources() -> list[Path]:
    return sorted((ROOT / "ops").rglob("*.md")) + [ROOT / "README.md"]


def chunk(name: str, text: str) -> list[dict]:
    """One chunk per heading section (path of headings kept), long sections split at blank lines."""
    out, path, body = [], [], []

    def flush():
        content = "\n".join(body).strip()
        if not content:
            return
        pieces, cur = [], ""
        for para in re.split(r"\n\s*\n", content):
            if cur and len(cur) + len(para) > MAX_CHARS:
                pieces.append(cur)
                cur = ""
            cur = f"{cur}\n\n{para}" if cur else para
        pieces.append(cur)
        for p in pieces:
            out.append({"id": f"{name}#{len(out)}", "doc": name, "section": " > ".join(path), "text": p})

    in_code = False
    for line in text.splitlines():
        in_code ^= line.startswith("```")
        m = None if in_code else re.match(r"(#{1,4}) (.+)", line)
        if m:
            flush()
            body = []
            level = len(m.group(1))
            path = path[:level - 1] + [m.group(2).strip()]
        else:
            body.append(line)
    flush()
    return out


def chunks() -> list[dict]:
    return [c for p in sources() for c in chunk(str(p.relative_to(ROOT / "ops")).removesuffix(".md")
                                               if p.parent != ROOT else p.stem, p.read_text())]


@cache
def _model():
    from fastembed import TextEmbedding
    return TextEmbedding(MODEL, cache_dir=str(_data_dir() / "copilot" / "models"))


def embed_passages(texts: list[str]) -> list[list[float]]:
    """Embeddings for texts, cached on disk by content hash, so only changed chunks are re-embedded."""
    path = _data_dir() / "copilot" / f"embeddings-{MODEL.replace('/', '_')}.parquet"
    cached = dict(pl.read_parquet(path).iter_rows()) if path.exists() else {}
    keys = [hashlib.sha256(t.encode()).hexdigest() for t in texts]
    todo = [t for k, t in zip(keys, texts) if k not in cached]
    if todo:
        cached |= {hashlib.sha256(t.encode()).hexdigest(): v.tolist() for t, v in zip(todo, _model().passage_embed(todo))}
        path.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"sha": list(cached), "emb": list(cached.values())},
                     schema={"sha": pl.String, "emb": pl.List(pl.Float32)}).write_parquet(path)
    return [cached[k] for k in keys]


def embed_query(text: str) -> list[float]:
    return next(iter(_model().query_embed([text]))).tolist()  # bge's query instruction is added by fastembed


class Index:
    def __init__(self, rows: list[dict], embed=(embed_passages, embed_query)):
        self.embed_passages, self.embed_query = embed
        self.con = duckdb.connect()
        self.con.execute("install fts; load fts")
        self.con.execute(f"create table chunks (id varchar, doc varchar, section varchar, text varchar, emb float[{DIM}])")
        embs = self.embed_passages([f"{r['doc']} | {r['section']}\n{r['text']}" for r in rows])
        self.con.executemany("insert into chunks values (?, ?, ?, ?, ?)",
                             [(r["id"], r["doc"], r["section"], r["text"], e) for r, e in zip(rows, embs)])
        # section and doc name are searchable too: a heading is often the best match for a question
        # keep digits: the default tokenizer drops them, and error codes (10197, 1100) are what ops questions quote
        self.con.execute("pragma create_fts_index('chunks', 'id', 'doc', 'section', 'text', stemmer = 'porter', "
                         "stopwords = 'english', ignore = '(\\.|[^a-z0-9])+', overwrite = 1)")

    def search(self, query: str, k: int = 5, mode: str = "hybrid") -> list[dict]:
        qv = self.embed_query(query) if mode != "bm25" else None
        sql = f"""
        with bm25 as (
            select id, row_number() over (order by s desc) as r
            from (select id, fts_main_chunks.match_bm25(id, $q) as s from chunks) where s is not null
        ), vec as (
            select id, row_number() over (order by array_cosine_similarity(emb, $v::float[{DIM}]) desc) as r
            from chunks where $v is not null
        )
        select c.id, c.doc, c.section, c.text, b.r as bm25_rank, v.r as vector_rank,
               (case when $mode in ('bm25', 'hybrid') then coalesce(1.0 / ({RRF_K} + b.r), 0) else 0 end) +
               (case when $mode in ('vector', 'hybrid') then coalesce(1.0 / ({RRF_K} + v.r), 0) else 0 end) as score
        from chunks c left join bm25 b using (id) left join vec v using (id)
        where score > 0
        order by score desc, c.id
        limit $k"""
        rel = self.con.execute(sql, {"q": query, "v": qv, "mode": mode, "k": k})
        cols = [d[0] for d in rel.description]
        return [dict(zip(cols, row)) for row in rel.fetchall()]


_index: tuple[str, Index] | None = None


def index() -> Index:
    """The index over the current docs, rebuilt when any source changes."""
    global _index
    rows = chunks()
    fingerprint = hashlib.sha256("".join(r["id"] + r["text"] for r in rows).encode()).hexdigest()
    if _index is None or _index[0] != fingerprint:
        _index = (fingerprint, Index(rows))
    return _index[1]


if __name__ == "__main__":
    mode = sys.argv[sys.argv.index("--mode") + 1] if "--mode" in sys.argv else "hybrid"
    query = " ".join(a for i, a in enumerate(sys.argv[1:], 1) if a != "--mode" and sys.argv[i - 1] != "--mode")
    for r in index().search(query, mode=mode):
        print(f"{r['score']:.4f}  bm25={r['bm25_rank']} vec={r['vector_rank']}  {r['doc']}  §{r['section']}")
