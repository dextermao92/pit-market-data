import hashlib

from pit.copilot.docs import DIM, Index, chunk, chunks

DOC = """# Runbook: Gateway

Intro line.

## C. No data

Self-healing never runs here.

| Code | Fix |
|---|---|
| 10197 | Log the other session out |

```bash
# not a heading inside code
docker compose ps
```

### Sub

Detail.
"""


def test_chunks_follow_headings_and_ignore_code_comments():
    cs = chunk("runbooks/gateway", DOC)
    assert [c["section"] for c in cs] == ["Runbook: Gateway", "Runbook: Gateway > C. No data", "Runbook: Gateway > C. No data > Sub"]
    assert "# not a heading inside code" in cs[1]["text"] and cs[1]["id"] == "runbooks/gateway#1"


def test_long_sections_split_at_paragraphs():
    long = "# T\n\n" + "\n\n".join("p" * 600 for _ in range(5))
    assert [len(c["text"]) for c in chunk("x", long)] == [1202, 1202, 600]


def test_real_corpus_has_every_ops_doc_and_the_readme():
    docs = {c["doc"] for c in chunks()}
    assert {"README", "slo", "runbooks/gateway", "incidents/2026-10-26"} <= docs


def fake_embed(text):
    """Deterministic bag-of-words vectors: no model download in tests."""
    v = [0.0] * DIM
    for w in text.lower().split():
        v[int(hashlib.md5(w.encode()).hexdigest(), 16) % DIM] += 1
    return v


ROWS = chunk("runbooks/gateway", DOC) + chunk("runbooks/disk", "# Disk\n\n## Delete\n\nNever delete raw. Old caches can go.")
INDEX = Index(ROWS, embed=(lambda texts: [fake_embed(t) for t in texts], fake_embed))


def test_bm25_finds_error_codes():
    # the default fts tokenizer drops digits; codes like 10197 must stay searchable
    assert INDEX.search("10197", k=1, mode="bm25")[0]["section"].endswith("C. No data")


def test_hybrid_fuses_both_rankings():
    top = INDEX.search("which caches can I delete", k=2, mode="hybrid")
    assert top[0]["doc"] == "runbooks/disk" and top[0]["bm25_rank"] and top[0]["vector_rank"]
    assert INDEX.search("which caches can I delete", k=1, mode="vector")[0]["bm25_rank"] is not None
