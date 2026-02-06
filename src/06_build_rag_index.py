# src/06_build_rag_index.py
from pathlib import Path
import re
import numpy as np
import faiss
from sentence_transformers import SentenceTransformer


HEADER_RE = re.compile(r"^(#{1,4})\s+(.+)$", re.MULTILINE)

def split_markdown_sections(md: str):
    """
    Coupe un Markdown en sections basées sur les headers.
    Retourne une liste de tuples: (section_title, section_text)
    """
    md = md.strip()
    if not md:
        return []

    matches = list(HEADER_RE.finditer(md))
    if not matches:
        return [("no_header", md)]

    sections = []
    for i, m in enumerate(matches):
        start = m.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(md)
        level = m.group(1)
        title = m.group(2).strip()
        block = md[start:end].strip()
        sections.append((f"{level} {title}", block))
    return sections


def chunk_text(text: str, chunk_size=800, overlap=120):
    """Chunking par caractères (simple, suffisant ici)."""
    text = text.strip()
    if not text:
        return []
    chunks = []
    start = 0
    n = len(text)
    while start < n:
        end = min(n, start + chunk_size)
        chunks.append(text[start:end].strip())
        if end == n:
            break
        start = max(0, end - overlap)
    return chunks


def main():
    base_dir = Path(__file__).resolve().parent.parent
    docs_dir = base_dir / "data" / "docs"
    out_dir = base_dir / "cleanData" / "rag"
    out_dir.mkdir(parents=True, exist_ok=True)

    doc_files = sorted([p for p in docs_dir.glob("**/*") if p.suffix.lower() in [".md", ".txt"]])
    if not doc_files:
        raise SystemExit(f"Aucun doc trouvé dans {docs_dir}. Lance d'abord 06_make_docs_from_policy.py")

    chunks = []
    sources = []

    for fp in doc_files:
        text = fp.read_text(encoding="utf-8", errors="ignore")
        rel = str(fp.relative_to(base_dir))

        # 1) split par sections markdown
        sections = split_markdown_sections(text)

        # 2) sous-chunking
        for sec_title, sec_text in sections:
            sub_chunks = chunk_text(sec_text, chunk_size=800, overlap=120)
            for j, ch in enumerate(sub_chunks):
                # on enrichit un peu le chunk pour retrieval
                enriched = f"SOURCE: {rel}\nSECTION: {sec_title}\n\n{ch}".strip()
                chunks.append(enriched)
                sources.append(f"{rel} | {sec_title} | chunk={j}")

    # Embeddings (E5)
    model_name = "intfloat/multilingual-e5-base"
    model = SentenceTransformer(model_name)

    passages = [f"passage: {c}" for c in chunks]
    emb = model.encode(passages, show_progress_bar=True, normalize_embeddings=True)
    emb = np.asarray(emb, dtype=np.float32)

    dim = emb.shape[1]
    index = faiss.IndexFlatIP(dim)
    index.add(emb)

    faiss.write_index(index, str(out_dir / "faiss.index"))
    (out_dir / "chunks.txt").write_text("\n---\n".join(chunks), encoding="utf-8")
    (out_dir / "sources.txt").write_text("\n".join(sources), encoding="utf-8")

    print("✅ Index FAISS créé:", out_dir / "faiss.index")
    print("✅ Nb chunks:", len(chunks))
    print("✅ Modèle embeddings:", model_name)


if __name__ == "__main__":
    main()
