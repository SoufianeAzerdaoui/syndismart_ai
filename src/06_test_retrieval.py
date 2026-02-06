# src/06_test_retrieval.py
from pathlib import Path
import numpy as np
import faiss
from sentence_transformers import SentenceTransformer


def safe_top_k(requested_k: int, nb_chunks: int) -> int:
    """Empêche k > nb_chunks (sinon FAISS renvoie des résultats invalides)."""
    if nb_chunks <= 0:
        return 0
    return max(1, min(int(requested_k), int(nb_chunks)))


def rewrite_query(raw_query: str, urgency_level: str | None = None, category: str | None = None) -> str:
    """
    Query rewrite simple:
    - préfixe selon niveau/catégorie
    - format E5 recommandé: 'query: ...'
    """
    q = (raw_query or "").strip()
    if not q:
        return q

    prefix = []
    if urgency_level == "P0":
        prefix.append("urgence critique P0")
    elif urgency_level == "P1":
        prefix.append("urgence P1")
    elif urgency_level == "P2":
        prefix.append("non urgent P2")
    elif urgency_level == "P3":
        prefix.append("administratif P3")

    if category:
        prefix.append(f"categorie {category}")

    return "query: " + " ".join(prefix + [q])


def main():
    base_dir = Path(__file__).resolve().parent.parent
    rag_dir = base_dir / "cleanData" / "rag"

    index = faiss.read_index(str(rag_dir / "faiss.index"))
    chunks = (rag_dir / "chunks.txt").read_text(encoding="utf-8").split("\n---\n")
    sources = (rag_dir / "sources.txt").read_text(encoding="utf-8").splitlines()

    assert len(chunks) == len(sources), "chunks.txt et sources.txt n'ont pas la même taille"

    model = SentenceTransformer("intfloat/multilingual-e5-base")

    raw_query = "ascenseur bloqué personne à l'intérieur procédure"
    query = rewrite_query(raw_query, urgency_level="P0", category="elevator")

    q_emb = model.encode([query], normalize_embeddings=True).astype(np.float32)

    k = safe_top_k(5, index.ntotal)
    scores, idxs = index.search(q_emb, k)

    print("\nRAW QUERY:", raw_query)
    print("REWRITTEN:", query)
    print(f"Index ntotal={index.ntotal} | top-k={k}")
    print("\nTop résultats:\n")

    for rank, (score, i) in enumerate(zip(scores[0], idxs[0]), 1):
        if i < 0 or i >= len(chunks):
            continue
        print(f"[{rank}] score={float(score):.4f} source={sources[i]}")
        print(chunks[i][:350].replace("\n", " "))
        print("-" * 80)


if __name__ == "__main__":
    main()
