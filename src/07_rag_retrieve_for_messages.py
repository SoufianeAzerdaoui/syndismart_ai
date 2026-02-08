# src/07_rag_retrieve_for_messages.py
from pathlib import Path
import json
import numpy as np
import pandas as pd
import faiss
from sentence_transformers import SentenceTransformer


MODEL_NAME = "intfloat/multilingual-e5-base"
REQUESTED_TOP_K = 5

# Docs "procédures" à forcer selon niveau
FORCED_DOC_BY_LEVEL = {
    "P0": "data/docs/procedures_p0.md",
    "P1": "data/docs/procedures_p1.md",
    "P2": "data/docs/procedures_p2.md",
    "P3": "data/docs/procedures_p3.md",
}
FORCED_DOC_BY_CATEGORY = {
    "admin": "data/docs/charges_et_quittances.md",
    "other": "data/docs/reservation_salle_polyvalente.md",
    "electricity": "data/docs/electricite_etincelles.md",
    "elevator": "data/docs/ascenseur_panne.md",
}
FORCED_DOC_BY_CATEGORY = {
    "admin": "data/docs/charges_et_quittances.md",
    "reservation": "data/docs/reservation_salle_polyvalente.md",
    "electricity": "data/docs/electricite_etincelles.md",
    "elevator": "data/docs/ascenseur_panne.md",
}


def safe_top_k(requested_k: int, nb_chunks: int) -> int:
    """Empêche k > nb_chunks (sinon FAISS renvoie des résultats invalides)."""
    if nb_chunks <= 0:
        return 0
    return max(1, min(int(requested_k), int(nb_chunks)))


def rewrite_query_from_row(text: str, urgency_level: str, category: str) -> str:
    """
    Query rewrite simple:
    - préfixe par niveau + catégorie pour guider l'embedding
    - format E5 recommandé: "query: ..."
    """
    q = (text or "").strip()
    if not q:
        return q

    prefix = []
    if urgency_level == "P0":
        prefix.append("urgence critique P0")
    elif urgency_level == "P1":
        prefix.append("urgence P1")
    elif urgency_level == "P2":
        prefix.append("non urgent P2")
    else:
        prefix.append("administratif P3")

    if category:
        prefix.append(f"procedure {category}")

    return "query: " + " ".join(prefix + [q])


def force_doc_in_results(
    picked_sources: list[str],
    picked_scores: list[float],
    target_doc_prefix: str,
    source_to_chunk: dict[str, str],
    boost_score: float = 1.0,
) -> tuple[list[str], list[float]]:
    """
    Force un document à apparaître dans les résultats.
    - target_doc_prefix = "data/docs/procedures_p0.md" (préfixe)
    - Les sources peuvent être "data/docs/procedures_p0.md | ### ..." -> on matche par préfixe.
    """
    if not target_doc_prefix:
        return picked_sources, picked_scores

    # Déjà présent ? (match par préfixe)
    if any(s.startswith(target_doc_prefix) for s in picked_sources):
        return picked_sources, picked_scores

    # Trouver une source réelle dans l'index qui correspond à ce doc
    candidates = [s for s in source_to_chunk.keys() if s.startswith(target_doc_prefix)]
    if not candidates:
        return picked_sources, picked_scores

    # On prend le premier chunk du doc (ou celui que tu préfères)
    forced_source = candidates[0]

    if not picked_sources:
        return [forced_source], [boost_score]

    picked_sources = list(picked_sources)
    picked_scores = list(picked_scores)

    # Remplacer le dernier résultat (le moins bon) par le doc forcé
    picked_sources[-1] = forced_source
    picked_scores[-1] = boost_score
    return picked_sources, picked_scores


def build_context_text(picked_sources: list[str], source_to_chunk: dict[str, str], sep: str = "\n\n---\n\n") -> str:
    parts = []
    for s in picked_sources:
        chunk = source_to_chunk.get(s, "")
        if chunk:
            parts.append(chunk)
    return sep.join(parts).strip()


def main():
    base_dir = Path(__file__).resolve().parent.parent

    # INPUT
    messages_path = base_dir / "cleanData" / "messages_rules.csv"

    # RAG store
    rag_dir = base_dir / "cleanData" / "rag"
    index_path = rag_dir / "faiss.index"
    chunks_path = rag_dir / "chunks.txt"
    sources_path = rag_dir / "sources.txt"

    # OUTPUT
    out_path = base_dir / "cleanData" / "messages_with_context.csv"

    # --- load messages
    df = pd.read_csv(messages_path)
    if "text_clean" not in df.columns:
        raise ValueError("messages_rules.csv doit contenir la colonne text_clean")

    # --- load rag store
    index = faiss.read_index(str(index_path))
    chunks = chunks_path.read_text(encoding="utf-8").split("\n---\n")
    sources = sources_path.read_text(encoding="utf-8").splitlines()

    if len(chunks) != len(sources):
        raise ValueError("chunks.txt et sources.txt n'ont pas la même taille")

    source_to_chunk = {s: c for s, c in zip(sources, chunks)}

    # --- embedding model
    model = SentenceTransformer(MODEL_NAME)

    # --- retrieval params
    top_k = safe_top_k(REQUESTED_TOP_K, index.ntotal)

    rag_sources_col = []
    rag_scores_col = []
    rag_context_col = []

    for _, row in df.iterrows():
        text = str(row.get("text_clean", "") or "").strip()

        # ✅ fallback: priority_rules si urgency_level absent
        urgency = str(row.get("urgency_level", "") or "").strip()
        if not urgency:
            urgency = str(row.get("priority_rules", "") or "P3").strip()

        category = str(row.get("category", "") or "").strip()

        query = rewrite_query_from_row(text, urgency_level=urgency, category=category)
        q_emb = model.encode([query], normalize_embeddings=True).astype(np.float32)

        scores, idxs = index.search(q_emb, top_k)

        picked_sources = []
        picked_scores = []

        for score, i in zip(scores[0], idxs[0]):
            if i < 0 or i >= len(chunks):
                continue
            picked_sources.append(sources[i])
            picked_scores.append(float(score))

        # ✅ Forcer le doc de procédures du niveau (P0/P1/P2/P3)
        # 1) Forcer doc par NIVEAU
        forced_level_doc = FORCED_DOC_BY_LEVEL.get(urgency)
        picked_sources, picked_scores = force_doc_in_results(
            picked_sources,
            picked_scores,
            target_doc_prefix=forced_level_doc,
            source_to_chunk=source_to_chunk,
            boost_score=1.2,
        )

        # 2) Forcer doc par CATÉGORIE
        forced_cat_doc = FORCED_DOC_BY_CATEGORY.get(category)
        picked_sources, picked_scores = force_doc_in_results(
            picked_sources,
            picked_scores,
            target_doc_prefix=forced_cat_doc,
            source_to_chunk=source_to_chunk,
            boost_score=1.3,
        )

        context = build_context_text(picked_sources, source_to_chunk)

        rag_sources_col.append(json.dumps(picked_sources, ensure_ascii=False))
        rag_scores_col.append(json.dumps(picked_scores, ensure_ascii=False))
        rag_context_col.append(context)

    df["rag_sources"] = rag_sources_col
    df["rag_scores"] = rag_scores_col
    df["rag_context"] = rag_context_col

    df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"✅ Saved: {out_path}")
    print(f"Index ntotal={index.ntotal} | top_k={top_k}")
    print("Colonnes ajoutées: rag_sources, rag_scores, rag_context")


if __name__ == "__main__":
    main()
