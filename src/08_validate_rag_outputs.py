# src/08_validate_rag_outputs.py
from pathlib import Path
import json
import pandas as pd


FORCED_DOC_BY_LEVEL = {
    "P0": "data/docs/procedures_p0.md",
    "P1": "data/docs/procedures_p1.md",
    "P2": "data/docs/procedures_p2.md",
    "P3": "data/docs/procedures_p3.md",
}


def safe_load_json_list(s: str):
    """Retourne (ok, list). ok=False si JSON invalide."""
    try:
        if pd.isna(s) or str(s).strip() == "":
            return True, []
        obj = json.loads(s)
        if not isinstance(obj, list):
            return False, []
        return True, obj
    except Exception:
        return False, []


def normalize_level(row) -> str:
    """urgency_level sinon priority_rules sinon P3."""
    u = str(row.get("urgency_level", "") or "").strip()
    if u:
        return u
    pr = str(row.get("priority_rules", "") or "").strip()
    return pr if pr else "P3"


def source_startswith_any(sources: list[str], prefix: str) -> bool:
    return any(str(x).startswith(prefix) for x in sources)


def top_doc_prefix(source: str) -> str:
    """Extrait le chemin doc avant ' | ' si présent."""
    if not source:
        return ""
    s = str(source)
    return s.split(" | ", 1)[0].strip()


def main():
    base_dir = Path(__file__).resolve().parent.parent

    # INPUT
    in_path = base_dir / "cleanData" / "messages_with_context.csv"
    if not in_path.exists():
        raise SystemExit(f"Fichier introuvable: {in_path}")

    # OUTPUT audit
    audit_dir = base_dir / "cleanData" / "audit"
    audit_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(in_path)

    required_cols = ["rag_sources", "rag_scores", "rag_context"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise SystemExit(f"Colonnes manquantes dans {in_path.name}: {missing}")

    # --- validations row-by-row
    rows = []
    for idx, row in df.iterrows():
        level = normalize_level(row)
        expected_doc = FORCED_DOC_BY_LEVEL.get(level, FORCED_DOC_BY_LEVEL["P3"])

        ok_sources, sources = safe_load_json_list(row.get("rag_sources", ""))
        ok_scores, scores = safe_load_json_list(row.get("rag_scores", ""))

        context = "" if pd.isna(row.get("rag_context", "")) else str(row.get("rag_context", ""))
        context_empty = (context.strip() == "")

        lengths_match = (len(sources) == len(scores))
        topk = len(sources)

        # checks
        json_ok = ok_sources and ok_scores
        topk_ok = topk >= 1
        has_expected_proc = source_startswith_any(sources, expected_doc)

        # derive some helpful fields
        top1 = sources[0] if sources else ""
        top1_doc = top_doc_prefix(top1)

        rows.append(
            {
                "row_index": idx,
                "message_id": row.get("message_id", ""),
                "residence_id": row.get("residence_id", ""),
                "level": level,
                "expected_proc_doc": expected_doc,
                "json_ok": int(json_ok),
                "sources_json_ok": int(ok_sources),
                "scores_json_ok": int(ok_scores),
                "lengths_match": int(lengths_match),
                "topk": topk,
                "topk_ok": int(topk_ok),
                "context_empty": int(context_empty),
                "has_expected_proc": int(has_expected_proc),
                "top1_doc": top1_doc,
            }
        )

    audit = pd.DataFrame(rows)

    # --- global checklist
    n = len(audit)
    def pct(x): 
        return 0.0 if n == 0 else (100.0 * x / n)

    json_ok_n = int(audit["json_ok"].sum())
    lengths_ok_n = int(audit["lengths_match"].sum())
    context_ok_n = int((1 - audit["context_empty"]).sum())
    proc_ok_n = int(audit["has_expected_proc"].sum())
    topk_ok_n = int(audit["topk_ok"].sum())

    print("\n=====================")
    print("✅ RAG OUTPUTS VALIDATION CHECKLIST")
    print("=====================")
    print(f"Rows total: {n}")
    print(f"JSON OK (sources+scores): {json_ok_n}/{n} ({pct(json_ok_n):.1f}%)")
    print(f"Lengths match (sources==scores): {lengths_ok_n}/{n} ({pct(lengths_ok_n):.1f}%)")
    print(f"Context non-empty: {context_ok_n}/{n} ({pct(context_ok_n):.1f}%)")
    print(f"Top-k ok (>=1): {topk_ok_n}/{n} ({pct(topk_ok_n):.1f}%)")
    print(f"Expected procedure doc present: {proc_ok_n}/{n} ({pct(proc_ok_n):.1f}%)")

    # --- per-level stats
    print("\n=====================")
    print("📌 Par niveau (level)")
    print("=====================")
    by_level = audit.groupby("level").agg(
        rows=("row_index", "count"),
        proc_ok=("has_expected_proc", "sum"),
        ctx_ok=("context_empty", lambda s: int((s == 0).sum())),
        json_ok=("json_ok", "sum"),
    ).reset_index()
    by_level["proc_ok_%"] = by_level.apply(lambda r: pct(r["proc_ok"]) if r["rows"] else 0.0, axis=1)
    by_level["ctx_ok_%"] = by_level.apply(lambda r: pct(r["ctx_ok"]) if r["rows"] else 0.0, axis=1)
    by_level["json_ok_%"] = by_level.apply(lambda r: pct(r["json_ok"]) if r["rows"] else 0.0, axis=1)
    print(by_level.sort_values("level").to_string(index=False))

    # --- Top-1 doc distribution (by level)
    print("\n=====================")
    print("📌 Top-1 doc (par niveau)")
    print("=====================")
    top1 = (
        audit.groupby(["level", "top1_doc"])
        .size()
        .reset_index(name="count")
        .sort_values(["level", "count"], ascending=[True, False])
    )
    # print only top 5 per level
    for lvl in sorted(top1["level"].unique()):
        sub = top1[top1["level"] == lvl].head(5)
        print(f"\nLevel {lvl}:")
        print(sub.to_string(index=False))

    # --- export errors
    errors = audit[
        (audit["json_ok"] == 0)
        | (audit["lengths_match"] == 0)
        | (audit["context_empty"] == 1)
        | (audit["has_expected_proc"] == 0)
        | (audit["topk_ok"] == 0)
    ].copy()

    audit_path = audit_dir / "rag_validation_report.csv"
    audit.to_csv(audit_path, index=False, encoding="utf-8")

    if len(errors) > 0:
        errors_path = audit_dir / "rag_validation_errors.csv"
        errors.to_csv(errors_path, index=False, encoding="utf-8")
        print("\n❌ Errors found!")
        print(f"- Report: {audit_path}")
        print(f"- Errors: {errors_path}")
        print("\nAperçu erreurs (5 lignes):")
        print(errors.head(5).to_string(index=False))
    else:
        print("\n✅ Aucun problème détecté (selon les checks).")
        print(f"- Report: {audit_path}")

    print("\n✅ Audit exports dans:", audit_dir)


if __name__ == "__main__":
    main()
