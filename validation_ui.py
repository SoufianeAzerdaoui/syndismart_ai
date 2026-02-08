# validation_ui.py
from __future__ import annotations

import pandas as pd
import streamlit as st
from datetime import datetime

CSV_PATH = "cleanData/messages_final.csv"
OUT_PATH = "cleanData/messages_validated.csv"

st.set_page_config(page_title="Validation humaine", layout="wide")
st.title("✅ Validation humaine - réponses LLM")

# =========================
# Load + compat colonnes
# =========================
df = pd.read_csv(CSV_PATH)

# --- Compat: colonnes attendues par l'UI (si absentes ou vides) ---
if "final_category" not in df.columns:
    df["final_category"] = df.get("category", "other")
else:
    df["final_category"] = df["final_category"].fillna("").astype(str).str.strip()
    if "category" in df.columns:
        df.loc[df["final_category"].eq(""), "final_category"] = df["category"].fillna("other")
    df["final_category"] = df["final_category"].replace("", "other")

if "final_urgency_level" not in df.columns:
    df["final_urgency_level"] = df.get("priority_rules", "P3")
else:
    df["final_urgency_level"] = df["final_urgency_level"].fillna("").astype(str).str.strip()
    if "priority_rules" in df.columns:
        df.loc[df["final_urgency_level"].eq(""), "final_urgency_level"] = df["priority_rules"].fillna("P3")
    df["final_urgency_level"] = df["final_urgency_level"].replace("", "P3")

# --- status par défaut ---
if "status" not in df.columns:
    df["status"] = "TO_VALIDATE"
else:
    df["status"] = df["status"].fillna("TO_VALIDATE")
    df.loc[df["status"].astype(str).str.strip().eq(""), "status"] = "TO_VALIDATE"

# --- colonnes validation ---
for col in ["validator_status", "validated_by", "validated_at", "final_response", "validator_comment"]:
    if col not in df.columns:
        df[col] = ""
    else:
        df[col] = df[col].fillna("")

# --- initialiser final_response si vide: réponse LLM ---
df["final_response"] = df["final_response"].astype(str)
mask_empty_final = df["final_response"].str.strip().eq("")
df.loc[mask_empty_final, "final_response"] = df.get("response_draft", "").astype(str)

# =========================
# UI
# =========================
left, right = st.columns([1, 3])

with left:
    st.subheader("Filtres")

    status = st.selectbox("Statut", ["TO_VALIDATE", "APPROVED", "REJECTED", "DRAFT", "ALL"], index=0)
    cat = st.selectbox("Catégorie", ["ALL"] + sorted(df["final_category"].fillna("other").unique().tolist()))
    urg = st.selectbox("Urgence", ["ALL", "P0", "P1", "P2", "P3"])

    view = df.copy()

    if status != "ALL":
        # statut UI = validator_status si présent sinon status
        # (on garde aussi compat si quelqu'un ne remplit que status)
        m = (view["validator_status"].fillna("") == status) | (view["status"].fillna("") == status)
        # pour TO_VALIDATE, on inclut aussi les vides
        if status == "TO_VALIDATE":
            m = m | view["validator_status"].fillna("").eq("") | view["status"].fillna("").eq("")
        view = view[m]

    if cat != "ALL":
        view = view[view["final_category"].fillna("other") == cat]

    if urg != "ALL":
        view = view[view["final_urgency_level"].fillna("P3") == urg]

    st.write("Lignes:", len(view))
    if len(view) == 0:
        st.info("Aucune ligne ne correspond aux filtres.")
        st.stop()

    idx = st.number_input("Index (0..n-1)", min_value=0, max_value=max(0, len(view) - 1), value=0)

row = view.iloc[int(idx)]
row_id = row.get("message_id", "")

with right:
    st.subheader(f"Message: {row_id}")

    c1, c2 = st.columns(2)

    with c1:
        st.markdown("**Texte résident**")
        st.write(row.get("text_clean", ""))

        st.markdown("**Contexte RAG**")
        st.text_area("rag_context", value=str(row.get("rag_context", "")), height=180)

    with c2:
        st.markdown("**Décision**")
        st.write(
            {
                "final_urgency_level": row.get("final_urgency_level", ""),
                "final_category": row.get("final_category", ""),
                "assigned_to": row.get("assigned_to", ""),
                "sla_target_minutes": row.get("sla_target_minutes", ""),
                "decision_source": row.get("decision_source", ""),
            }
        )

        st.markdown("**Required info**")
        st.write(row.get("required_info", "[]"))

    st.divider()

    st.markdown("### Réponse (modifiable)")
    new_resp = st.text_area(
        "final_response",
        value=str(row.get("final_response", row.get("response_draft", ""))),
        height=140,
    )

    comment = st.text_input("Commentaire validateur", value=str(row.get("validator_comment", "")))
    name = st.text_input("Validé par", value=str(row.get("validated_by", "") or "human"))

    b1, b2, b3 = st.columns(3)

    def save(decision: str):
        df.loc[df["message_id"] == row_id, "validator_status"] = decision
        df.loc[df["message_id"] == row_id, "status"] = decision
        df.loc[df["message_id"] == row_id, "final_response"] = new_resp
        df.loc[df["message_id"] == row_id, "validator_comment"] = comment
        df.loc[df["message_id"] == row_id, "validated_by"] = name
        df.loc[df["message_id"] == row_id, "validated_at"] = datetime.now().isoformat(timespec="seconds")
        df.to_csv(OUT_PATH, index=False, encoding="utf-8")

    if b1.button("✅ APPROUVER"):
        save("APPROVED")
        st.success(f"Saved -> {OUT_PATH}")

    if b2.button("❌ REJETER"):
        save("REJECTED")
        st.warning(f"Saved -> {OUT_PATH}")

    if b3.button("💾 Sauver brouillon"):
        save("DRAFT")
        st.info(f"Saved -> {OUT_PATH}")

    st.caption(f"Fichier source: {CSV_PATH}  |  Export: {OUT_PATH}")
