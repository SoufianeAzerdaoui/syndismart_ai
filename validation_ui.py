import json
import time
from datetime import datetime
import pandas as pd
import streamlit as st

CSV_PATH = "cleanData/messages_final.csv"
OUT_PATH = "cleanData/messages_validated.csv"

st.set_page_config(page_title="Validation humaine", layout="wide")
st.title("✅ Validation humaine - réponses LLM")

df = pd.read_csv(CSV_PATH)

# colonnes validation
for col in ["validator_status","validated_by","validated_at","final_response","validator_comment"]:
    if col not in df.columns:
        df[col] = ""

left, right = st.columns([1,3])

with left:
    st.subheader("Filtres")
    status = st.selectbox("Statut", ["TO_VALIDATE","APPROVED","REJECTED","ALL"], index=0)
    cat = st.selectbox("Catégorie", ["ALL"] + sorted(df["final_category"].fillna("other").unique().tolist()))
    urg = st.selectbox("Urgence", ["ALL","P0","P1","P2","P3"])

    view = df.copy()
    if status != "ALL":
        view = view[view["status"].fillna("TO_VALIDATE") == status]
    if cat != "ALL":
        view = view[view["final_category"].fillna("other") == cat]
    if urg != "ALL":
        view = view[view["final_urgency_level"].fillna("P3") == urg]

    st.write("Lignes:", len(view))
    idx = st.number_input("Index (0..n-1)", min_value=0, max_value=max(0, len(view)-1), value=0)
    if len(view) == 0:
        st.stop()

row = view.iloc[int(idx)]
row_id = row["message_id"]

with right:
    st.subheader(f"Message: {row_id}")
    c1, c2 = st.columns(2)

    with c1:
        st.markdown("**Texte résident**")
        st.write(row.get("text", ""))

        st.markdown("**Contexte RAG**")
        st.text_area("rag_context", value=str(row.get("rag_context","")), height=180)

    with c2:
        st.markdown("**Décision**")
        st.write({
            "final_urgency_level": row.get("final_urgency_level",""),
            "final_category": row.get("final_category",""),
            "assigned_to": row.get("assigned_to",""),
            "sla_target_minutes": row.get("sla_target_minutes",""),
            "decision_source": row.get("decision_source",""),
        })

        st.markdown("**Required info**")
        st.write(row.get("required_info","[]"))

    st.divider()
    st.markdown("### Réponse (modifiable)")
    new_resp = st.text_area("final_response", value=str(row.get("response_draft","")), height=120)

    comment = st.text_input("Commentaire validateur", value=str(row.get("validator_comment","")))

    name = st.text_input("Validé par", value=str(row.get("validated_by","") or "human"))

    b1, b2, b3 = st.columns(3)
    if b1.button("✅ APPROUVER"):
        df.loc[df["message_id"]==row_id, "validator_status"] = "APPROVED"
        df.loc[df["message_id"]==row_id, "status"] = "APPROVED"
        df.loc[df["message_id"]==row_id, "final_response"] = new_resp
        df.loc[df["message_id"]==row_id, "validator_comment"] = comment
        df.loc[df["message_id"]==row_id, "validated_by"] = name
        df.loc[df["message_id"]==row_id, "validated_at"] = datetime.now().isoformat(timespec="seconds")
        df.to_csv(OUT_PATH, index=False, encoding="utf-8")
        st.success(f"Saved -> {OUT_PATH}")

    if b2.button("❌ REJETER"):
        df.loc[df["message_id"]==row_id, "validator_status"] = "REJECTED"
        df.loc[df["message_id"]==row_id, "status"] = "REJECTED"
        df.loc[df["message_id"]==row_id, "final_response"] = new_resp
        df.loc[df["message_id"]==row_id, "validator_comment"] = comment
        df.loc[df["message_id"]==row_id, "validated_by"] = name
        df.loc[df["message_id"]==row_id, "validated_at"] = datetime.now().isoformat(timespec="seconds")
        df.to_csv(OUT_PATH, index=False, encoding="utf-8")
        st.warning(f"Saved -> {OUT_PATH}")

    if b3.button("💾 Sauver brouillon"):
        df.loc[df["message_id"]==row_id, "validator_status"] = "DRAFT"
        df.loc[df["message_id"]==row_id, "final_response"] = new_resp
        df.loc[df["message_id"]==row_id, "validator_comment"] = comment
        df.loc[df["message_id"]==row_id, "validated_by"] = name
        df.loc[df["message_id"]==row_id, "validated_at"] = datetime.now().isoformat(timespec="seconds")
        df.to_csv(OUT_PATH, index=False, encoding="utf-8")
        st.info(f"Saved -> {OUT_PATH}")
