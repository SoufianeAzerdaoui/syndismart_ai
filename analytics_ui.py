# analytics_ui.py
from __future__ import annotations

import re
from datetime import datetime

import pandas as pd
import streamlit as st

# -------------------------
# Paths
# -------------------------
CSV_PATH = "cleanData/messages_final.csv"  # ou messages_validated.csv

# -------------------------
# Page config + light styling
# -------------------------
st.set_page_config(page_title="Analytics — Pré Power BI", layout="wide")

CSS = """
<style>
.block-container { padding-top: 1.0rem; max-width: 1600px; }
.kpi {
  border: 1px solid rgba(255,255,255,0.08);
  background: rgba(255,255,255,0.03);
  border-radius: 14px;
  padding: 14px 14px;
}
.kpi-title { opacity: 0.8; font-size: 12px; font-weight: 600; margin-bottom: 6px; }
.kpi-value { font-size: 28px; font-weight: 800; letter-spacing: -0.6px; }
.small { opacity: 0.85; font-size: 12px; }
hr { border: none; border-top: 1px solid rgba(255,255,255,0.08); margin: 12px 0; }
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)

st.title("📊 Analytics — Mini reporting (pré-Power BI)")
st.caption("KPIs + tendances. Filtrable par date, résidence, statut, urgence et catégorie.")

# -------------------------
# Load
# -------------------------
df = pd.read_csv(CSV_PATH)

def col_first(*cands: str) -> str | None:
    for c in cands:
        if c in df.columns:
            return c
    return None

# ---- compatibility columns
col_text = col_first("text_clean", "text", "message")
col_date = col_first("created_at", "date", "timestamp", "datetime", "received_at", "validated_at")
col_status = col_first("validator_status", "status")
col_cat = col_first("final_category", "category")
col_urg = col_first("final_urgency_level", "priority_rules", "urgency_level")
col_res = col_first("residence", "résidence", "residence_name", "residence_bloc", "bloc", "building", "site")

# If residence column missing, try to extract from text (best-effort)
def extract_residence_from_text(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return "UNKNOWN"
    # patterns: "Résidence X", "Residence X", "bloc A", "bloc B"
    m = re.search(r"(résidence|residence)\s*[:\-]?\s*([a-z0-9_ \-]{2,30})", s, flags=re.I)
    if m:
        name = m.group(2).strip()
        name = re.split(r"[,.;\n]", name)[0].strip()
        return name.upper()
    m2 = re.search(r"\bbloc\s*([a-z0-9]{1,4})\b", s, flags=re.I)
    if m2:
        return f"BLOC {m2.group(1).upper()}"
    return "UNKNOWN"

# Create working columns
work = df.copy()

# category / urgency / status
work["cat"] = work[col_cat].astype(str).fillna("other").str.strip() if col_cat else "other"
work.loc[work["cat"].eq(""), "cat"] = "other"

work["urg"] = work[col_urg].astype(str).fillna("P3").str.strip().str.upper() if col_urg else "P3"
work.loc[~work["urg"].isin(["P0", "P1", "P2", "P3"]), "urg"] = "P3"

work["status2"] = work[col_status].astype(str).fillna("TO_VALIDATE").str.strip().str.upper() if col_status else "TO_VALIDATE"
work.loc[work["status2"].eq(""), "status2"] = "TO_VALIDATE"

# date parsing
if col_date:
    work["dt"] = pd.to_datetime(work[col_date], errors="coerce", utc=False)
else:
    work["dt"] = pd.NaT

# fallback if dt all NaT -> try index as "today"
if work["dt"].isna().all():
    work["dt"] = pd.to_datetime(datetime.now().date())

work["day"] = work["dt"].dt.date

# residence
if col_res:
    work["res"] = work[col_res].astype(str).fillna("").str.strip()
    work.loc[work["res"].eq(""), "res"] = "UNKNOWN"
else:
    base_text = work[col_text].astype(str) if col_text else pd.Series([""] * len(work))
    work["res"] = base_text.apply(extract_residence_from_text)

# is_urgent
work["is_urgent"] = work["urg"].isin(["P0", "P1"]).astype(int)

# -------------------------
# Sidebar filters
# -------------------------
with st.sidebar:
    st.header("Filtres")

    # date range
    min_dt = pd.to_datetime(work["dt"].min()).date()
    max_dt = pd.to_datetime(work["dt"].max()).date()
    date_range = st.date_input("Période", value=(min_dt, max_dt), min_value=min_dt, max_value=max_dt)

    if isinstance(date_range, tuple) and len(date_range) == 2:
        d1, d2 = date_range
    else:
        d1, d2 = min_dt, max_dt

    status_opt = ["ALL"] + sorted(work["status2"].dropna().unique().tolist())
    urg_opt = ["ALL", "P0", "P1", "P2", "P3"]
    cat_opt = ["ALL"] + sorted(work["cat"].dropna().unique().tolist())
    res_opt = ["ALL"] + sorted(work["res"].dropna().unique().tolist())

    status_f = st.selectbox("Statut", status_opt, index=0)
    urg_f = st.selectbox("Urgence", urg_opt, index=0)
    cat_f = st.selectbox("Catégorie", cat_opt, index=0)
    res_f = st.selectbox("Résidence", res_opt, index=0)

# Apply filters
view = work.copy()
view = view[(view["day"] >= d1) & (view["day"] <= d2)]

if status_f != "ALL":
    view = view[view["status2"] == status_f]
if urg_f != "ALL":
    view = view[view["urg"] == urg_f]
if cat_f != "ALL":
    view = view[view["cat"] == cat_f]
if res_f != "ALL":
    view = view[view["res"] == res_f]

if len(view) == 0:
    st.info("Aucune donnée après filtres.")
    st.stop()

# -------------------------
# KPIs
# -------------------------
total = len(view)
urgent = int(view["is_urgent"].sum())
pct_urgent = (urgent / total * 100.0) if total else 0.0

k1, k2, k3, k4 = st.columns(4)
k1.markdown(f"<div class='kpi'><div class='kpi-title'>Messages</div><div class='kpi-value'>{total}</div></div>", unsafe_allow_html=True)
k2.markdown(f"<div class='kpi'><div class='kpi-title'>Urgents (P0/P1)</div><div class='kpi-value'>{urgent}</div><div class='small'>{pct_urgent:.1f}%</div></div>", unsafe_allow_html=True)
k3.markdown(f"<div class='kpi'><div class='kpi-title'>P0</div><div class='kpi-value'>{int((view['urg']=='P0').sum())}</div></div>", unsafe_allow_html=True)
k4.markdown(f"<div class='kpi'><div class='kpi-title'>P1</div><div class='kpi-value'>{int((view['urg']=='P1').sum())}</div></div>", unsafe_allow_html=True)

st.markdown("<hr/>", unsafe_allow_html=True)

# -------------------------
# Top categories + residence + daily
# -------------------------
c1, c2 = st.columns([1, 1], gap="large")

with c1:
    st.subheader("Top catégories")
    top_cat = (
        view["cat"]
        .value_counts()
        .rename_axis("category")
        .reset_index(name="count")
        .head(12)
    )
    st.bar_chart(top_cat.set_index("category")["count"])

    st.caption("Astuce: filtre catégorie/résidence à gauche pour analyser un périmètre précis.")

with c2:
    st.subheader("Messages par résidence")
    by_res = (
        view["res"]
        .value_counts()
        .rename_axis("residence")
        .reset_index(name="count")
        .head(15)
    )
    st.bar_chart(by_res.set_index("residence")["count"])

st.markdown("<hr/>", unsafe_allow_html=True)

st.subheader("Messages par jour")
daily = (
    view.groupby("day", as_index=False)
    .size()
    .rename(columns={"size": "count"})
    .sort_values("day")
)
daily["day"] = pd.to_datetime(daily["day"])
st.line_chart(daily.set_index("day")["count"])

st.markdown("<hr/>", unsafe_allow_html=True)

# -------------------------
# Optional: drilldown table
# -------------------------
st.subheader("Détails (drilldown)")
cols_show = []
for c in ["message_id", "day", "urg", "cat", "res", "status2"]:
    if c in view.columns:
        cols_show.append(c)

# add a text preview
if col_text:
    view["text_preview"] = view[col_text].astype(str).str.replace("\n", " ").str.slice(0, 140)
    cols_show.append("text_preview")

st.dataframe(
    view[cols_show].sort_values(["day"], ascending=False),
    use_container_width=True,
    hide_index=True,
)
