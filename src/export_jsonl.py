import json
import pandas as pd
from pathlib import Path
from datetime import datetime


# -------------------------
# Helpers
# -------------------------
def to_iso(dt_val) -> str:
    """
    Convert datetime value to ISO 'YYYY-MM-DDTHH:MM:SS' (no timezone).
    Accepts 'YYYY-MM-DD HH:MM:SS' strings too.
    """
    if pd.isna(dt_val):
        return ""
    s = str(dt_val).strip()
    # common formats in your CSV
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            pass
    # fallback: keep original, best effort
    return s.replace(" ", "T")


def safe_json_load(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return {"matched_level": "none", "matched_keywords": [], "matched_rules": []}
    if isinstance(x, dict):
        return x
    try:
        return json.loads(x)
    except Exception:
        return {"matched_level": "none", "matched_keywords": [], "matched_rules": []}


def infer_category(text_clean: str, urgency_level: str, policy_trace: dict) -> str:
    """
    Map to one of:
    fire|gas|flood|elevator|security|medical|water|electricity|garage|leak|noise|cleaning|parking|admin|other
    """
    t = (text_clean or "").lower()
    rules = set((policy_trace or {}).get("matched_rules", []) or [])
    kws = " ".join((policy_trace or {}).get("matched_keywords", []) or []).lower()

    # From guardrail rules (most reliable)
    if "P0_GAS" in rules:
        return "gas"
    if "P0_FIRE" in rules:
        return "fire"
    if "P0_VIOLENCE" in rules:
        return "security"
    if "P0_MEDICAL" in rules:
        return "medical"
    if "P0_ELEVATOR_WITH_PERSON" in rules:
        return "elevator"
    if "P0_FLOOD_MAJOR" in rules:
        return "flood"

    # Keyword-based heuristics
    if "gaz" in t or "lghaz" in t or "lgaz" in t:
        return "gas"
    if "incendie" in t or "fum" in t or "flamme" in t or "dokhan" in t or "afia" in t or "3afia" in t:
        return "fire"
    if "inondation" in t or "fayadan" in t or "gher9" in t or "gherk" in t or "flood" in t:
        return "flood"
    if "ascenseur" in t or "asansour" in t:
        return "elevator"
    if "agression" in t or "bagarre" in t or "violence" in t or "menace" in t or "mdabza" in t:
        return "security"
    if "respiration" in t or "cardiaque" in t or "inconscient" in t:
        return "medical"

    if "panne d’eau" in t or "panne eau" in t or "coupure eau" in t or "lma" in t:
        return "water"
    if "électri" in t or "electric" in t or "coupure" in t or "escaliers" in t or "noir" in t or "daw" in t or "dow" in t:
        return "electricity"
    if "garage" in t or "garaj" in t or "porte du garage" in t:
        return "garage"
    if "fuite" in t or "ça coule" in t or "coule" in t:
        return "leak"
    if "bruit" in t or "nuisance" in t or "voisin" in t or "sda3" in t:
        return "noise"
    if "propreté" in t or "sale" in t or "poubelle" in t or "zbel" in t or "mwsekh" in t:
        return "cleaning"
    if "stationnement" in t or "garé" in t or "gare " in t or "place" in t:
        return "parking"
    if "attestation" in t or "charges" in t or "quittance" in t or "facture" in t or "règlement" in t or "war9a" in t or "khlass" in t:
        return "admin"

    return "other"


def required_info_for(level: str, category: str):
    """
    Minimal required fields to collect depending on type.
    """
    base = ["residence_id", "localisation"]
    if level == "P0":
        if category in {"gas", "fire"}:
            return base + ["bloc", "étage", "danger immédiat (oui/non)"]
        if category == "elevator":
            return base + ["bloc", "étage", "personne bloquée (oui/non)", "numéro ascenseur (si connu)"]
        if category == "flood":
            return base + ["bloc", "étage", "source eau (si connue)", "près tableau électrique (oui/non)"]
        if category in {"security", "medical"}:
            return base + ["bloc", "étage", "besoin autorités (oui/non)"]
        return base + ["bloc", "étage"]
    if level == "P1":
        return base + ["bloc", "depuis quand", "photo/vidéo (si possible)"]
    if level == "P2":
        if category == "noise":
            return base + ["heure/jour", "fréquence", "bloc/étage"]
        return base + ["photo (si possible)"]
    # P3
    return ["residence_id", "numéro appartement", "référence (si disponible)"]


# -------------------------
# Main export
# -------------------------
def main():
    base_dir = Path(__file__).resolve().parent.parent
    input_path = base_dir / "cleanData" / "messages_triage.csv"
    output_path = base_dir / "cleanData" / "messages_final.jsonl"

    df = pd.read_csv(input_path)

    # Ensure minimal columns exist
    for col in ["message_id", "datetime", "residence_id", "text_clean", "language", "urgency_level", "is_urgent", "decision_source", "sla_target_minutes", "response_draft", "assigned_to"]:
        if col not in df.columns:
            df[col] = ""

    # Optional columns
    if "channel" not in df.columns:
        df["channel"] = "whatsapp"
    if "has_media" not in df.columns:
        df["has_media"] = 0
    if "media_type" not in df.columns:
        df["media_type"] = "none"
    if "text" not in df.columns:
        df["text"] = df["text_clean"]

    # Parse policy_trace
    if "policy_trace" in df.columns:
        df["_policy_trace_obj"] = df["policy_trace"].apply(safe_json_load)
    else:
        df["_policy_trace_obj"] = [{"matched_level": "none", "matched_keywords": [], "matched_rules": []}] * len(df)

    # Build JSONL
    with open(output_path, "w", encoding="utf-8") as f:
        for _, row in df.iterrows():
            policy_trace = row["_policy_trace_obj"]
            level = str(row["urgency_level"]) if pd.notna(row["urgency_level"]) else "P3"
            level = level if level in {"P0", "P1", "P2", "P3"} else "P3"

            category = infer_category(str(row["text_clean"]), level, policy_trace)

            record = {
                "message_id": str(row["message_id"]),
                "datetime": to_iso(row["datetime"]),
                "residence_id": str(row["residence_id"]),
                "channel": str(row.get("channel", "whatsapp")),

                "text": str(row.get("text", "")),
                "text_clean": str(row.get("text_clean", "")),

                "language": str(row.get("language", "unknown")),

                "has_media": int(row.get("has_media", 0)) if str(row.get("has_media", "0")).strip() != "" else 0,
                "media_type": str(row.get("media_type", "none")),

                "urgency_level": level,
                "is_urgent": int(row.get("is_urgent", 1 if level in {"P0","P1"} else 0)),

                "category": category,

                "decision_source": str(row.get("decision_source", "RULE")),

                "policy_trace": {
                    "matched_level": policy_trace.get("matched_level", "none"),
                    "matched_keywords": policy_trace.get("matched_keywords", []) or [],
                    "matched_rules": policy_trace.get("matched_rules", []) or []
                },

                "sla_target_minutes": int(row.get("sla_target_minutes", 0)) if str(row.get("sla_target_minutes", "")).strip() != "" else 0,

                "response_draft": str(row.get("response_draft", "")),
                "required_info": required_info_for(level, category),

                "status": "TO_VALIDATE",
                "assigned_to": str(row.get("assigned_to", "UNKNOWN")),
            }

            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    print(f"✅ JSONL generated: {output_path}")


if __name__ == "__main__":
    main()
