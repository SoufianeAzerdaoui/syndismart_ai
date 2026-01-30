import json
import re
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Tuple

# =====================================================
# Utils
# =====================================================
def norm_text(s: str) -> str:
    if pd.isna(s):
        return ""
    s = str(s).lower()
    s = re.sub(r"\s+", " ", s)
    return s.strip()


# =====================================================
# P0 GUARDRAILS (CRITIQUES – NE PAS RATER)
# =====================================================
P0_GAS = re.compile(r"(gaz|odeur de gaz|fuite gaz|ri7a dyal gaz|ri7t lgaz)", re.I)
P0_FIRE = re.compile(r"(incendie|fumée|fumee|flammes|dokhan|dkhan|afia|3afia)", re.I)
P0_VIOLENCE = re.compile(r"(agression|bagarre|menace|violence|mdabza)", re.I)

ELEVATOR_WORD = re.compile(r"(ascenseur|asansour)", re.I)
ELEVATOR_BLOCK = re.compile(r"(bloqué|bloquee|coincé|coince|m7bouss|habs)", re.I)
ELEVATOR_PERSON = re.compile(
    r"(personne|quelqu'un|résident|resident|à l'intérieur|a l interieur|dedans|coincée|coincee|dakhel)",
    re.I
)

FLOOD_WORD = re.compile(r"(inondation|fayadan|gher9at|gherkat)", re.I)
FLOOD_MAJOR = re.compile(r"(majeure|partout|monte vite|eau monte|risque électrique)", re.I)

SPARKS = re.compile(r"(étincelles|chertat|spark)", re.I)


def p0_guardrail(text: str) -> List[str]:
    rules = []

    if P0_GAS.search(text):
        rules.append("P0_GAS")
    if P0_FIRE.search(text):
        rules.append("P0_FIRE")
    if P0_VIOLENCE.search(text):
        rules.append("P0_VIOLENCE")

    if (
        ELEVATOR_WORD.search(text)
        and ELEVATOR_BLOCK.search(text)
        and ELEVATOR_PERSON.search(text)
    ):
        rules.append("P0_ELEVATOR_WITH_PERSON_STRICT")

    if FLOOD_WORD.search(text) and FLOOD_MAJOR.search(text):
        rules.append("P0_FLOOD_MAJOR")

    return rules


# =====================================================
# Classification
# =====================================================
def classify_message(text_clean: str) -> Dict[str, Any]:
    text = norm_text(text_clean)

    # ---------- P0 ----------
    p0_rules = p0_guardrail(text)
    if p0_rules:
        return {
            "urgency_level": "P0",
            "is_urgent": 1,
            "category": "critical",
            "decision_source": "RULE",
            "policy_trace": {
                "matched_level": "P0",
                "matched_keywords": [],
                "matched_rules": p0_rules,
            },
            "sla_target_minutes": 5,
            "assigned_to": "PRESTATAIRE",
        }

    # ---------- P1 ----------
    if "panne d'eau" in text or "ma kaynch lma" in text:
        return p1("water", "P1_WATER_RULE")

    if "étincelles" in text or "spark" in text:
        return p1("electricity", "P1_ELECTRIC_SPARKS")

    if "ascenseur en panne" in text:
        return p1("elevator", "P1_ELEVATOR_OUT", ["ascenseur en panne"])

    # ---------- P2 ----------
    if "bruit" in text or "sda3" in text:
        return p2("noise", ["bruit"])

    if "propreté" in text or "poubelles" in text:
        return p2("cleaning", ["propreté"])

    # ---------- P3 ----------
    return {
        "urgency_level": "P3",
        "is_urgent": 0,
        "category": "other",
        "decision_source": "RULE",
        "policy_trace": {
            "matched_level": "P3",
            "matched_keywords": [],
            "matched_rules": ["NO_MATCH_DEFAULT_P3"],
        },
        "sla_target_minutes": 1440,
        "assigned_to": "SYNDIC",
    }


def p1(category: str, rule: str, keywords: List[str] = None):
    return {
        "urgency_level": "P1",
        "is_urgent": 1,
        "category": category,
        "decision_source": "RULE",
        "policy_trace": {
            "matched_level": "P1",
            "matched_keywords": keywords or [],
            "matched_rules": [rule],
        },
        "sla_target_minutes": 30,
        "assigned_to": "PRESTATAIRE",
    }


def p2(category: str, keywords: List[str]):
    return {
        "urgency_level": "P2",
        "is_urgent": 0,
        "category": category,
        "decision_source": "RULE",
        "policy_trace": {
            "matched_level": "P2",
            "matched_keywords": keywords,
            "matched_rules": [],
        },
        "sla_target_minutes": 240,
        "assigned_to": "SYNDIC",
    }


# =====================================================
# MAIN – CSV → CSV
# =====================================================

def main():
    base_dir = Path(__file__).resolve().parent.parent

    input_csv = base_dir / "cleanData" / "messages_final.csv"
    output_csv = base_dir / "cleanData" / "messages_triage.csv"

    df = pd.read_csv(input_csv)

    if "text_clean" not in df.columns:
        raise ValueError("❌ colonne 'text_clean' manquante")

    results = df["text_clean"].apply(classify_message)
    results_df = pd.DataFrame(list(results))  # IMPORTANT: ne pas aplatir

    df_out = pd.concat([df, results_df], axis=1)

    if "policy_trace" in df_out.columns:
        df_out["policy_trace"] = df_out["policy_trace"].apply(
            lambda x: json.dumps(x, ensure_ascii=False)
        )

    df_out.to_csv(output_csv, index=False, encoding="utf-8")
    print(f"✅ CSV généré : {output_csv}")
    print(df_out["urgency_level"].value_counts())


if __name__ == "__main__":
    main()
