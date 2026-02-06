# 03_rules_baseline.py
# Baseline rules for:
# - priority (P0/P1/P2/P3) using policy_config.json
# - category detection (rule-based) for syndic WhatsApp messages
#
# PATCH v2:
# ✅ Fix false-positives from substring matching (ex: "tableau" contains "eau")
# ✅ Uses word-boundary regex for single words
# ✅ Keeps substring matching ONLY for true multi-word phrases
#
# Input : cleanData/messages_processed.csv (must contain text_clean)
# Output: cleanData/messages_rules.csv (adds priority_rules, rule_match, is_urgent_rules, category, category_match)

import json
import pandas as pd
from pathlib import Path
import unicodedata
import re
from typing import Dict, List, Tuple, Optional

_space_re = re.compile(r"\s+")
_non_word_re = re.compile(r"[^a-z0-9\s'-]")  # keep basic useful chars

# -------------------------
# TEXT NORMALIZATION
# -------------------------
def normalize(s: str) -> str:
    s = (s or "").lower().strip()

    # unify apostrophes
    s = s.replace("’", "'").replace("`", "'").replace("´", "'")

    # unicode normalize + remove accents
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))

    # remove weird punctuation (keep letters, digits, space, ' and -)
    s = _non_word_re.sub(" ", s)

    # collapse spaces
    s = _space_re.sub(" ", s).strip()
    return s


# -------------------------
# SAFE MATCHING (word boundary)
# -------------------------
_WORD_CHARS = r"a-z0-9"  # after normalize(), only these matter for boundaries

def compile_term(term_norm: str) -> re.Pattern:
    """
    Compile a regex that matches a term safely:
    - If term contains spaces or apostrophes/hyphens => treat as phrase and match as substring with spacing normalization
    - Else match as whole word using boundaries, avoiding cases like "tableau" containing "eau"
    """
    term_norm = (term_norm or "").strip()
    if not term_norm:
        # never matches
        return re.compile(r"(?!x)x")

    # phrase: contains space
    if " " in term_norm:
        # collapse multiple spaces in term to \s+
        parts = [re.escape(p) for p in term_norm.split() if p]
        pat = r"\b" + r"\s+".join(parts) + r"\b"
        return re.compile(pat)

    # single token: strict word boundary (no letters/digits before/after)
    # e.g. (?<![a-z0-9])eau(?![a-z0-9])
    pat = rf"(?<![{_WORD_CHARS}]){re.escape(term_norm)}(?![{_WORD_CHARS}])"
    return re.compile(pat)


def term_in_text(t_norm: str, term_regex: re.Pattern) -> bool:
    return bool(term_regex.search(t_norm))


# -------------------------
# PRIORITY RULES (from policy)
# -------------------------
def match_pattern(t_norm: str, pattern: dict) -> bool:
    # pattern is expected pre-normalized + pre-compiled in normalize_policy()
    # Structure supported:
    # - all_rx: list[regex]
    # - any_rx: list[regex]
    # - any_group_rx: list[list[regex]]

    if "all_rx" in pattern:
        for rx in pattern["all_rx"]:
            if not term_in_text(t_norm, rx):
                return False

    if "any_rx" in pattern:
        if not any(term_in_text(t_norm, rx) for rx in pattern["any_rx"]):
            return False

    if "any_group_rx" in pattern:
        for group in pattern["any_group_rx"]:
            if not any(term_in_text(t_norm, rx) for rx in group):
                return False

    return True


def _normalize_and_compile_pattern_obj(p: dict) -> dict:
    """
    Normalize and compile regex for a single guardrail pattern dict.
    Keeps original fields for readability, adds compiled:
      - all_rx / any_rx / any_group_rx
    """
    p2 = dict(p)

    if "all" in p2:
        all_norm = [normalize(x) for x in p2["all"]]
        p2["all"] = all_norm
        p2["all_rx"] = [compile_term(x) for x in all_norm]

    if "any" in p2:
        any_norm = [normalize(x) for x in p2["any"]]
        p2["any"] = any_norm
        p2["any_rx"] = [compile_term(x) for x in any_norm]

    if "any_group" in p2:
        anyg_norm = [[normalize(x) for x in grp] for grp in p2["any_group"]]
        p2["any_group"] = anyg_norm
        p2["any_group_rx"] = [[compile_term(x) for x in grp] for grp in anyg_norm]

    return p2


def normalize_policy(policy: dict) -> dict:
    policy = dict(policy)

    # Normalize keywords P1/P2/P3 (keep as strings + also compile)
    for lvl in ["P1", "P2", "P3"]:
        if "levels" in policy and lvl in policy["levels"]:
            kws = [normalize(k) for k in policy["levels"][lvl].get("keywords", [])]
            policy["levels"][lvl]["keywords"] = kws
            policy["levels"][lvl]["keywords_rx"] = [compile_term(k) for k in kws]

    # Normalize + compile ALL guardrails patterns
    guard = policy.get("guardrails", {})
    patterns = guard.get("patterns", {})
    compiled_patterns = {}

    for lvl, plist in patterns.items():
        if isinstance(plist, list):
            compiled_patterns[lvl] = [_normalize_and_compile_pattern_obj(p) for p in plist]
        else:
            compiled_patterns[lvl] = plist

    # write back
    if "guardrails" not in policy:
        policy["guardrails"] = {}
    policy["guardrails"]["patterns"] = compiled_patterns

    return policy


def rules_classify_priority(text_clean: str, policy: dict) -> Tuple[str, str]:
    t = normalize(text_clean)
    if not t:
        return "P3", "EMPTY_TEXT"

    patterns = policy.get("guardrails", {}).get("patterns", {})

    # 1) P0 guardrails first
    for p in patterns.get("P0", []):
        if match_pattern(t, p):
            return "P0", p.get("id", "P0_PATTERN")

    # 2) P1 guardrails
    for p in patterns.get("P1", []):
        if match_pattern(t, p):
            return "P1", p.get("id", "P1_PATTERN")

    # 3) P1/P2/P3 keywords
    for level in ["P1", "P2", "P3"]:
        kws_rx = policy.get("levels", {}).get(level, {}).get("keywords_rx", [])
        if any(term_in_text(t, rx) for rx in kws_rx):
            return level, f"{level}_KEYWORD"

    return "P3", "DEFAULT"


# -------------------------
# CATEGORY RULES (baseline) with compiled regex
# -------------------------
WATER_TERMS_PATCH = [
    # fuite eau (explicite)
    "fuite d eau", "fuite d'eau", "fuite eau",
    # eau / plomberie
    "inondation", "eau", "eau monte", "eau partout",
    "canalisation", "egout", "égout", "evacuation", "évacuation",
    "humidite", "humidité",
    # darija
    "lma", "kayn lma", "kayna lma", "lma kaydir", "mouchkil f lma",
    # coupure/panne eau
    "panne d eau", "panne d'eau", "coupure d eau", "coupure d'eau",
    "plus d eau", "plus d'eau", "ma kaynch lma", "ma kaynach lma",
]

CATEGORY_RULES: List[Dict] = [
    # Order matters: first match wins
    {
        "category": "security",
        "id": "CAT_SECURITY",
        "terms": [
            "agression", "bagarre", "violence", "menace", "personne suspecte", "intrus",
            "voleur", "vol", "cambriolage", "porte forcee", "porte forcée",
            "serrure cassee", "serrure cassée",
            "camera", "caméra", "camera securite", "caméra securite", "camera sécurité", "caméra sécurité",
            "gardien", "securite", "sécurité",
            "mdabza", "tferga3"
        ],
    },
    {
        "category": "elevator",
        "id": "CAT_ELEVATOR",
        "terms": [
            "ascenseur", "asansour", "asenseur", "lift", "m7bouss", "m7boussa",
            "coince", "coincé", "bloque", "bloqué"
        ],
    },
    {
        "category": "electricity",
        "id": "CAT_ELECTRICITY",
        "terms": [
            "panne electrique", "panne électrique", "coupure electrique", "coupure électrique",
            "electricite", "électricité", "courant", "daw", "dow",
            "disjoncteur", "court circuit", "court-circuit",
            "etincelles", "étincelles", "sparks", "risque electrique", "risque électrique",
            "tchicha dyal lbar9"
        ],
    },
    {
        "category": "watr_leak",
        "id": "CAT_WATER",
        "terms": WATER_TERMS_PATCH,
    },
    {
        "category": "garage_access",
        "id": "CAT_GARAGE_ACCESS",
        "terms": [
            "garage", "porte garage", "portail", "barriere", "barrière",
            "badge", "telecommande", "télécommande",
            "acces", "accès", "entree", "entrée", "sortie",
            "mbloque", "bloque", "bloqué", "mabghach yt7el", "kayt7ellch"
        ],
    },
    
    {
        "category": "cleanliness",
        "id": "CAT_CLEANLINESS",
        "terms": [
            "proprete", "propreté", "salete", "saleté", "mwsekh", "moskhin",
            "poubelles", "poubelle", "zbel", "dechets", "déchets",
            "nettoyage", "sale", "ordures", "escaliers mwsekh", "droj moskhin"
        ],
    },
    {
        "category": "noise",
        "id": "CAT_NOISE",
        "terms": [
            "bruit", "nuisance", "voisin derange", "voisin dérange",
            "musique", "tapage", "sda3"
        ],
    },
    {
        "category": "admin",
        "id": "CAT_ADMIN",
        "terms": [
            "attestation", "attestation de residence", "attestation de résidence",
            "quittance", "facture", "charges", "paiement", "reglement", "règlement",
            "suivi", "retard dossier", "pas de reponse", "pas de réponse", "delai", "délai",
            "khlass", "flouss", "war9a", "ma jawbounich", "ch7al khassni nkhless"
        ],
    },
]


def compile_category_rules(rules: List[Dict]) -> List[Dict]:
    compiled = []
    for r in rules:
        terms_norm = [normalize(x) for x in r.get("terms", []) if str(x).strip()]
        compiled.append(
            {
                "category": r["category"],
                "id": r["id"],
                "terms": terms_norm,
                "terms_rx": [compile_term(t) for t in terms_norm],
            }
        )
    return compiled


CATEGORY_RULES_COMPILED = compile_category_rules(CATEGORY_RULES)


def detect_category(text_clean: str) -> Tuple[str, str]:
    t = normalize(text_clean)
    if not t:
        return "other", "CAT_EMPTY_TEXT"

    for r in CATEGORY_RULES_COMPILED:
        if any(term_in_text(t, rx) for rx in r["terms_rx"]):
            return r["category"], r["id"]

    return "other", "CAT_DEFAULT"


# -------------------------
# MAIN
# -------------------------
def main():
    base_dir = Path(__file__).resolve().parent.parent
    input_path = base_dir / "cleanData" / "messages_processed.csv"
    policy_path = base_dir / "policy" / "policy_config.json"
    output_path = base_dir / "cleanData" / "messages_rules.csv"

    df = pd.read_csv(input_path)

    if "text_clean" not in df.columns:
        raise ValueError("❌ Column 'text_clean' not found in dataset")

    with open(policy_path, "r", encoding="utf-8") as f:
        policy = json.load(f)

    policy = normalize_policy(policy)

    # Priority
    df["priority_rules"], df["rule_match"] = zip(
        *df["text_clean"].fillna("").apply(lambda x: rules_classify_priority(x, policy))
    )
    df["is_urgent_rules"] = df["priority_rules"].isin(["P0", "P1"]).astype(int)

    # Category
    df["category"], df["category_match"] = zip(
        *df["text_clean"].fillna("").apply(detect_category)
    )
    FORCE_TO_SECURITY = {"P0_GAS", "P0_FIRE"}

    mask_force = df["rule_match"].isin(FORCE_TO_SECURITY)
    df.loc[mask_force, "category"] = "security"
    df.loc[mask_force, "category_match"] = "CAT_FORCED_SECURITY_P0"

    print(f"\n🔧 Forced security for {mask_force.sum()} rows (P0_GAS/P0_FIRE).")

    df.to_csv(output_path, index=False, encoding="utf-8")
    print("✅ Saved:", output_path)

    print("\npriority_rules")
    print(df["priority_rules"].value_counts())

    print("\ncategory")
    print(df["category"].value_counts())

    print("\nTop rule_match:")
    print(df["rule_match"].value_counts().head(15))

    print("\nTop category_match:")
    print(df["category_match"].value_counts().head(15))


if __name__ == "__main__":
    main()
