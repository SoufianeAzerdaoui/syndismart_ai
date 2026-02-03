# 03_rules_baseline.py
import json
import pandas as pd
from pathlib import Path
import unicodedata
import re

_space_re = re.compile(r"\s+")
_non_word_re = re.compile(r"[^a-z0-9\s'-]")  # keep basic useful chars

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

def match_pattern(t: str, pattern: dict) -> bool:
    # "all": all terms must appear
    if "all" in pattern:
        for w in pattern["all"]:
            if w not in t:
                return False

    # "any": at least one term must appear
    if "any" in pattern:
        if not any(w in t for w in pattern["any"]):
            return False

    # "any_group": for each group, at least one term must appear
    if "any_group" in pattern:
        for group in pattern["any_group"]:
            if not any(w in t for w in group):
                return False

    return True

def _normalize_pattern_obj(p: dict) -> dict:
    # Normalize a single pattern dict in-place
    if "all" in p:
        p["all"] = [normalize(x) for x in p["all"]]
    if "any" in p:
        p["any"] = [normalize(x) for x in p["any"]]
    if "any_group" in p:
        p["any_group"] = [[normalize(x) for x in grp] for grp in p["any_group"]]
    return p

def normalize_policy(policy: dict) -> dict:
    # Normalize keywords P1/P2/P3
    for lvl in ["P1", "P2", "P3"]:
        if "levels" in policy and lvl in policy["levels"]:
            policy["levels"][lvl]["keywords"] = [
                normalize(k) for k in policy["levels"][lvl].get("keywords", [])
            ]

    # Normalize ALL guardrails patterns (P0, P1, P2, P3 if present)
    guard = policy.get("guardrails", {})
    patterns = guard.get("patterns", {})
    for lvl, plist in patterns.items():
        if isinstance(plist, list):
            for p in plist:
                _normalize_pattern_obj(p)

    return policy

def rules_classify(text_clean: str, policy: dict):
    t = normalize(text_clean)
    if not t:
        return "P3", "EMPTY_TEXT"

    patterns = policy.get("guardrails", {}).get("patterns", {})

    # 1) P0 guardrails first
    for p in patterns.get("P0", []):
        if match_pattern(t, p):
            return "P0", p.get("id", "P0_PATTERN")

    # 2) P1 guardrails (IMPORTANT)
    for p in patterns.get("P1", []):
        if match_pattern(t, p):
            return "P1", p.get("id", "P1_PATTERN")

    # (Optionnel) si tu ajoutes plus tard guardrails P2/P3
    # for p in patterns.get("P2", []):
    #     if match_pattern(t, p):
    #         return "P2", p.get("id", "P2_PATTERN")
    # for p in patterns.get("P3", []):
    #     if match_pattern(t, p):
    #         return "P3", p.get("id", "P3_PATTERN")

    # 3) P1/P2/P3 keywords (simple baseline)
    for level in ["P1", "P2", "P3"]:
        kws = policy.get("levels", {}).get(level, {}).get("keywords", [])
        if any(k in t for k in kws):
            return level, f"{level}_KEYWORD"

    # 4) default
    return "P3", "DEFAULT"

def main():
    base_dir = Path(__file__).resolve().parent.parent
    input_path = base_dir / "cleanData" / "messages_processed.csv"
    policy_path = base_dir / "policy" / "policy_config.json"
    output_path = base_dir / "cleanData" / "messages_rules.csv"

    df = pd.read_csv(input_path)

    with open(policy_path, "r", encoding="utf-8") as f:
        policy = json.load(f)

    policy = normalize_policy(policy)

    df["priority_rules"], df["rule_match"] = zip(
        *df["text_clean"].fillna("").apply(lambda x: rules_classify(x, policy))
    )
    df["is_urgent_rules"] = df["priority_rules"].isin(["P0", "P1"]).astype(int)

    df.to_csv(output_path, index=False, encoding="utf-8")
    print("✅ Saved:", output_path)
    print("\npriority_rules")
    print(df["priority_rules"].value_counts())
    print("\nTop rule_match:")
    print(df["rule_match"].value_counts().head(15))

if __name__ == "__main__":
    main()
