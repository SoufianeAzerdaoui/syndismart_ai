# src/10_validate_generation_outputs.py
from __future__ import annotations

import json
from pathlib import Path
import pandas as pd


SLA_TABLE = {"P0": 5, "P1": 30, "P2": 240, "P3": 1440}


def is_json_dict(s: str) -> bool:
    try:
        obj = json.loads(s)
        return isinstance(obj, dict)
    except Exception:
        return False


def safe_load_json(s: str):
    try:
        return json.loads(s)
    except Exception:
        return None


def main():
    base_dir = Path(__file__).resolve().parent.parent
    in_path = base_dir / "cleanData" / "messages_final.csv"
    audit_dir = base_dir / "cleanData" / "audit"
    audit_dir.mkdir(parents=True, exist_ok=True)

    if not in_path.exists():
        raise SystemExit(f"File not found: {in_path}")

    df = pd.read_csv(in_path)

    required_cols = {
        "message_id",
        "gen_json",
        "urgency_level",
        "category",
        "is_urgent",
        "sla_target_minutes",
        "assigned_to",
        "status",
        "decision_source",
        "response_draft",
        "required_info",
    }
    missing = required_cols - set(df.columns)
    if missing:
        raise SystemExit(f"Missing columns in messages_final.csv: {sorted(missing)}")

    # ---- Checks
    df["check_gen_json_dict"] = df["gen_json"].fillna("").apply(is_json_dict)

    # required_info should be JSON list
    def required_info_ok(x: str) -> bool:
        obj = safe_load_json(str(x or ""))
        return isinstance(obj, list)

    df["check_required_info_list"] = df["required_info"].fillna("").apply(required_info_ok)

    # SLA check
    def sla_ok(row) -> bool:
        lvl = str(row.get("urgency_level", "")).strip().upper()
        try:
            sla = int(row.get("sla_target_minutes"))
        except Exception:
            return False
        return SLA_TABLE.get(lvl, 1440) == sla

    df["check_sla"] = df.apply(sla_ok, axis=1)

    # is_urgent check
    def is_urgent_ok(row) -> bool:
        lvl = str(row.get("urgency_level", "")).strip().upper()
        try:
            iu = int(row.get("is_urgent"))
        except Exception:
            return False
        expected = 1 if lvl in {"P0", "P1"} else 0
        return iu == expected

    df["check_is_urgent"] = df.apply(is_urgent_ok, axis=1)

    # response_draft non-empty
    df["check_response_nonempty"] = df["response_draft"].fillna("").str.strip().ne("")

    # decision_source == RAG
    df["check_decision_source"] = df["decision_source"].fillna("").str.strip().eq("RAG")

    # status exists
    df["check_status"] = df["status"].fillna("").str.strip().ne("")

    checks = [
        "check_gen_json_dict",
        "check_required_info_list",
        "check_sla",
        "check_is_urgent",
        "check_response_nonempty",
        "check_decision_source",
        "check_status",
    ]

    # ---- Report summary
    total = len(df)
    print("\n=====================")
    print("✅ GENERATION OUTPUTS VALIDATION CHECKLIST")
    print("=====================")
    print(f"Rows total: {total}")

    for c in checks:
        ok = int(df[c].sum())
        print(f"{c}: {ok}/{total} ({(ok/total*100):.1f}%)")

    # ---- Save detailed report + errors
    report_path = audit_dir / "generation_validation_report.csv"
    df[["message_id", "urgency_level", "category"] + checks].to_csv(report_path, index=False, encoding="utf-8")

    # errors only
    err_mask = ~df[checks].all(axis=1)
    errors = df.loc[err_mask, ["message_id", "urgency_level", "category"] + checks]
    errors_path = audit_dir / "generation_validation_errors.csv"
    errors.to_csv(errors_path, index=False, encoding="utf-8")

    if len(errors) == 0:
        print("\n✅ Aucun problème détecté (selon les checks).")
    else:
        print(f"\n⚠️ Problèmes détectés: {len(errors)} lignes")
        print(f"- Errors: {errors_path}")

    print(f"\n✅ Report: {report_path}")


if __name__ == "__main__":
    main()
