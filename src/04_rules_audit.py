# 04_audit_rules.py
import pandas as pd
from pathlib import Path

def main():
    base_dir = Path(__file__).resolve().parent.parent
    rules_path = base_dir / "cleanData" / "messages_rules.csv"
    out_dir = base_dir / "cleanData" / "audit"
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(rules_path)

    # Exports “vérité terrain” pour inspection manuelle
    df_p0 = df[df["priority_rules"] == "P0"].copy()
    df_default = df[df["rule_match"] == "DEFAULT"].copy()

    p0_path = out_dir / "p0_all.csv"
    default_path = out_dir / "default_all.csv"

    df_p0.to_csv(p0_path, index=False, encoding="utf-8")
    df_default.to_csv(default_path, index=False, encoding="utf-8")

    print("\n=====================")
    print("📌 STATS par residence_id")
    print("=====================")
    pivot = (
        df.pivot_table(index="residence_id", columns="priority_rules", values="message_id",
                       aggfunc="count", fill_value=0)
        .reset_index()
    )
    # Assure colonnes dans l’ordre P0..P3
    for col in ["P0","P1","P2","P3"]:
        if col not in pivot.columns:
            pivot[col] = 0
    pivot = pivot[["residence_id","P0","P1","P2","P3"]]
    print(pivot.to_string(index=False))

    print("\n✅ Audit exports dans:", out_dir)
    print(" -", p0_path)
    print(" -", default_path)
    print("\n📌 Counts:")
    print("P0:", len(df_p0), "| DEFAULT:", len(df_default))

if __name__ == "__main__":
    main()
