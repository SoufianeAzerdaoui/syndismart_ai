import pandas as pd
from pathlib import Path

from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, confusion_matrix


### executer ce fichier pour avoir l'accuracu du model ok 



def main():
    base_dir = Path(__file__).resolve().parent.parent
    rules_csv = base_dir / "cleanData" / "messages_rules.csv"
    out_dir = base_dir / "cleanData" / "ml_baseline"
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(rules_csv)

    # Colonnes minimales
    for col in ["text_clean","priority_rules","message_id","rule_match","residence_id"]:
        if col not in df.columns:
            df[col] = ""

    df = df.dropna(subset=["text_clean","priority_rules"]).copy()
    df["text_clean"] = df["text_clean"].astype(str).fillna("")
    df["priority_rules"] = df["priority_rules"].astype(str).fillna("P3")

    # Split stratifié pour garder proportions
    X_train, X_test, y_train, y_test = train_test_split(
        df["text_clean"],
        df["priority_rules"],
        test_size=0.25,
        random_state=42,
        stratify=df["priority_rules"] if df["priority_rules"].nunique() > 1 else None
    )

    # Pipeline simple
    clf = Pipeline([
        ("tfidf", TfidfVectorizer(
            lowercase=True,
            ngram_range=(1,2),
            min_df=1,
            max_df=0.95
        )),
        ("lr", LogisticRegression(
            max_iter=2000,
            class_weight="balanced"
        ))
    ])

    clf.fit(X_train, y_train)
    y_pred = clf.predict(X_test)

    # Reports
    report = classification_report(y_test, y_pred, digits=4)
    cm = confusion_matrix(y_test, y_pred, labels=["P0","P1","P2","P3"])

    print("\n=====================")
    print("📌 CLASSIFICATION REPORT")
    print("=====================")
    print(report)

    print("\n=====================")
    print("📌 CONFUSION MATRIX (labels P0,P1,P2,P3)")
    print("=====================")
    print(cm)

    # Sauver outputs
    (out_dir / "classification_report.txt").write_text(report, encoding="utf-8")

    cm_df = pd.DataFrame(cm, index=["true_P0","true_P1","true_P2","true_P3"],
                            columns=["pred_P0","pred_P1","pred_P2","pred_P3"])
    cm_df.to_csv(out_dir / "confusion_matrix.csv", index=True, encoding="utf-8")

    # Export erreurs
    test_df = df.loc[X_test.index].copy()
    test_df["ml_pred"] = y_pred
    test_df["is_error"] = (test_df["ml_pred"] != test_df["priority_rules"]).astype(int)

    errors = test_df[test_df["is_error"] == 1].copy()
    errors.to_csv(out_dir / "errors_ml_vs_rules.csv", index=False, encoding="utf-8")

    print("\n✅ Exports ML dans:", out_dir)
    print("Erreurs ML vs Rules:", len(errors), "/", len(test_df))

if __name__ == "__main__":
    main()
