# 02_language_detection_ml.py
# ML (CPU-friendly) language detection for WhatsApp messages:
# classes: fr / darija / mixed / unknown
#
# Input : ../cleanData/messages_processed.csv (must contain text_clean)
# Output: updates the SAME file (messages_processed.csv) by adding:
#         - language
#         - language_confidence
# Also saves: ../cleanData/to_label_uncertain.csv (top uncertain samples)
# Creates a backup once per run: messages_processed.backup.csv

import re
import numpy as np
import pandas as pd
from pathlib import Path

from sklearn.pipeline import Pipeline
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import StratifiedKFold, cross_val_score


# -------------------------
# 1) Weak supervision (seed labels)
# -------------------------
ARABIC_RE = re.compile(r"[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]")
LATIN_RE = re.compile(r"[A-Za-zÀ-ÖØ-öø-ÿ]")
ARABIZI_RE = re.compile(r"\b[a-z]{2,}[35679][a-z]{2,}\b", re.IGNORECASE)

def seed_label(text_clean: str) -> str:
    t = (text_clean or "").strip().lower()
    if not t or len(t) < 2:
        return "unknown"

    has_ar = bool(ARABIC_RE.search(t))
    has_lat = bool(LATIN_RE.search(t))
    has_arabizi = bool(ARABIZI_RE.search(t))

    # Arabic script present
    if has_ar and has_lat:
        return "mixed"
    if has_ar and not has_lat:
        return "darija"

    # Latin only
    if has_arabizi:
        return "darija"

    return "fr"


# -------------------------
# 2) Train ML model
# -------------------------
def choose_training_label_column(df: pd.DataFrame) -> str:
    """
    Prefer manual labels when available and sufficient, else fall back to seed labels.
    """
    if "language_manual" in df.columns:
        m = df["language_manual"].fillna("").astype(str).str.strip().str.lower()
        df["language_manual"] = m
        if (m != "").sum() >= 20:
            print("✅ Using language_manual for training (>=20 labeled rows).")
            return "language_manual"
        else:
            print("ℹ️ language_manual exists but <20 labels. Using language_seed.")
    return "language_seed"


def train_lang_model(df: pd.DataFrame, text_col="text_clean", label_col="language_seed") -> Pipeline:
    X = df[text_col].fillna("").astype(str)
    y = df[label_col].fillna("").astype(str).str.strip().str.lower()

    # Train only on known labels
    mask = y.isin(["fr", "darija", "mixed"])
    X_train, y_train = X[mask], y[mask]

    if len(X_train) < 20:
        raise ValueError("Not enough labeled samples to train (need at least ~20 labeled rows).")

    model = Pipeline([
        ("tfidf", TfidfVectorizer(
            analyzer="char",
            ngram_range=(3, 6),
            min_df=1
        )),
        ("clf", LogisticRegression(
            max_iter=2000,
            class_weight="balanced"
        ))
    ])

    vc = y_train.value_counts()
    if len(vc) >= 2 and vc.min() >= 3:
        cv = StratifiedKFold(n_splits=3, shuffle=True, random_state=42)
        scores = cross_val_score(model, X_train, y_train, cv=cv, scoring="f1_macro")
        print(f"📌 CV f1_macro: {scores.round(3).tolist()} | mean={scores.mean():.3f}")
    else:
        print("ℹ️ Skipping CV (not enough samples per class).")

    model.fit(X_train, y_train)
    return model


# -------------------------
# 3) Predict with confidence + mixed handling
# -------------------------
def predict_with_confidence(model: Pipeline, texts: pd.Series, p_min=0.45, margin=0.08):
    """
    Predict label + confidence.
    - If confidence < p_min -> unknown
    - If top1-top2 < margin -> mixed
    """
    X = texts.fillna("").astype(str).values
    proba = model.predict_proba(X)
    classes = model.classes_

    top1_idx = np.argmax(proba, axis=1)
    top1_label = classes[top1_idx]
    top1_p = proba[np.arange(len(X)), top1_idx]

    top2_p = np.partition(proba, -2, axis=1)[:, -2]

    final = []
    for lbl, p1, p2, txt in zip(top1_label, top1_p, top2_p, X):
        t = (txt or "").strip()
        if not t or len(t) < 2:
            final.append("unknown")
            continue

        if p1 < p_min:
            final.append("unknown")
        elif (p1 - p2) < margin:
            final.append("mixed")
        else:
            final.append(lbl)

    return final, top1_p


def build_uncertain_report(df: pd.DataFrame, conf: np.ndarray, n=15) -> pd.DataFrame:
    tmp = df.copy()
    tmp["language_confidence"] = conf
    cols = [c for c in ["message_id", "datetime", "residence_id", "text_clean", "language_confidence"] if c in tmp.columns]
    return tmp.sort_values("language_confidence", ascending=True).head(n)[cols]


# -------------------------
# 4) Main (auto-merge into messages_processed.csv)
# -------------------------
def main():
    # src/ -> project root
    base_dir = Path(__file__).resolve().parent.parent

    input_path = base_dir / "cleanData" / "messages_processed.csv"
    uncertain_path = base_dir / "cleanData" / "to_label_uncertain.csv"
    backup_path = base_dir / "cleanData" / "messages_processed.backup.csv"

    print(f"📥 Loading: {input_path}")
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    df = pd.read_csv(input_path)

    if "text_clean" not in df.columns:
        raise ValueError("❌ Column 'text_clean' not found in dataset")

    # Backup once per run
    df.to_csv(backup_path, index=False, encoding="utf-8")
    print(f"🧷 Backup saved: {backup_path}")

    # Seed labels
    df["language_seed"] = df["text_clean"].apply(seed_label)
    print("\n📊 Seed label distribution:")
    print(df["language_seed"].value_counts(dropna=False))

    # Train ML model using manual labels if enough
    label_col = choose_training_label_column(df)
    model = train_lang_model(df, label_col=label_col)

    # Predict final language + confidence
    df["language"], conf = predict_with_confidence(
        model,
        df["text_clean"],
        p_min=0.45,
        margin=0.08
    )
    df["language_confidence"] = conf

    # Save uncertain samples for manual labeling
    uncertain_df = build_uncertain_report(df, conf, n=15)
    uncertain_df.to_csv(uncertain_path, index=False, encoding="utf-8")
    print(f"📝 Uncertain samples saved: {uncertain_path}")

    df = df.drop(columns=["language_seed"], errors="ignore")

    # IMPORTANT: write back to the SAME file, adding only language columns (+ keeps all existing cols)
    df.to_csv(input_path, index=False, encoding="utf-8")
    print(f"\n✅ Updated in-place: {input_path}")

    print("\n📊 Final language distribution:")
    print(df["language"].value_counts(dropna=False))


if __name__ == "__main__":
    main()
