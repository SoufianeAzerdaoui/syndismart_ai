import re
import pandas as pd
from pathlib import Path

# =========================
# Regex & tokens
# =========================

ARABIC_RE = re.compile(r"[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]")
LATIN_RE  = re.compile(r"[A-Za-zÀ-ÖØ-öø-ÿ]")

ARABIZI_WORD_RE = re.compile(
    r"\b[a-z]+[2356789][a-z]+\b|\b[a-z]*[2356789][a-z]{2,}\b",
    re.IGNORECASE
)

DARIJA_TOKENS = [
    "salam","slm","bghit","kayn","kaynch","ma kaynch","wach","ash","chno",
    "khassni","ndir","lma","daw","ri7a","hadi","bzaf","khatira",
    "m7bouss","bab","kayt7ellch","7abssna","dakhel","f darna","mn sbah",
    "jaran","bghina","tdkhl","sda3","mdabza"
]

FRENCH_TOKENS = [
    "bonjour","merci","svp","s'il","c'est","procédure","facture","charges","quittance",
    "ascenseur","inondation","gaz","électricité","panne","porte","garage","propreté",
    "comment","pouvez-vous","envoyer","détail","personnes âgées","immeuble","couloir",
    "urgence","urgent","intervenir","réclamation","nettoyage","voisins"
]

DARIJA_SINGLE = [t for t in DARIJA_TOKENS if " " not in t]
DARIJA_MULTI  = [t for t in DARIJA_TOKENS if " " in t]

FRENCH_SINGLE = [t for t in FRENCH_TOKENS if " " not in t]
FRENCH_MULTI  = [t for t in FRENCH_TOKENS if " " in t]

DARIJA_SINGLE_RE = re.compile(r"\b(" + "|".join(map(re.escape, DARIJA_SINGLE)) + r")\b", re.IGNORECASE)
FRENCH_SINGLE_RE = re.compile(r"\b(" + "|".join(map(re.escape, FRENCH_SINGLE)) + r")\b", re.IGNORECASE)


# =========================
# Language detection logic
# =========================

def detect_language_scored(text_clean: str) -> str:
    t = (text_clean or "").strip().lower()
    if not t:
        return "unknown"

    has_ar  = bool(ARABIC_RE.search(t))
    has_lat = bool(LATIN_RE.search(t))

    if has_ar:
        return "mixed" if has_lat else "darija"

    if not has_lat:
        return "unknown"

    darija_hits = 0
    fr_hits = 0

    darija_hits += len(DARIJA_SINGLE_RE.findall(t))
    darija_hits += sum(1 for tok in DARIJA_MULTI if tok in t)

    if ARABIZI_WORD_RE.search(t):
        darija_hits += 2

    fr_hits += len(FRENCH_SINGLE_RE.findall(t))
    fr_hits += sum(1 for tok in FRENCH_MULTI if tok in t)

    if darija_hits >= 2 and fr_hits >= 2:
        return "mixed"
    if darija_hits >= 2 and fr_hits == 0:
        return "darija"
    if fr_hits >= 1 and darija_hits == 0:
        return "fr"

    return "fr" if fr_hits >= darija_hits else "darija"


# =========================
# Main pipeline
# =========================

def main():
    base_dir = Path(__file__).resolve().parent.parent
    input_path  = base_dir / "cleanData" / "messages_processed.csv"
    output_path = base_dir / "cleanData" / "messages_final.csv"

    print(f"📥 Loading: {input_path}")
    df = pd.read_csv(input_path)

    if "text_clean" not in df.columns:
        raise ValueError("❌ Column 'text_clean' not found in dataset")

    df["language"] = df["text_clean"].apply(detect_language_scored)

    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"✅ Language detection done. Saved to: {output_path}")

    print("\n📊 Language distribution:")
    print(df["language"].value_counts())


if __name__ == "__main__":
    main()
