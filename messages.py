import pandas as pd

df = pd.read_csv("whatsapp_syndic_texts_100.csv")
df = df.dropna(subset=["text"])

df["text_clean"] = df["text"].str.strip().str.lower()

df["has_media"] = df["has_media"].fillna(0)

    
df.to_csv("messages_clean.csv", index=False)

print(df.head())
