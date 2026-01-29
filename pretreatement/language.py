import pandas as pd
import fasttext
import urllib.request
import os
import re
import ssl

# 1. Chargement du modèle FastText pour l'IA (si pas déjà présent)
ssl._create_default_https_context = ssl._create_unverified_context

model_path = "lid.176.ftz"
url = "https://dl.fbaipublicfiles.com/fasttext/lid.176.ftz"

if not os.path.exists(model_path):
    print("Téléchargement du modèle FastText avec User-Agent...")
    # Configuration d'un User-Agent de navigateur
    opener = urllib.request.build_opener()
    opener.addheaders = [('User-agent', 'Mozilla/5.0')]
    urllib.request.install_opener(opener)
    
    # Tentative de téléchargement
    urllib.request.urlretrieve(url, model_path)
    print("Modèle téléchargé avec succès.")

# 2. Chargement de l'input CleanData [cite: 90, 94]
input_file = "cleanData/messages_preprocessed.csv"
df = pd.read_csv(input_file)

def detect_language_expert(text):
    if pd.isna(text) or str(text).strip() == "":
        return "Inconnu"
    
    # IA : Prédiction globale (FastText)
    predictions = ft_model.predict(text.replace('\n', ' '), k=1)
    detected_lang = predictions[0][0].replace('__label__', '')
    
    # Heuristique Data Engineer : Détection Darija (Arabizi) [cite: 20]
    # On cherche des patterns spécifiques à vos messages (ex: "ri7a", "dyal", "lma")
    darija_patterns = r'\b(kayn|moshkil|dyal|ri7a|lma|darna|fhal|shno|chrayti|7ta|bazaf|khadira|7abssna)\b'
    is_darija = bool(re.search(darija_patterns, text.lower()))
    
    if is_darija and detected_lang == 'fr':
        return "Français/Darija"
    elif is_darija:
        return "Darija"
    elif detected_lang == 'fr':
        return "Français"
    else:
        return "Autre"

# 3. Application sur text_clean
df['language'] = df['text_clean'].apply(detect_language_expert)

# 4. Sauvegarde du dataset enrichi [cite: 13, 80]
df.to_csv("cleanData/messages_with_lang.csv", index=False, encoding='utf-8')

print("Détection terminée. Colonne 'language' ajoutée.")
display(df[['text_clean', 'language']].head(10))