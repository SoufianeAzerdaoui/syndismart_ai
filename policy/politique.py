import json

def get_syndic_policy():
    """
    Définit la politique d'urgence sous forme de dictionnaire structuré.
    Cette structure sera utilisée pour le filtrage par mots-clés et le prompt du LLM.
    """
    policy = {
        "P0": {
            "label": "Urgence critique",
            "sla_response_min": 5,
            "sla_action_min": 30,
            "keywords": [
                "incendie", "fumée", "flammes", "gaz", "fuite gaz", "inondation majeure",
                "ascenseur bloqué", "agression", "bagarre", "menace", "dokhan", "afia",
                "m7bouss", "tferga3", "gher9at", "fayadan", "inconsciente", "cardiaque",
                "respiration", "porte forcée", "serrure cassée", "mdabza"
            ],
            "response_template": "⚠️ Urgence critique détectée. Restez en sécurité. Un agent est alerté immédiatement. Appelez le service d'urgence si danger de mort."
        },
        "P1": {
            "label": "Urgent",
            "sla_response_min": 30,
            "sla_action_hours": 4,
            "keywords": [
                "panne eau", "panne électrique", "garage bloqué", "fuite", "ascenseur en panne",
                "ma m9to3", "dow m9to3", "mabghach yt7el", "suspect", "camera panne"
            ],
            "response_template": "Nous avons bien reçu votre signalement urgent. Un ticket a été créé et le prestataire est contacté."
        },
        "P2": {
            "label": "Maintenance / Nuisance",
            "sla_response_min": 240, # 4h
            "sla_action_hours": 72,
            "keywords": [
                "bruit", "propreté", "poubelles", "badge", "télécommande", "saleté",
                "stationnement", "zbel", "mwsekh", "nuisance", "sda3"
            ],
            "response_template": "Votre demande de maintenance a été enregistrée. Nous reviendrons vers vous pour planifier l'intervention."
        },
        "P3": {
            "label": "Administratif",
            "sla_response_min": 1440,
            "sla_action_days": 5,
            "keywords": [
                "attestation", "charges", "quittance", "facture", "règlement", "war9a",
                "khlass", "flouss", "suivi", "ma jawbounich", "delai"
            ],
            "response_template": "Demande administrative reçue. Nous traitons votre dossier dans les plus brefs délais."
        }
    }
    return policy

with open('policy_config.json', 'w', encoding='utf-8') as f:
    json.dump(get_syndic_policy(), f, ensure_ascii=False, indent=4)

print("La politique a été générée et sauvegardée dans 'policy_config.json'.")