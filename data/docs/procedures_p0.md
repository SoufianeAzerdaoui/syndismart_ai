# Procédures - Urgence critique (P0)

Ces cas sont critiques. Toujours prioriser la sécurité.

## Règles P0 (patterns)

### P0_GAS
- Description: Gaz / odeur de gaz => P0
- Un des mots (any): ['gaz', 'odeur de gaz', 'fuite gaz', 'suspicion fuite gaz', 'ri7t lghaz', 'ri7t lgaz']

### P0_FIRE
- Description: Incendie / fumée / flammes => P0
- Un des mots (any): ['incendie', 'fumee', 'flammes', 'dokhan', 'dkhan', 'afia', '3afia']

### P0_ELEVATOR_TRAPPED
- Description: Ascenseur bloqué + personne à l’intérieur => P0
- Doit contenir (all): ['ascenseur']
- Groupes (any_group): [['bloque', 'coince', 'm7bouss', 'm7boussa'], ['personne', "a l'interieur", 'dakhel', 'dakhla'], ["a l'interieur", 'a linterieur', 'a l interieur', 'interieur', "a l'interieur"]]

### P0_FLOOD_MAJOR
- Description: Inondation majeure / eau monte / partout / risque électrique => P0
- Doit contenir (all): ['inondation']
- Un des mots (any): ['majeure', 'partout', 'eau monte', 'risque electrique']

### P0_VIOLENCE
- Description: Agression / bagarre => P0
- Un des mots (any): ['agression', 'bagarre', 'violence', 'menace', 'mdabza', 'tferga3']

### P0_MEDICAL
- Description: Urgence médicale => P0
- Un des mots (any): ['inconscient', 'personne inconsciente', 'ne respire pas', 'respiration', 'cardiaque', 'personne agee tombee']

## Actions recommandées (P0)
- Demander la localisation exacte (résidence/bloc/étage).
- Confirmer si danger immédiat (oui/non).
- Demander photo/vidéo si possible (sans retarder l'urgence).
- Escalader immédiatement vers prestataire/sécurité selon catégorie.
