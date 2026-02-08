from pathlib import Path

DOCS = {
"reservation_salle_polyvalente.md": """# Réservation de la salle polyvalente

## Objectif
Décrire la procédure standard pour réserver la salle polyvalente (résidence), avec les informations minimales à collecter, les délais, et le circuit de validation.

## Cas concernés
- Demande de réservation (événement, réunion, anniversaire, etc.)
- Demande de disponibilité d’une date
- Demande de tarifs / caution / règlement intérieur

## Procédure (étapes)
1. **Collecter les informations minimales** (voir section "Informations à demander").
2. **Vérifier la disponibilité** (calendrier / registre de réservation).
3. **Vérifier l’éligibilité** (résident de la résidence + absence d’impayés si applicable).
4. **Confirmer les conditions** (horaires, capacité, bruit, nettoyage, caution).
5. **Créer un ticket** et transmettre au **syndic** pour validation finale.
6. **Confirmation au résident** : date/heure, règles, modalités de remise des clés, caution.
7. **Après événement** : état des lieux / restitution clés / validation nettoyage.

## Informations à demander (required_info)
- Résidence / bloc
- Nom + numéro d’appartement
- Date souhaitée
- Heure début / heure fin
- Type d’événement + nombre de personnes approximatif
- Téléphone de contact (si différent)
- Besoins particuliers (tables, chaises, sono si existant)

## Règles & contraintes (exemples standard)
- Respect des horaires autorisés (ex: pas après 22h si règlement intérieur).
- Respect du voisinage : bruit modéré, pas de débordements dans les parties communes.
- Nettoyage obligatoire après usage.
- Interdiction de certains usages si règlement (ex: cuisson/BBQ, alcool, etc. selon résidence).

## Délais (SLA interne indicatif)
- Réponse initiale : **P3 (administratif) = 24h**
- Validation complète : selon disponibilité + règles internes (souvent 24–72h)

## Responsabilités
- **Syndic** : validation, conditions, suivi administratif.
- **Gardien** (si applicable) : remise des clés / ouverture / fermeture / état des lieux.
- **Résident** : respect des règles, caution, état des lieux, nettoyage.

## Modèle de réponse (base)
- Confirmer la demande + demander les infos minimales.
- Indiquer que la réservation est soumise à disponibilité + validation syndic.
- Donner les règles clés (horaires / nettoyage / caution si applicable).

## Notes
- Si le résident demande "la procédure" sans date : fournir étapes + infos à préparer.
- Si le résident propose une date : demander uniquement les infos manquantes (ne pas sur-demander).
""",

"charges_et_quittances.md": """# Détail des charges & quittances (facturation / justificatifs)

## Objectif
Standardiser les réponses pour :
- détail des charges mensuelles
- quittance / reçu de paiement
- facture / relevé
- demande de justificatif (PDF, email)

## Cas typiques
- "Envoyez-moi le détail des charges du mois dernier"
- "Je veux une quittance"
- "Je n’ai pas reçu la facture"
- "Combien je dois payer ?"

## Procédure (étapes)
1. **Identifier la demande** : détail charges / quittance / facture / solde.
2. **Collecter les informations minimales** (voir section).
3. **Vérifier la période** demandée (mois/année).
4. **Vérifier l’identité** (résidence + appartement ; selon politique interne).
5. **Transmettre au syndic / service admin** pour extraction (ou automatisé si disponible).
6. **Envoyer le document** via canal convenu (WhatsApp / email) ou indiquer comment le récupérer.
7. **Créer / mettre à jour le ticket** (traçabilité).

## Informations à demander (required_info)
- Résidence / bloc
- Numéro d’appartement
- Période concernée (mois + année)
- Type de document : détail charges / quittance / facture
- Email (si envoi PDF) ou confirmation d’envoi via WhatsApp

## Délais (SLA interne indicatif)
- P3 (administratif) : **24h** pour réponse initiale
- Envoi du document : 24–72h selon process interne

## Responsabilités
- **Syndic / Admin** : génération du document, vérification des montants, envoi.
- **Résident** : fournir période + références nécessaires.

## Points d’attention
- Si le résident demande "mois dernier" : reformuler en demandant le mois exact si ambigu (ex: fin/début d’année).
- Ne pas demander photo/vidéo (inutile).
- Si demande de paiement / montant : demander la période + type + vérifier si "solde" ou "appel de fonds".

## Modèle de réponse (base)
- Confirmer la demande.
- Demander résidence/bloc + appart + période.
- Proposer l’envoi PDF par email (si besoin).
- Indiquer délai de traitement.
""",

"electricite_etincelles.md": """# Électricité : étincelles / court-circuit / risque électrique

## Objectif
Gérer un signalement électrique potentiellement dangereux (P1, voire P0 si danger grave).
Inclure consignes de sécurité + collecte d’infos minimales + escalade prestataire.

## Cas typiques
- "Étincelles près du tableau électrique"
- "Odeur de brûlé"
- "Court-circuit"
- "Disjoncteur qui saute"
- "Fumée / feu" (dans ce cas -> P0 incendie)

## Priorité recommandée
- **P1** : étincelles, court-circuit, panne parties communes, odeur de brûlé
- **P0** : fumée/flammes/incendie confirmé OU risque immédiat majeur + personnes en danger

## Consignes de sécurité (à inclure dans la réponse P1/P0)
- Ne pas toucher au tableau / câbles
- Éloigner les personnes (surtout enfants/personnes âgées)
- Si possible et sans danger : couper l’alimentation générale (uniquement si procédure interne autorise)
- En cas de fumée/flammes : appeler les urgences immédiatement

## Informations à demander (required_info)
- Résidence / bloc
- Localisation précise (étage, couloir, local technique, près du tableau électrique, etc.)
- Depuis quand
- Présence de fumée / odeur de brûlé
- Est-ce que l’électricité est coupée dans les parties communes ?
- Photo/vidéo uniquement si possible **sans danger**

## Procédure (étapes)
1. Confirmer réception + **sécurité d’abord**.
2. Classer P1 (ou P0 si feu/fumée).
3. Créer ticket **prestataire électricité** + alerte immédiate si risque.
4. Si parties communes impactées : informer gardien si applicable.
5. Suivi : heure d’intervention + clôture après sécurisation.

## Responsabilités
- **Prestataire** : diagnostic et intervention.
- **Syndic** : coordination + communication.
- **Gardien** (si applicable) : sécuriser zone, accès techniciens.

## Modèle de réponse (base)
- 1 phrase : "Signalement électrique urgent reçu."
- 1 phrase sécurité : "Ne touchez pas… éloignez-vous…"
- 1 phrase action : "Prestataire alerté / ticket créé."
- 2–3 questions max (localisation + fumée/odeur + depuis quand).
""",

"ascenseur_panne.md": """# Ascenseur : panne / blocage / personne coincée

## Objectif
Différencier :
- **panne simple** (P1)
- **personne bloquée à l’intérieur** (P0)
et standardiser l’escalade + infos à collecter.

## Cas typiques
- "Ascenseur en panne depuis 3 jours"
- "Ascenseur bloqué entre 2 étages"
- "Quelqu’un est coincé à l’intérieur"
- "Portes ne s’ouvrent pas"

## Priorité recommandée
- **P0** : ascenseur bloqué + personne(s) à l’intérieur / détresse / malaise
- **P1** : ascenseur hors service sans personne bloquée (surtout si personnes âgées/PMR dépendantes)

## Informations à demander (required_info)
### Si P0 (personne coincée)
- Résidence / bloc
- Ascenseur concerné (A/B si plusieurs)
- Étage approximatif où il est bloqué
- Nombre de personnes à l’intérieur + état (panique/malaise)
- Depuis combien de minutes
- Numéro de téléphone sur place (si possible)

### Si P1 (panne simple)
- Résidence / bloc
- Ascenseur concerné
- Depuis quand (heures/jours)
- Symptômes : ne démarre pas / portes bloquées / bruit anormal
- Impact : personnes âgées/PMR concernées (oui/non)

## Procédure (étapes)
1. Si P0 : rassurer + escalade immédiate prestataire ascenseur + consigne sécurité.
2. Si P1 : ticket prestataire + planification + communication délai.
3. Informer gardien si applicable (accès techniciens / sécurisation).
4. Suivi : confirmation heure d’intervention, mise hors service si nécessaire, retour résident.

## Consignes de sécurité (P0)
- Rester calme, ne pas forcer les portes, attendre l’intervention.
- Si malaise/détresse : appeler les urgences.

## Responsabilités
- **Prestataire ascenseur** : intervention / maintenance.
- **Syndic** : coordination + suivi.
- **Gardien** : accès machine/local + information résidents (si requis).

## Modèle de réponse (base)
- P0 : rassurer + urgence + infos minimales + appel urgences si détresse.
- P1 : confirmer ticket + demander infos utiles + indiquer suivi.
""",
}

def main():
    base_dir = Path(__file__).resolve().parent.parent
    docs_dir = base_dir / "data" / "docs"
    docs_dir.mkdir(parents=True, exist_ok=True)

    for name, content in DOCS.items():
        p = docs_dir / name
        p.write_text(content.strip() + "\n", encoding="utf-8")
        print("✅ écrit:", p)

if __name__ == "__main__":
    main()
