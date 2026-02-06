# src/06_make_docs_p2_p3.py
from pathlib import Path

P2_MD = """# Procédures - Non urgent (P2)

Ces cas ne sont pas urgents mais nécessitent une intervention planifiée (souvent < 24h).

## Cas typiques (P2)
- Bruit / nuisance répétitive
- Propreté / saleté / poubelles pleines
- Stationnement abusif
- Badge / télécommande / accès (si pas bloquant/sécurité)

## Actions recommandées (P2)
- Confirmer localisation exacte (résidence/bloc/étage/porte).
- Demander depuis quand + fréquence.
- Demander preuve légère si utile (photo/vidéo si possible).
- Créer ticket + planifier passage (gardien/prestataire selon catégorie).
"""

P3_MD = """# Procédures - Administratif (P3)

Cas administratifs, information, demandes de documents.

## Cas typiques (P3)
- Attestation de résidence
- Quittance / facture
- Détail des charges
- Questions générales (horaires, procédures, réservation)

## Actions recommandées (P3)
- Identifier la demande exacte + période concernée.
- Vérifier identité / appartement / résidence.
- Donner procédure + délais + canal de livraison (WhatsApp/email).
- Ticket syndic (ou équipe admin).
"""

def main():
    base_dir = Path(__file__).resolve().parent.parent
    docs_dir = base_dir / "data" / "docs"
    docs_dir.mkdir(parents=True, exist_ok=True)

    p2_path = docs_dir / "procedures_p2.md"
    p3_path = docs_dir / "procedures_p3.md"

    p2_path.write_text(P2_MD, encoding="utf-8")
    p3_path.write_text(P3_MD, encoding="utf-8")

    print("✅ Docs créés :")
    print(" -", p2_path)
    print(" -", p3_path)

if __name__ == "__main__":
    main()
