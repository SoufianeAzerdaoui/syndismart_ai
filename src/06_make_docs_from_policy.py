# src/06_make_docs_from_policy.py
import json
from pathlib import Path

def main():
    base_dir = Path(__file__).resolve().parent.parent
    policy_path = base_dir / "policy" / "policy_config.json"
    docs_dir = base_dir / "data" / "docs"
    docs_dir.mkdir(parents=True, exist_ok=True)

    policy = json.loads(policy_path.read_text(encoding="utf-8"))

    # --- Doc P0 ---
    p0 = policy.get("guardrails", {}).get("patterns", {}).get("P0", [])
    p0_lines = [
        "# Procédures - Urgence critique (P0)",
        "",
        "Ces cas sont critiques. Toujours prioriser la sécurité.",
        "",
        "## Règles P0 (patterns)",
        ""
    ]
    for r in p0:
        rid = r.get("id", "P0_RULE")
        explain = r.get("explain", "")
        p0_lines.append(f"### {rid}")
        if explain:
            p0_lines.append(f"- Description: {explain}")
        if "all" in r:
            p0_lines.append(f"- Doit contenir (all): {r['all']}")
        if "any" in r:
            p0_lines.append(f"- Un des mots (any): {r['any']}")
        if "any_group" in r:
            p0_lines.append(f"- Groupes (any_group): {r['any_group']}")
        p0_lines.append("")
    p0_lines += [
        "## Actions recommandées (P0)",
        "- Demander la localisation exacte (résidence/bloc/étage).",
        "- Confirmer si danger immédiat (oui/non).",
        "- Demander photo/vidéo si possible (sans retarder l'urgence).",
        "- Escalader immédiatement vers prestataire/sécurité selon catégorie.",
        "",
    ]
    (docs_dir / "procedures_p0.md").write_text("\n".join(p0_lines), encoding="utf-8")

    # --- Doc P1 ---
    p1 = policy.get("guardrails", {}).get("patterns", {}).get("P1", [])
    p1_lines = [
        "# Procédures - Urgence (P1)",
        "",
        "Ces cas sont urgents mais pas forcément vitaux. Intervention rapide.",
        "",
        "## Règles P1 (patterns)",
        ""
    ]
    for r in p1:
        rid = r.get("id", "P1_RULE")
        explain = r.get("explain", "")
        p1_lines.append(f"### {rid}")
        if explain:
            p1_lines.append(f"- Description: {explain}")
        if "all" in r:
            p1_lines.append(f"- Doit contenir (all): {r['all']}")
        if "any" in r:
            p1_lines.append(f"- Un des mots (any): {r['any']}")
        if "any_group" in r:
            p1_lines.append(f"- Groupes (any_group): {r['any_group']}")
        p1_lines.append("")
    p1_lines += [
        "## Actions recommandées (P1)",
        "- Collecter: résidence/bloc/localisation, depuis quand, impact (combien de voisins).",
        "- Demander photo/vidéo si possible.",
        "- Prévenir prestataire (selon catégorie).",
        "",
    ]
    (docs_dir / "procedures_p1.md").write_text("\n".join(p1_lines), encoding="utf-8")

    # --- Doc Keywords (P1/P2/P3) ---
    levels = policy.get("levels", {})
    kw_lines = [
        "# Guide - Mots-clés par niveau",
        "",
        "Ce document liste les mots-clés utilisés par le baseline rules (P1/P2/P3).",
        ""
    ]
    for lvl in ["P1", "P2", "P3"]:
        kw = levels.get(lvl, {}).get("keywords", [])
        kw_lines.append(f"## {lvl}")
        kw_lines.append(", ".join(kw) if kw else "(aucun)")
        kw_lines.append("")
    (docs_dir / "keywords_levels.md").write_text("\n".join(kw_lines), encoding="utf-8")

    # --- Mini FAQ admin (template) ---
    faq = [
        "# FAQ - Administratif (P3)",
        "",
        "## Attestation de résidence",
        "- Informations à demander: nom, résidence, numéro appartement, raison (école/travail/dossier), copie CIN si nécessaire (selon règlement).",
        "",
        "## Quittance / facture",
        "- Informations à demander: mois concerné, email/WhatsApp de réception, identité du demandeur.",
        "",
        "## Détail des charges",
        "- Informations à demander: période, type de charges, justificatif souhaité.",
        "",
    ]
    (docs_dir / "admin_faq.md").write_text("\n".join(faq), encoding="utf-8")

    print("✅ Docs générés dans:", docs_dir)
    for fp in sorted(docs_dir.glob("*.md")):
        print(" -", fp)

if __name__ == "__main__":
    main()
