from __future__ import annotations

import os
import sys
import json
import time
from pathlib import Path
from typing import Any, Dict, Optional, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests

# =========================
# CONFIG
# =========================
SLA_TABLE = {"P0": 5, "P1": 30, "P2": 240, "P3": 1440}
ASSIGNED_TO_TABLE = {"P0": "PRESTATAIRE", "P1": "PRESTATAIRE", "P2": "SYNDIC", "P3": "SYNDIC"}
DEFAULT_STATUS = "TO_VALIDATE"

OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434").rstrip("/")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:7b-instruct-q4_K_M").strip()

MAX_CONTEXT_CHARS = int(os.getenv("MAX_CONTEXT_CHARS", "1500"))
MAX_TEXT_CHARS = int(os.getenv("MAX_TEXT_CHARS", "900"))

# Pour le 7B, un peu de créativité contrôlée
TEMPERATURE = float(os.getenv("TEMPERATURE", "0.1"))
TOP_P = float(os.getenv("TOP_P", "0.2"))
NUM_PREDICT = int(os.getenv("NUM_PREDICT", "220"))

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "1"))
SLEEP_BETWEEN_RETRIES = float(os.getenv("SLEEP_BETWEEN_RETRIES", "0.6"))
TIMEOUT_SEC = int(os.getenv("TIMEOUT_SEC", "90"))

LOG_EVERY = int(os.getenv("LOG_EVERY", "1"))
WORKERS = int(os.getenv("WORKERS", "2"))

SESSION = requests.Session()

# =========================
# PROMPTS (FR ONLY)
# =========================
def build_system_prompt() -> str:
    return (
        "Tu es un assistant IA pour un syndic / gestion immobilière au Maroc.\n\n"
        "RÈGLES STRICTES (OBLIGATOIRES):\n"
        "- Tu réponds TOUJOURS en français professionnel, même si le message est en darija.\n"
        "- Réponse courte, claire, précise et actionnable.\n"
        "- Base-toi UNIQUEMENT sur le CONTEXTE RAG fourni (preuves).\n"
        "- Si le CONTEXTE RAG contient une procédure claire, tu DOIS l’utiliser.\n"
        "- Interdiction de répondre par un message générique si une procédure est disponible.\n"
        "- Si le contexte est insuffisant, ne donne pas d'action technique précise.\n"
        "- Respecte urgency_level et category EXACTEMENT (ne jamais les modifier).\n"
        "- Si urgency_level est P0 ou P1 : ajoute des consignes de sécurité + une escalade claire.\n"
        "- Commence toujours response_draft par une phrase montrant que tu as compris la demande spécifique.\n"
        "- required_info : maximum 3 éléments.\n"
        "- Ne demande PAS de photo/vidéo pour les demandes administratives.\n"
        "- Si le message mentionne un risque (étincelles, fumée, gaz, eau près électricité), tu priorises la sécurité "
        "même si le message parle aussi de propreté/bruit.\n"
        "- Si secondary_category est présent, ajoute une phrase courte indiquant que ce point sera traité après "
        "sécurisation/intervention.\n\n"
        "FORMAT STRICT:\n"
        "- Tu réponds UNIQUEMENT en JSON valide.\n"
        "- Aucun texte hors JSON.\n"
        "- required_info doit toujours être une liste [].\n\n"
        "STYLE ATTENDU:\n"
        "- Ton professionnel, humain, crédible syndic.\n"
        "- Pas de phrases vagues type : 'Demande reçue', 'Merci de préciser'.\n"
        "- Chaque réponse doit être spécifique au message.\n"
        "- Le CONTEXTE RAG peut mentionner plusieurs niveaux (P0/P1/P2) : applique uniquement celui fourni dans urgency_level. \n"
    )


def build_user_prompt(
    message_text: str,
    urgency_level: str,
    category: str,
    rag_context: str,
    secondary_category: str = "",
) -> str:
    msg = (message_text or "")[:MAX_TEXT_CHARS].strip()
    ctx = (rag_context or "")[:MAX_CONTEXT_CHARS].strip()
    ctx_flag = "VIDE" if not ctx else "DISPONIBLE"
    sec = (secondary_category or "").strip()

    return f"""
ENTRÉE CLASSIFIÉE (NE PAS MODIFIER):
- urgency_level: {urgency_level}
- category: {category}
- secondary_category: {sec}

MESSAGE UTILISATEUR:
<<<USER_MESSAGE
{msg}
USER_MESSAGE>>>

CONTEXTE RAG [{ctx_flag}] (preuves):
<<<RAG_CONTEXT
{ctx}
RAG_CONTEXT>>>

TÂCHE:
1) Rédige une réponse AU RÉSIDENT en français, courte et actionnable, dans response_draft.
2) Si CONTEXTE RAG est VIDE ou insuffisant -> required_info liste les infos à demander (max 3).
3) Si P0/P1 -> ajouter consignes sécurité + escalade.
4) Si secondary_category est renseigné, mentionne le sujet secondaire en 1 phrase ("Nous traiterons aussi ... après sécurisation").
5) Retourne UNIQUEMENT un JSON strict.

JSON attendu (forme exacte):
{{
  "response_draft": "string",
  "required_info": [],
  "secondary_category": "{sec}"
}}
""".strip()

# =========================
# REQUIRED_INFO RULES
# =========================
REQ_BY_CATEGORY = {
    "admin": ["résidence/bloc", "num appartement", "mois/période concernée"],
    "reservation": ["date souhaitée", "heure début/fin", "nombre de personnes"],
    "elevator": ["résidence/bloc", "ascenseur concerné", "depuis quand"],
    "electricity": ["résidence/bloc", "localisation exacte", "fumée ou odeur de brûlé ?"],
    "watr_leak": ["résidence/bloc", "localisation exacte", "depuis quand"],
    "garage_access": ["résidence/bloc", "porte/accès concerné", "badge/télécommande ?"],
    "cleanliness": ["résidence/bloc", "zone exacte (escaliers/étage)", "depuis quand"],
    "noise": ["résidence/bloc", "heure", "source du bruit (si connue)"],
    "security": ["résidence/bloc", "localisation exacte", "danger immédiat ?"],
    "other": ["résidence/bloc", "localisation exacte", "détails"],
}

def coerce_level(level: str) -> str:
    level = (level or "").strip().upper()
    return level if level in {"P0", "P1", "P2", "P3"} else "P3"

def ensure_list_str(x: Any) -> List[str]:
    if x is None:
        return []
    if isinstance(x, list):
        return [str(i).strip() for i in x if str(i).strip()]
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return []
        if s.startswith("[") and s.endswith("]"):
            try:
                obj = json.loads(s)
                if isinstance(obj, list):
                    return [str(i).strip() for i in obj if str(i).strip()]
            except Exception:
                pass
        return [p.strip() for p in s.split(",") if p.strip()]
    return [str(x).strip()]

def merge_required_info(existing: List[str], category: str, urgency: str, rag_context: str) -> List[str]:
    base = list(existing or [])
    cat = (category or "other").strip() or "other"
    urg = coerce_level(urgency)

    # si pas de contexte => demander le set par catégorie
    if not (rag_context or "").strip():
        base += REQ_BY_CATEGORY.get(cat, REQ_BY_CATEGORY["other"])

    # urgent => ajouter localisation/depuis quand sauf admin/reservation
    if urg in {"P0", "P1"} and cat not in {"admin", "reservation"}:
        base += ["localisation exacte", "depuis quand"]

    out: List[str] = []
    for x in base:
        x = str(x).strip()
        if x and x not in out:
            out.append(x)
    return out[:3]

def fallback_json(urgency: str, category: str, rag_context: str, secondary_category: str = "") -> Dict[str, Any]:
    urgency = coerce_level(urgency)
    category = (category or "other").strip() or "other"
    decision_source = "RAG" if (rag_context or "").strip() else "NO_RAG"
    sec = (secondary_category or "").strip()

    if category == "admin":
        resp = (
            "Je note votre demande administrative. Merci de confirmer votre résidence/bloc, "
            "votre numéro d’appartement et la période concernée."
        )
    elif category == "reservation":
        resp = (
            "Pour réserver la salle polyvalente, merci d’indiquer la date souhaitée, l’horaire (début/fin) "
            "et le nombre de personnes. Nous vérifions la disponibilité et revenons vers vous."
        )
    elif category == "elevator":
        resp = (
            "Je prends en charge votre signalement d’ascenseur hors service. "
            "Merci de confirmer la résidence/bloc, l’ascenseur concerné et depuis quand. "
            "Nous transmettons au prestataire pour intervention."
        )
    elif category == "electricity":
        resp = (
            "Signalement électrique urgent reçu. Par sécurité, ne touchez pas au tableau et éloignez les personnes. "
            "Nous alertons le prestataire. Merci d’indiquer la résidence/bloc et la localisation exacte, "
            "et si vous constatez fumée ou odeur de brûlé."
        )
    elif category == "cleanliness":
        resp = (
            "Votre réclamation de propreté est enregistrée. Merci d’indiquer la résidence/bloc et la zone exacte "
            "afin de planifier le passage du nettoyage."
        )
    else:
        resp = (
            "Je prends en compte votre signalement. Merci de préciser la résidence/bloc, la localisation exacte "
            "et depuis quand le problème est constaté."
        )

    if urgency in {"P0", "P1"} and category not in {"admin", "reservation"}:
        resp += " Si danger immédiat, appelez les urgences."

    if sec:
        resp += f" Nous traiterons aussi le point suivant après sécurisation/intervention : {sec}."

    return {
        "urgency_level": urgency,
        "category": category,
        "secondary_category": sec,
        "is_urgent": 1 if urgency in {"P0", "P1"} else 0,
        "sla_target_minutes": int(SLA_TABLE.get(urgency, 1440)),
        "assigned_to": ASSIGNED_TO_TABLE.get(urgency, "SYNDIC"),
        "response_draft": resp,
        "required_info": merge_required_info([], category, urgency, rag_context),
        "decision_source": decision_source,
        "status": DEFAULT_STATUS,
    }

def normalize_output_json(
    obj: Dict[str, Any],
    urgency: str,
    category: str,
    rag_context: str,
    secondary_category: str = "",
) -> Dict[str, Any]:
    urgency = coerce_level(urgency)
    category = (category or "other").strip() or "other"
    decision_source = "RAG" if (rag_context or "").strip() else "NO_RAG"
    sec = (secondary_category or "").strip()

    out: Dict[str, Any] = {
        "urgency_level": urgency,
        "category": category,
        "secondary_category": sec,
        "is_urgent": 1 if urgency in {"P0", "P1"} else 0,
        "sla_target_minutes": int(SLA_TABLE.get(urgency, 1440)),
        "assigned_to": ASSIGNED_TO_TABLE.get(urgency, "SYNDIC"),
        "decision_source": decision_source,
        "status": DEFAULT_STATUS,
    }

    response = str(obj.get("response_draft", "") or "").strip()
    if not response:
        response = fallback_json(urgency, category, rag_context, secondary_category=sec)["response_draft"]

    if sec and sec.lower() not in response.lower():
        response += f" Nous traiterons aussi le point suivant après sécurisation/intervention : {sec}."

    out["response_draft"] = response

    req = ensure_list_str(obj.get("required_info", []))
    out["required_info"] = merge_required_info(req, category, urgency, rag_context)

    assigned = str(obj.get("assigned_to", "") or "").strip().upper()
    if assigned in {"PRESTATAIRE", "SYNDIC", "GARDIEN"}:
        out["assigned_to"] = assigned

    return out

# =========================
# JSON parsing
# =========================
def parse_json_robust(text: str) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    s = text.strip()
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else None
    except Exception:
        pass
    a = s.find("{")
    b = s.rfind("}")
    if a != -1 and b != -1 and b > a:
        try:
            obj = json.loads(s[a:b+1])
            return obj if isinstance(obj, dict) else None
        except Exception:
            return None
    return None

def validate_min_schema(obj: Dict[str, Any]) -> bool:
    return isinstance(obj, dict) and ("response_draft" in obj) and ("required_info" in obj)

# =========================
# Ollama call
# =========================
def post_json(url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    r = SESSION.post(url, json=payload, timeout=TIMEOUT_SEC)
    r.raise_for_status()
    return r.json()

def call_ollama_chat(system_prompt: str, user_prompt: str) -> str:
    # IMPORTANT: /api/chat + format="json" est souvent problématique => on le retire
    payload = {
        "model": OLLAMA_MODEL,
        "stream": False,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "options": {"temperature": TEMPERATURE, "top_p": TOP_P, "num_predict": NUM_PREDICT},
    }
    data = post_json(f"{OLLAMA_HOST}/api/chat", payload)
    return ((data.get("message") or {}).get("content")) or ""

def call_llm(system_prompt: str, user_prompt: str) -> Tuple[Dict[str, Any], str]:
    raw = call_ollama_chat(system_prompt, user_prompt)
    obj = parse_json_robust(raw)
    if not isinstance(obj, dict):
        raise ValueError("LLM output is not valid JSON")
    if not validate_min_schema(obj):
        raise ValueError("LLM JSON does not match minimal schema")
    return obj, raw

# =========================
# CLI args
# =========================
def parse_args(argv: List[str]) -> Dict[str, Any]:
    args = {"limit": None, "interactive": False}
    if "--limit" in argv:
        try:
            i = argv.index("--limit")
            args["limit"] = int(argv[i + 1])
        except Exception:
            args["limit"] = None
    if "--interactive" in argv:
        args["interactive"] = True
    return args

# =========================
# Interactive chat terminal
# =========================
def interactive_mode():
    system_prompt = build_system_prompt()
    print(f"🔌 Chat terminal | {OLLAMA_HOST} | model={OLLAMA_MODEL}")
    print("Tape 'exit' pour quitter.\n")

    while True:
        text = input("Message: ").strip()
        if not text or text.lower() in {"exit", "quit"}:
            break

        urg = coerce_level(input("Urgency (P0/P1/P2/P3) [P3]: ").strip().upper() or "P3")
        cat = (input("Category [other]: ").strip() or "other").strip()
        sec = (input("Secondary category (optionnel): ").strip() or "")
        ctx = input("RAG context (optionnel): ").strip()

        prompt = build_user_prompt(text, urg, cat, ctx, secondary_category=sec)
        t0 = time.time()
        try:
            obj, _raw = call_llm(system_prompt, prompt)
            norm = normalize_output_json(obj, urg, cat, ctx, secondary_category=sec)
        except Exception as e:
            print("❌", e)
            norm = fallback_json(urg, cat, ctx, secondary_category=sec)

        print(json.dumps(norm, ensure_ascii=False, indent=2))
        print(f"⏱️ {time.time()-t0:.1f}s\n")

# =========================
# Worker per row (for threads)
# =========================
def process_one(
    i: int,
    row_dict: Dict[str, Any],
    system_prompt: str,
) -> Tuple[int, Dict[str, Any], Optional[Dict[str, Any]]]:
    text = str(row_dict.get("text_clean", "") or "")
    rag_ctx = str(row_dict.get("rag_context", "") or "")
    urg = coerce_level(str(row_dict.get("final_urgency_level", "") or row_dict.get("priority_rules", "P3")))
    cat = str(row_dict.get("final_category", "") or row_dict.get("category", "other") or "other").strip() or "other"
    sec = str(row_dict.get("secondary_category", "") or "").strip()

    prompt = build_user_prompt(text, urg, cat, rag_ctx, secondary_category=sec)

    last_raw = ""
    last_error = ""
    for attempt in range(MAX_RETRIES + 1):
        try:
            obj, last_raw = call_llm(system_prompt, prompt)
            norm = normalize_output_json(obj, urg, cat, rag_ctx, secondary_category=sec)
            return i, norm, None
        except Exception as e:
            last_error = str(e)
            if attempt < MAX_RETRIES:
                time.sleep(SLEEP_BETWEEN_RETRIES)

    norm = fallback_json(urg, cat, rag_ctx, secondary_category=sec)
    fail = {
        "row_index": i,
        "message_id": row_dict.get("message_id", ""),
        "urgency_level": urg,
        "category": cat,
        "secondary_category": sec,
        "error": last_error[:1500],
        "raw_output": (last_raw or "")[:3000],
    }
    return i, norm, fail

# =========================
# Main
# =========================
def main():
    args = parse_args(sys.argv[1:])
    if args["interactive"]:
        interactive_mode()
        return

    base_dir = Path(__file__).resolve().parent.parent
    in_path = base_dir / "cleanData" / "messages_with_context.csv"
    out_path = base_dir / "cleanData" / "messages_final.csv"

    audit_fail = base_dir / "cleanData" / "audit_llm_failures.csv"
    audit_fail.parent.mkdir(parents=True, exist_ok=True)

    if not in_path.exists():
        raise SystemExit(f"Fichier introuvable: {in_path}")

    df = pd.read_csv(in_path)
    if args["limit"] is not None:
        df = df.head(args["limit"]).copy()

    rows = df.to_dict(orient="records")
    n = len(rows)

    print(f"Rows loaded: {n}")
    print(f"Workers: {WORKERS}\n")

    system_prompt = build_system_prompt()
    print("SYSTEM_PROMPT_HASH=", hash(system_prompt))
    print(system_prompt[:300])

    results: Dict[int, Dict[str, Any]] = {}
    fails: List[Dict[str, Any]] = []

    t_all0 = time.time()
    done = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = [ex.submit(process_one, i+1, rows[i], system_prompt) for i in range(n)]
        for fut in as_completed(futures):
            i, norm, fail = fut.result()
            results[i] = norm
            if fail:
                fails.append(fail)

            done += 1
            if (done % LOG_EVERY) == 0 or done == n:
                elapsed = time.time() - t_all0
                rate = elapsed / max(done, 1)
                eta = rate * (n - done)
                pct = int(done / n * 100) if n else 100
                print(f"➡️ {done}/{n} | {pct}% | ETA {eta/60:.1f} min", flush=True)

    gen_json_col, response_col, required_info_col = [], [], []
    assigned_to_col, status_col, sla_col, is_urgent_col, decision_source_col = [], [], [], [], []
    secondary_col = []

    for i in range(1, n+1):
        norm = results[i]
        gen_json_col.append(json.dumps(norm, ensure_ascii=False))
        response_col.append(norm["response_draft"])
        required_info_col.append(json.dumps(norm["required_info"], ensure_ascii=False))
        assigned_to_col.append(norm["assigned_to"])
        status_col.append(norm["status"])
        sla_col.append(norm["sla_target_minutes"])
        is_urgent_col.append(norm["is_urgent"])
        decision_source_col.append(norm["decision_source"])
        secondary_col.append(norm.get("secondary_category", ""))

    df["gen_json"] = gen_json_col
    df["response_draft"] = response_col
    df["required_info"] = required_info_col
    df["assigned_to"] = assigned_to_col
    df["status"] = status_col
    df["sla_target_minutes"] = sla_col
    df["is_urgent"] = is_urgent_col
    df["decision_source"] = decision_source_col
    df["secondary_category"] = secondary_col

    df.to_csv(out_path, index=False, encoding="utf-8")
    print("\n✅ DONE")
    print(f"Saved: {out_path}")

    if fails:
        pd.DataFrame(fails).to_csv(audit_fail, index=False, encoding="utf-8")
        print(f"⚠️ LLM failures saved: {audit_fail}")

if __name__ == "__main__":
    main()
