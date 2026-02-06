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
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:3b-instruct").strip()

MAX_CONTEXT_CHARS = int(os.getenv("MAX_CONTEXT_CHARS", "1500"))
MAX_TEXT_CHARS = int(os.getenv("MAX_TEXT_CHARS", "900"))

TEMPERATURE = float(os.getenv("TEMPERATURE", "0.0"))
TOP_P = float(os.getenv("TOP_P", "0.05"))
NUM_PREDICT = int(os.getenv("NUM_PREDICT", "110"))

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "1"))
SLEEP_BETWEEN_RETRIES = float(os.getenv("SLEEP_BETWEEN_RETRIES", "0.6"))
TIMEOUT_SEC = int(os.getenv("TIMEOUT_SEC", "90"))

# progress
LOG_EVERY = int(os.getenv("LOG_EVERY", "1"))

# threads (2 par défaut)
WORKERS = int(os.getenv("WORKERS", "2"))

SESSION = requests.Session()

# =========================
# PROMPTS (FR ONLY)
# =========================
def build_system_prompt() -> str:
    return (
        "Tu es un assistant IA pour un syndic / gestion immobilière au Maroc.\n\n"
        "RÈGLES STRICTES:\n"
        "- Tu réponds TOUJOURS en français professionnel, même si le message est en darija.\n"
        "- Réponse courte, claire, actionnable.\n"
        "- Base-toi UNIQUEMENT sur le CONTEXTE fourni.\n"
        "- Si le contexte est insuffisant: ne donne pas d'action technique précise; demande les infos via required_info.\n"
        "- Respecte urgency_level et category EXACTEMENT.\n"
        "- Si urgency_level est P0 ou P1: ajoute consignes de sécurité + escalade.\n\n"
        "FORMAT STRICT:\n"
        "- Répond UNIQUEMENT en JSON valide. Aucun texte hors JSON.\n"
        "- required_info doit toujours être une liste [].\n"
    )


def build_user_prompt(message_text: str, urgency_level: str, category: str, rag_context: str) -> str:
    msg = (message_text or "")[:MAX_TEXT_CHARS].strip()
    ctx = (rag_context or "")[:MAX_CONTEXT_CHARS].strip()
    ctx_flag = "VIDE" if not ctx else "DISPONIBLE"

    return f"""
ENTRÉE CLASSIFIÉE (NE PAS MODIFIER):
- urgency_level: {urgency_level}
- category: {category}

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
2) Si CONTEXTE RAG est VIDE ou insuffisant -> required_info liste les infos à demander.
3) Si P0/P1 -> ajouter consignes sécurité + escalade.
4) Retourne UNIQUEMENT un JSON strict.

JSON attendu (forme exacte):
{{
  "urgency_level": "{urgency_level}",
  "category": "{category}",
  "is_urgent": {1 if urgency_level in ["P0","P1"] else 0},
  "sla_target_minutes": {SLA_TABLE.get(urgency_level,1440)},
  "assigned_to": "{ASSIGNED_TO_TABLE.get(urgency_level,"SYNDIC")}",
  "response_draft": "string",
  "required_info": [],
  "decision_source": "RAG",
  "status": "TO_VALIDATE"
}}
""".strip()

# =========================
# REQUIRED_INFO RULES (anti répétition)
# =========================
REQ_BY_CATEGORY = {
    "admin": ["mois/période concernée", "résidence/bloc", "num appartement", "email (si envoi PDF)"],
    "elevator": ["résidence/bloc", "étage/localisation exacte", "depuis quand", "personnes bloquées ?", "danger immédiat ?"],
    "electricity": ["résidence/bloc", "localisation exacte", "depuis quand", "étincelles/odeur brûlé ?", "danger immédiat ?"],
    "watr_leak": ["résidence/bloc", "localisation exacte", "depuis quand", "photo/vidéo si possible"],
    "garage_access": ["résidence/bloc", "porte/accès concerné", "badge/télécommande ?", "depuis quand"],
    "cleanliness": ["résidence/bloc", "zone exacte (escaliers/étage)", "photo si possible"],
    "noise": ["résidence/bloc", "heure", "source du bruit (si connue)"],
    "security": ["résidence/bloc", "localisation exacte", "description", "danger immédiat ?", "appel sécurité/police ?"],
    "other": ["résidence/bloc", "localisation exacte", "détails", "photo/vidéo si possible"],
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
        # try json list
        if s.startswith("[") and s.endswith("]"):
            try:
                obj = json.loads(s)
                if isinstance(obj, list):
                    return [str(i).strip() for i in obj if str(i).strip()]
            except Exception:
                pass
        # comma fallback
        return [p.strip() for p in s.split(",") if p.strip()]
    return [str(x).strip()]

def merge_required_info(existing: List[str], category: str, urgency: str, rag_context: str) -> List[str]:
    base = list(existing or [])
    cat = (category or "other").strip()
    urg = coerce_level(urgency)
    # si pas de contexte -> demander plus
    if not (rag_context or "").strip():
        base += REQ_BY_CATEGORY.get(cat, REQ_BY_CATEGORY["other"])
    # si urgent -> ajouter photo + sécurité
    if urg in {"P0", "P1"}:
        base += ["localisation exacte", "depuis quand", "photo/vidéo si possible"]
    # unique
    out = []
    for x in base:
        x = str(x).strip()
        if x and x not in out:
            out.append(x)
    return out

def fallback_json(urgency: str, category: str, rag_context: str) -> Dict[str, Any]:
    urgency = coerce_level(urgency)
    category = (category or "other").strip() or "other"
    decision_source = "RAG" if (rag_context or "").strip() else "NO_RAG"

    resp = (
        "Demande reçue. Merci de préciser la résidence/bloc, la localisation exacte, "
        "et joindre une photo/vidéo si possible."
    )
    if urgency in {"P0","P1"}:
        resp = (
            "Signalement urgent reçu. Merci d’indiquer la résidence/bloc, la localisation exacte, "
            "depuis quand, et joindre une photo/vidéo si possible. "
            "Si danger immédiat, appelez les urgences. Nous alertons le prestataire."
        )

    return {
        "urgency_level": urgency,
        "category": category,
        "is_urgent": 1 if urgency in {"P0", "P1"} else 0,
        "sla_target_minutes": SLA_TABLE.get(urgency, 1440),
        "assigned_to": ASSIGNED_TO_TABLE.get(urgency, "SYNDIC"),
        "response_draft": resp,
        "required_info": merge_required_info([], category, urgency, rag_context),
        "decision_source": decision_source,
        "status": DEFAULT_STATUS,
    }

def normalize_output_json(obj: Dict[str, Any], urgency: str, category: str, rag_context: str) -> Dict[str, Any]:
    urgency = coerce_level(urgency)
    category = (category or "other").strip() or "other"
    decision_source = "RAG" if (rag_context or "").strip() else "NO_RAG"

    out: Dict[str, Any] = {}
    out["urgency_level"] = urgency
    out["category"] = category
    out["is_urgent"] = 1 if urgency in {"P0", "P1"} else 0
    out["sla_target_minutes"] = int(SLA_TABLE.get(urgency, 1440))
    out["assigned_to"] = ASSIGNED_TO_TABLE.get(urgency, "SYNDIC")
    out["decision_source"] = decision_source
    out["status"] = DEFAULT_STATUS

    assigned = str(obj.get("assigned_to", "") or "").strip().upper()
    if assigned in {"PRESTATAIRE", "SYNDIC", "GARDIEN"}:
        out["assigned_to"] = assigned

    response = str(obj.get("response_draft", "") or "").strip()
    if not response:
        # fallback “soft”
        response = fallback_json(urgency, category, rag_context)["response_draft"]

    out["response_draft"] = response

    req = ensure_list_str(obj.get("required_info", []))
    out["required_info"] = merge_required_info(req, category, urgency, rag_context)

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
    required_keys = {
        "urgency_level": str,
        "category": str,
        "is_urgent": (int, bool),
        "sla_target_minutes": (int, float, str),
        "assigned_to": str,
        "response_draft": str,
        "required_info": (list, str),
        "decision_source": str,
        "status": str,
    }
    for k, t in required_keys.items():
        if k not in obj or not isinstance(obj[k], t):
            return False
    return True

# =========================
# Ollama call
# =========================
def post_json(url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    r = SESSION.post(url, json=payload, timeout=TIMEOUT_SEC)
    r.raise_for_status()
    return r.json()

def call_ollama_chat(system_prompt: str, user_prompt: str) -> str:
    payload = {
        "model": OLLAMA_MODEL,
        "stream": False,
        "format": "json",
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
        cat = (input("Category [other]: ").strip() or "other")
        ctx = input("RAG context (optionnel): ").strip()

        prompt = build_user_prompt(text, urg, cat, ctx)
        t0 = time.time()
        try:
            obj, raw = call_llm(system_prompt, prompt)
            norm = normalize_output_json(obj, urg, cat, ctx)
        except Exception as e:
            print("❌", e)
            norm = fallback_json(urg, cat, ctx)

        print(json.dumps(norm, ensure_ascii=False, indent=2))
        print(f"⏱️ {time.time()-t0:.1f}s\n")

# =========================
# Worker per row (for threads)
# =========================
def process_one(i: int, row_dict: Dict[str, Any], system_prompt: str) -> Tuple[int, Dict[str, Any], Optional[Dict[str, Any]]]:
    text = str(row_dict.get("text_clean", "") or "")
    rag_ctx = str(row_dict.get("rag_context", "") or "")
    urg = coerce_level(str(row_dict.get("final_urgency_level", "") or row_dict.get("priority_rules", "P3")))
    cat = str(row_dict.get("final_category", "") or row_dict.get("category", "other") or "other").strip() or "other"

    prompt = build_user_prompt(text, urg, cat, rag_ctx)

    last_raw = ""
    last_error = ""
    for attempt in range(MAX_RETRIES + 1):
        try:
            obj, last_raw = call_llm(system_prompt, prompt)
            norm = normalize_output_json(obj, urg, cat, rag_ctx)
            return i, norm, None
        except Exception as e:
            last_error = str(e)
            if attempt < MAX_RETRIES:
                time.sleep(SLEEP_BETWEEN_RETRIES)

    # fail -> fallback + audit record
    norm = fallback_json(urg, cat, rag_ctx)
    fail = {
        "row_index": i,
        "message_id": row_dict.get("message_id", ""),
        "urgency_level": urg,
        "category": cat,
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

    # write columns
    gen_json_col, response_col, required_info_col = [], [], []
    assigned_to_col, status_col, sla_col, is_urgent_col, decision_source_col = [], [], [], [], []

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

    df["gen_json"] = gen_json_col
    df["response_draft"] = response_col
    df["required_info"] = required_info_col
    df["assigned_to"] = assigned_to_col
    df["status"] = status_col
    df["sla_target_minutes"] = sla_col
    df["is_urgent"] = is_urgent_col
    df["decision_source"] = decision_source_col

    df.to_csv(out_path, index=False, encoding="utf-8")
    print("\n✅ DONE")
    print(f"Saved: {out_path}")

    if fails:
        pd.DataFrame(fails).to_csv(audit_fail, index=False, encoding="utf-8")
        print(f"⚠️ LLM failures saved: {audit_fail}")

if __name__ == "__main__":
    main()
