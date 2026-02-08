from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/home/onizuka/Bureau/ai_project"
VENV_ACTIVATE = f"{PROJECT_DIR}/venv/bin/activate"

default_args = {
    "owner": "onizuka",
    "retries": 1,
}

with DAG(
    dag_id="syndic_rag_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None,      # manuel (tu peux mettre un cron ensuite)
    catchup=False,
    tags=["syndic", "rag"],
) as dag:

    # 0) Préparer dossiers
    prep_dirs = BashOperator(
        task_id="prep_dirs",
        bash_command=f"""
        set -euo pipefail
        mkdir -p "{PROJECT_DIR}/notebooks/output" "{PROJECT_DIR}/cleanData/rag"
        """.strip(),
    )

    # 1) Cleaning via notebook (papermill)
    clean_messages = BashOperator(
        task_id="01_clean_messages",
        bash_command=f"""
        set -euo pipefail
        cd "{PROJECT_DIR}"
        source "{VENV_ACTIVATE}"
        papermill notebooks/01_clean_messages.ipynb notebooks/output/01_clean_messages_out.ipynb
        """.strip(),
    )

    # 2) Détection langue
    detect_lang = BashOperator(
        task_id="02_language_detection",
        bash_command=f"""
        set -euo pipefail
        cd "{PROJECT_DIR}"
        source "{VENV_ACTIVATE}"
        python src/02_language_detection_ml.py
        """.strip(),
    )

    # 3) Classification rules (urgency + category)
    rules_baseline = BashOperator(
        task_id="03_rules_baseline",
        bash_command=f"""
        set -euo pipefail
        cd "{PROJECT_DIR}"
        source "{VENV_ACTIVATE}"
        python src/03_rules_baseline.py
        """.strip(),
    )

    # 4) Audit rules (optionnel mais OK à garder)
    rules_audit = BashOperator(
        task_id="04_rules_audit",
        bash_command=f"""
        set -euo pipefail
        cd "{PROJECT_DIR}"
        source "{VENV_ACTIVATE}"
        python src/04_rules_audit.py
        """.strip(),
    )

    # 5) Générer docs (optionnel mais recommandé si policy/docs changent)
    make_docs_policy = BashOperator(
        task_id="06_make_docs_from_policy",
        bash_command=f"""
        set -euo pipefail
        cd "{PROJECT_DIR}"
        source "{VENV_ACTIVATE}"
        python src/06_make_docs_from_policy.py
        """.strip(),
    )

    make_docs_p2p3 = BashOperator(
        task_id="06_make_docs_p2_p3",
        bash_command=f"""
        set -euo pipefail
        cd "{PROJECT_DIR}"
        source "{VENV_ACTIVATE}"
        python src/06_make_docs_p2_p3.py
        """.strip(),
    )

    make_docs_cases = BashOperator(
        task_id="06_make_docs_specific_cases",
        bash_command=f"""
        set -euo pipefail
        cd "{PROJECT_DIR}"
        source "{VENV_ACTIVATE}"
        python src/06_make_docs_specific_cases.py
        """.strip(),
    )

    # 6) Build RAG index
    build_rag_index = BashOperator(
        task_id="06_build_rag_index",
        bash_command=f"""
        set -euo pipefail
        cd "{PROJECT_DIR}"
        source "{VENV_ACTIVATE}"
        python src/06_build_rag_index.py
        """.strip(),
    )

    # 7) Retrieve context (RAG)
    rag_retrieve = BashOperator(
        task_id="07_rag_retrieve_for_messages",
        bash_command=f"""
        set -euo pipefail
        cd "{PROJECT_DIR}"
        source "{VENV_ACTIVATE}"
        python src/07_rag_retrieve_for_messages.py
        """.strip(),
    )

    # 8) Generate LLM responses (limit configurable)
    # Lancement: "Trigger DAG w/ config" -> {"limit": 10}
    generate_llm = BashOperator(
        task_id="09_rag_generate_responses",
        bash_command=f"""
        set -euo pipefail
        cd "{PROJECT_DIR}"
        source "{VENV_ACTIVATE}"
        python src/09_rag_generate_responses.py --limit {{{{ dag_run.conf.get('limit', 10) }}}}
        """.strip(),
        # Optionnel: variables d'env utiles
        env={
            "OLLAMA_HOST": "http://localhost:11434",
            "OLLAMA_MODEL": "qwen2.5:7b-instruct-q4_K_M",
            # tu peux aussi régler WORKERS/TEMP ici
            # "WORKERS": "2",
            # "TEMPERATURE": "0.1",
            # "TOP_P": "0.2",
            # "NUM_PREDICT": "220",
        },
    )

    # Dépendances
    prep_dirs >> clean_messages >> detect_lang >> rules_baseline >> rules_audit
    rules_audit >> make_docs_policy >> make_docs_p2p3 >> make_docs_cases
    make_docs_cases >> build_rag_index >> rag_retrieve >> generate_llm
